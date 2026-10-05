"""
Trade copier relay server.

Master (NinjaTrader add-on)  --HTTP POST-->  this server  --WebSocket-->  Followers (NinjaTrader add-on)
Control panel for you: https://<server>/admin

Wire format is pipe-delimited plain text (easy to parse in NinjaScript):
  master -> server:
    order|new|<id>|<instrument>|<action>|<orderType>|<qty>|<limit>|<stop>|<oco>|<tif>
    order|change|<id>|...same as new
    order|cancel|<id>
    hb|<positions>|<current account>|<all account names, comma separated>
        positions = "NQ 12-26=1;ES 12-26=-2"
  server -> master (in the reply to any POST):
    ok  |  flatten|ALL  |  account|<new account name>
  server -> follower:
    new|<seq>|<id>|<instrument>|<action>|<orderType>|<qty>|<limit>|<stop>|<oco>|<tif>
    change|<seq>|...same as new
    cancel|<seq>|<id>
    flatten|<seq>|ALL  (or an instrument name)
  follower -> server:
    hb|<pnl>|<positions>
    ack|<seq>|ok|<detail>   or   ack|<seq>|err|<detail>
"""
import asyncio
import itertools
import json
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
from fastapi import Body, FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, PlainTextResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("copier")

HERE = Path(__file__).parent
CONFIG_PATH = os.getenv("COPIER_CONFIG", "config.json")
DB_PATH = os.getenv("COPIER_DB", "copier.db")
ADMIN_KEY = os.getenv("ADMIN_KEY", "change-me")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
MASTER_TIMEOUT_SEC = 15         # no heartbeat for this long -> master offline alert
FOLLOWER_TIMEOUT_SEC = 15
MASTER_COMMAND_EXPIRY_SEC = 60  # a queued master command is dropped if the master doesn't pick it up in time
NEW_YORK = ZoneInfo("America/New_York")


def trading_day() -> str:
    """CME futures sessions start at 6 pm New York time, so 6 pm counts as the start of the next trading day."""
    return (datetime.now(NEW_YORK) + timedelta(hours=6)).date().isoformat()


RECON_STRIKES = 3               # consecutive 5s checks with mismatched positions before alerting

with open(CONFIG_PATH) as f:
    CFG = json.load(f)

DB = sqlite3.connect(DB_PATH, check_same_thread=False)
DB.execute("create table if not exists events (ts real, source text, who text, body text)")
DB.execute("create table if not exists settings (k text primary key, v text)")
DB.commit()


def log_event(source: str, who: str, body: str) -> None:
    DB.execute("insert into events values (?, ?, ?, ?)", (time.time(), source, who, body))
    DB.commit()


def load_setting(k: str) -> Optional[dict]:
    row = DB.execute("select v from settings where k = ?", (k,)).fetchone()
    return json.loads(row[0]) if row else None


def save_setting(k: str, v: dict) -> None:
    DB.execute("insert or replace into settings values (?, ?)", (k, json.dumps(v)))
    DB.commit()


EDITABLE = ("multiplier", "max_contracts", "daily_loss_limit")


class FollowerState:
    def __init__(self, cfg: dict):
        self.cfg = dict(cfg)
        saved = load_setting(f"follower:{cfg['id']}")
        if saved:  # changes made in the control panel override config.json
            self.cfg.update({k: v for k, v in saved.items() if k in EDITABLE})
        self.id = cfg["id"]
        self.ws: Optional[WebSocket] = None
        self.paused = False
        self.pause_reason: Optional[str] = None   # "loss_limit" or "manual"
        self.loss_day: Optional[str] = None       # trading day the loss limit was hit
        self.last_hb = 0.0
        self.pnl = 0.0
        self.positions: dict = {}
        self.account: Optional[str] = None
        self.accounts: list = []
        self.last_error: Optional[str] = None
        self.last_error_at = 0.0
        self.strikes = 0
        self.stale_alerted = False


MASTERS = {m["key"]: m for m in CFG["masters"]}
MASTER_STATE = {
    m["id"]: {"last_hb": 0.0, "positions": {}, "offline_alerted": False,
              "account": None, "accounts": [], "commands": []}
    for m in CFG["masters"]
}
FOLLOWERS = [FollowerState(fc) for fc in CFG["followers"]]
FOLLOWER_BY_KEY = {fs.cfg["key"]: fs for fs in FOLLOWERS}
FOLLOWER_BY_ID = {fs.id: fs for fs in FOLLOWERS}
SEQ = itertools.count(1)


async def alert(msg: str) -> None:
    log.warning(msg)
    log_event("alert", "server", msg)
    if DISCORD_WEBHOOK:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                await client.post(DISCORD_WEBHOOK, json={"content": f"[copier] {msg}"})
        except Exception:
            log.exception("Discord alert failed")


def parse_positions(text: str) -> dict:
    out = {}
    for item in filter(None, text.split(";")):
        inst, _, qty = item.rpartition("=")
        if inst:
            out[inst] = int(qty)
    return out


def scale_qty(qty: int, fs: FollowerState) -> int:
    if qty <= 0:
        return 0
    scaled = max(1, round(qty * float(fs.cfg.get("multiplier", 1.0))))
    return min(scaled, int(fs.cfg.get("max_contracts", scaled)))


def expected_position(master_pos: int, fs: FollowerState) -> int:
    q = scale_qty(abs(master_pos), fs)
    return q if master_pos > 0 else -q


async def send(fs: FollowerState, kind: str, rest: str) -> None:
    msg = f"{kind}|{next(SEQ)}|{rest}"
    if fs.ws is None:
        log_event("missed", fs.id, msg)
        await alert(f"Follower {fs.id} is offline, missed: {msg}")
        return
    try:
        await fs.ws.send_text(msg)
        log_event("sent", fs.id, msg)
    except Exception as e:
        log_event("send_error", fs.id, f"{msg} :: {e}")
        await alert(f"Send to {fs.id} failed: {e}")


async def fan_out(master_id: str, parts: list) -> None:
    kind = parts[1]
    for fs in FOLLOWERS:
        if fs.cfg["master_id"] != master_id:
            continue
        if kind == "cancel":
            await send(fs, "cancel", parts[2])  # cancels always go through, even when paused
            continue
        if fs.paused:
            continue
        oid, inst, action, otype, qty, limit, stop, oco, tif = parts[2:11]
        q = scale_qty(int(qty), fs)
        await send(fs, kind, f"{oid}|{inst}|{action}|{otype}|{q}|{limit}|{stop}|{oco}|{tif}")


def queue_master_command(master_id: str, command: str) -> None:
    MASTER_STATE[master_id]["commands"].append((time.time(), command))


def master_reply(master_id: str, ms: dict) -> PlainTextResponse:
    """Commands for the master ride back on the response to its next POST (heartbeats come every 5s)."""
    while ms["commands"]:
        queued_at, command = ms["commands"].pop(0)
        if time.time() - queued_at <= MASTER_COMMAND_EXPIRY_SEC:
            log_event("sent", master_id, command)
            return PlainTextResponse(command)
        log_event("expired", master_id, command)
    return PlainTextResponse("ok")


async def maybe_auto_resume(fs: "FollowerState", now: float) -> None:
    """A follower paused by the daily loss limit starts copying again in the next trading session,
    once its NinjaTrader reports a fresh P&L that is back above the limit."""
    if not (fs.paused and fs.pause_reason == "loss_limit") or trading_day() == fs.loss_day:
        return
    if fs.ws is None or now - fs.last_hb > FOLLOWER_TIMEOUT_SEC:
        return  # wait for a fresh heartbeat so the P&L is today's
    limit = float(fs.cfg.get("daily_loss_limit") or 0)
    if limit > 0 and fs.pnl <= -limit:
        return  # NinjaTrader hasn't reset the day's P&L yet; check again in 5 seconds
    fs.paused = False
    fs.pause_reason = None
    fs.strikes = 0
    await alert(f"{fs.id}: new trading session, copying resumed automatically after yesterday's loss limit")


async def monitor() -> None:
    while True:
        await asyncio.sleep(5)
        now = time.time()
        for mid, ms in MASTER_STATE.items():
            if ms["last_hb"] and now - ms["last_hb"] > MASTER_TIMEOUT_SEC and not ms["offline_alerted"]:
                ms["offline_alerted"] = True
                await alert(f"Master {mid} stopped sending heartbeats")
        for fs in FOLLOWERS:
            await maybe_auto_resume(fs, now)
            if fs.ws is None or fs.paused:
                fs.strikes = 0
                continue
            if now - fs.last_hb > FOLLOWER_TIMEOUT_SEC:
                if not fs.stale_alerted:
                    fs.stale_alerted = True
                    await alert(f"Follower {fs.id} connected but no heartbeat")
                continue
            fs.stale_alerted = False
            ms = MASTER_STATE[fs.cfg["master_id"]]
            expected = {i: expected_position(p, fs) for i, p in ms["positions"].items() if p}
            actual = {i: p for i, p in fs.positions.items() if p}
            if expected != actual:
                fs.strikes += 1
                if fs.strikes == RECON_STRIKES:
                    await alert(f"Position mismatch for {fs.id}: expected {expected}, has {actual}")
            else:
                fs.strikes = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(monitor())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


@app.get("/health")
async def health():
    return {"ok": True}


# copymaster.<anything> opens the master page, copyfollower.<anything> the follower page.
PAGE_BY_NAME = {"copymaster": "master.html", "copyfollower": "follower.html"}


@app.get("/")
async def home(request: Request):
    host = (request.headers.get("host") or "").split(":")[0].lower()
    if host.startswith("www."):
        host = host[4:]
    page = PAGE_BY_NAME.get(host.split(".")[0])
    if page:
        return FileResponse(HERE / page, headers={"Cache-Control": "no-store"})
    return PlainTextResponse("Trade copier server is running.")


@app.get("/admin")
async def admin_page():
    return FileResponse(HERE / "admin.html", headers={"Cache-Control": "no-store"})


@app.get("/master")
async def master_page():
    return FileResponse(HERE / "master.html", headers={"Cache-Control": "no-store"})


@app.get("/follower")
async def follower_page():
    return FileResponse(HERE / "follower.html", headers={"Cache-Control": "no-store"})


# ---------------- master ----------------

@app.post("/master/event")
async def master_event(request: Request, x_master_key: Optional[str] = Header(default=None)):
    master = MASTERS.get(x_master_key or "")
    if not master:
        raise HTTPException(status_code=401, detail="bad master key")
    body = (await request.body()).decode("utf-8").strip()
    parts = body.split("|")
    ms = MASTER_STATE[master["id"]]
    ms["last_hb"] = time.time()
    if ms["offline_alerted"]:
        ms["offline_alerted"] = False
        await alert(f"Master {master['id']} is back online")

    if parts[0] == "hb":
        ms["positions"] = parse_positions(parts[1] if len(parts) > 1 else "")
        if len(parts) > 2 and parts[2]:
            if ms["account"] and ms["account"] != parts[2]:
                await alert(f"Master {master['id']} now copying from account {parts[2]} (was {ms['account']})")
            ms["account"] = parts[2]
        if len(parts) > 3:
            ms["accounts"] = sorted(a for a in parts[3].split(",") if a)
        return master_reply(master["id"], ms)

    log_event("master", master["id"], body)
    if parts[0] == "order" and len(parts) >= 3 and parts[1] in ("new", "change", "cancel"):
        if parts[1] != "cancel" and len(parts) < 11:
            raise HTTPException(status_code=400, detail="malformed order message")
        await fan_out(master["id"], parts)
    return master_reply(master["id"], ms)


# ---------------- followers ----------------

@app.websocket("/ws/follower")
async def follower_socket(ws: WebSocket):
    fs = FOLLOWER_BY_KEY.get(ws.query_params.get("key", ""))
    if fs is None:
        await ws.close(code=4401)
        return
    await ws.accept()
    if fs.ws is not None:
        try:
            await fs.ws.close()
        except Exception:
            pass
    fs.ws = ws
    fs.last_hb = time.time()
    log.info("Follower %s connected", fs.id)
    log_event("connect", fs.id, "")
    first_hb = True
    try:
        while True:
            text = await ws.receive_text()
            parts = text.split("|")
            if parts[0] == "hb" and first_hb:
                first_hb = False
                log.info("Follower %s first heartbeat: %s", fs.id, text[:300])
            if parts[0] == "hb" and len(parts) >= 3:
                fs.last_hb = time.time()
                fs.pnl = float(parts[1])
                fs.positions = parse_positions(parts[2])
                if len(parts) > 3:
                    fs.account = parts[3] or None
                if len(parts) > 4:
                    fs.accounts = sorted(a for a in parts[4].split(",") if a)
                await check_loss_limit(fs)
            elif parts[0] == "ack":
                log_event("ack", fs.id, text)
                if len(parts) > 2 and parts[2] == "err":
                    fs.last_error = "|".join(parts[3:])
                    fs.last_error_at = time.time()
                    await alert(f"{fs.id} rejected seq {parts[1]}: {'|'.join(parts[3:])}")
    except WebSocketDisconnect:
        pass
    finally:
        if fs.ws is ws:
            fs.ws = None
            await alert(f"Follower {fs.id} disconnected")


async def check_loss_limit(fs: FollowerState) -> None:
    limit = float(fs.cfg.get("daily_loss_limit") or 0)
    if limit > 0 and fs.pnl <= -limit and not fs.paused:
        fs.paused = True
        fs.pause_reason = "loss_limit"
        fs.loss_day = trading_day()
        await send(fs, "flatten", "ALL")
        await alert(f"{fs.id} hit daily loss limit (P&L {fs.pnl:.2f}); flattened and paused until the next session (6 pm ET)")


# ---------------- admin ----------------

def require_admin(key: Optional[str]) -> None:
    if key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="bad admin key")


def get_follower(follower_id: str) -> FollowerState:
    fs = FOLLOWER_BY_ID.get(follower_id)
    if fs is None:
        raise HTTPException(status_code=404, detail=f"no follower {follower_id}")
    return fs


@app.get("/status")
async def status(x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    now = time.time()
    return {
        "masters": {
            mid: {
                "online": bool(ms["last_hb"]) and now - ms["last_hb"] <= MASTER_TIMEOUT_SEC,
                "seconds_since_hb": round(now - ms["last_hb"], 1) if ms["last_hb"] else None,
                "positions": ms["positions"],
                "account": ms["account"],
                "accounts": ms["accounts"],
                "pending_commands": [c for _, c in ms["commands"]],
            }
            for mid, ms in MASTER_STATE.items()
        },
        "followers": {
            fs.id: {
                "connected": fs.ws is not None,
                "paused": fs.paused,
                "pause_reason": fs.pause_reason,
                "pnl": fs.pnl,
                "positions": fs.positions,
                "master": fs.cfg["master_id"],
                "mismatch": fs.strikes >= RECON_STRIKES,
                "account": fs.account,
                "accounts": fs.accounts,
                "last_error": fs.last_error if time.time() - fs.last_error_at < 60 else None,
                "multiplier": float(fs.cfg.get("multiplier", 1.0)),
                "max_contracts": int(fs.cfg.get("max_contracts", 1)),
                "daily_loss_limit": float(fs.cfg.get("daily_loss_limit") or 0),
            }
            for fs in FOLLOWERS
        },
    }


def validate_settings(body: dict, fields: tuple) -> dict:
    out = {}
    try:
        if "multiplier" in fields:
            m = float(body["multiplier"])
            if not m.is_integer() or not 1 <= m <= 50:
                raise HTTPException(status_code=400, detail="The multiplier must be a whole number from 1 to 50.")
            out["multiplier"] = int(m)
        if "max_contracts" in fields:
            out["max_contracts"] = int(body["max_contracts"])
            if not 1 <= out["max_contracts"] <= 500:
                raise HTTPException(status_code=400, detail="Max contracts must be between 1 and 500.")
        if "daily_loss_limit" in fields:
            out["daily_loss_limit"] = float(body["daily_loss_limit"])
            if out["daily_loss_limit"] < 0:
                raise HTTPException(status_code=400, detail="The daily loss limit can't be negative (use 0 for no limit).")
    except (KeyError, TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Please enter numbers only.")
    return out


async def apply_settings(fs: FollowerState, new: dict, changed_by: str) -> None:
    fs.cfg.update(new)
    saved = load_setting(f"follower:{fs.id}") or {}
    saved.update(new)
    save_setting(f"follower:{fs.id}", saved)
    summary = ", ".join(f"{k}={v}" for k, v in new.items())
    await alert(f"{fs.id} settings changed by {changed_by}: {summary}")
    await check_loss_limit(fs)


@app.post("/admin/followers/{follower_id}/settings")
async def update_follower_settings(follower_id: str, body: dict = Body(...),
                                   x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    fs = get_follower(follower_id)
    await apply_settings(fs, validate_settings(body, EDITABLE), "admin")
    return {"ok": True}


@app.post("/admin/masters/{master_id}/account")
async def switch_master_account(master_id: str, body: dict = Body(...),
                                x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    ms = MASTER_STATE.get(master_id)
    if ms is None:
        raise HTTPException(status_code=404, detail=f"no master {master_id}")
    account = str(body.get("account", "")).strip()
    if not account or "|" in account:
        raise HTTPException(status_code=400, detail="pick an account")
    if ms["accounts"] and account not in ms["accounts"]:
        raise HTTPException(status_code=400, detail=f"{account} isn't connected in NinjaTrader on the master computer")
    queue_master_command(master_id, f"account|{account}")
    await alert(f"Master {master_id}: switch to account {account} requested")
    return {"ok": True}


@app.post("/admin/kill")
async def kill_switch(x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    for fs in FOLLOWERS:
        fs.paused = True
        fs.pause_reason = "manual"
        await send(fs, "flatten", "ALL")
    await alert("KILL SWITCH: all followers flattened and paused")
    return {"ok": True}


@app.post("/admin/kill-all")
async def kill_all(x_admin_key: Optional[str] = Header(default=None)):
    """Flatten and pause every follower, AND flatten the master account(s)."""
    require_admin(x_admin_key)
    for fs in FOLLOWERS:
        fs.paused = True
        fs.pause_reason = "manual"
        await send(fs, "flatten", "ALL")
    now = time.time()
    offline = []
    for mid, ms in MASTER_STATE.items():
        queue_master_command(mid, "flatten|ALL")
        if not ms["last_hb"] or now - ms["last_hb"] > MASTER_TIMEOUT_SEC:
            offline.append(mid)
    note = f" WARNING: master(s) {offline} offline - flatten them by hand." if offline else ""
    await alert("KILL ALL: followers flattened and paused; master flatten sent (applies within ~5s)." + note)
    return {"ok": True, "masters_offline": offline}


@app.post("/admin/pause/{follower_id}")
async def pause(follower_id: str, flatten: bool = False, x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    fs = get_follower(follower_id)
    fs.paused = True
    fs.pause_reason = "manual"
    if flatten:
        await send(fs, "flatten", "ALL")
    await alert(f"{fs.id} paused (flatten={flatten})")
    return {"ok": True}


@app.post("/admin/resume/{follower_id}")
async def resume(follower_id: str, x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    fs = get_follower(follower_id)
    fs.paused = False
    fs.pause_reason = None
    fs.strikes = 0
    await alert(f"{fs.id} resumed")
    return {"ok": True}


# ---------------- self-service pages: /master and /follower ----------------
# The master page logs in with the master key (from master.txt) and can only switch accounts.
# The follower page logs in with that follower's own key (from follower.txt) and only sees that follower.

def master_online(ms: dict) -> bool:
    return bool(ms["last_hb"]) and time.time() - ms["last_hb"] <= MASTER_TIMEOUT_SEC


def master_from_key(key: Optional[str]) -> dict:
    master = MASTERS.get(key or "")
    if not master:
        raise HTTPException(status_code=401, detail="That master key isn't recognized.")
    return master


def follower_from_key(key: Optional[str]) -> FollowerState:
    fs = FOLLOWER_BY_KEY.get(key or "")
    if fs is None:
        raise HTTPException(status_code=401, detail="That follower key isn't recognized.")
    return fs


@app.get("/me/master")
async def my_master_status(x_master_key: Optional[str] = Header(default=None)):
    master = master_from_key(x_master_key)
    ms = MASTER_STATE[master["id"]]
    switching = next((c.split("|", 1)[1] for _, c in ms["commands"] if c.startswith("account|")), None)
    return {
        "name": master["id"],
        "online": master_online(ms),
        "account": ms["account"],
        "accounts": ms["accounts"],
        "switching_to": switching,
        "followers_connected": any(fs.cfg["master_id"] == master["id"] and fs.ws is not None for fs in FOLLOWERS),
    }


@app.post("/me/master/account")
async def my_master_switch(body: dict = Body(...), x_master_key: Optional[str] = Header(default=None)):
    master = master_from_key(x_master_key)
    ms = MASTER_STATE[master["id"]]
    if not master_online(ms):
        raise HTTPException(status_code=409, detail="NinjaTrader on the master computer isn't connected, so the account can't be switched right now.")
    account = str(body.get("account", "")).strip()
    if not account or "|" in account:
        raise HTTPException(status_code=400, detail="Pick an account first.")
    if ms["accounts"] and account not in ms["accounts"]:
        raise HTTPException(status_code=400, detail=f"{account} isn't connected in NinjaTrader on the master computer.")
    queue_master_command(master["id"], f"account|{account}")
    await alert(f"Master {master['id']}: switch to account {account} requested from the master page")
    return {"ok": True}


@app.get("/me/follower")
async def my_follower_status(x_follower_key: Optional[str] = Header(default=None)):
    fs = follower_from_key(x_follower_key)
    ms = MASTER_STATE[fs.cfg["master_id"]]
    return {
        "name": fs.id,
        "ninjatrader_connected": fs.ws is not None,
        "copying": fs.ws is not None and not fs.paused,
        "paused": fs.paused,
        "pause_reason": fs.pause_reason,
        "pnl": fs.pnl,
        "positions": fs.positions,
        "account": fs.account,
        "accounts": fs.accounts,
        "last_error": fs.last_error if time.time() - fs.last_error_at < 60 else None,
        "multiplier": float(fs.cfg.get("multiplier", 1.0)),
        "max_contracts": int(fs.cfg.get("max_contracts", 1)),
        "daily_loss_limit": float(fs.cfg.get("daily_loss_limit") or 0),
        "master": {
            "name": fs.cfg["master_id"],
            "online": master_online(ms),
            "seconds_since_hb": round(time.time() - ms["last_hb"], 1) if ms["last_hb"] else None,
            "positions": {i: expected_position(p, fs) for i, p in ms["positions"].items() if p},
        },
    }


@app.post("/me/follower/settings")
async def my_follower_settings(body: dict = Body(...), x_follower_key: Optional[str] = Header(default=None)):
    fs = follower_from_key(x_follower_key)
    await apply_settings(fs, validate_settings(body, ("multiplier", "max_contracts", "daily_loss_limit")), fs.id)
    return {"ok": True}


async def request_follower_account(fs: FollowerState, body: dict, changed_by: str) -> dict:
    if fs.ws is None:
        raise HTTPException(status_code=409, detail="That follower's NinjaTrader isn't connected, so the account can't be changed right now.")
    account = str(body.get("account", "")).strip()
    if not account or "|" in account:
        raise HTTPException(status_code=400, detail="Pick an account first.")
    if fs.accounts and account not in fs.accounts:
        raise HTTPException(status_code=400, detail=f"{account} isn't connected in that follower's NinjaTrader.")
    await send(fs, "account", account)
    await alert(f"{fs.id} switching follower account to {account} (was {fs.account}), by {changed_by}")
    return {"ok": True}


@app.post("/me/follower/account")
async def my_follower_account(body: dict = Body(...), x_follower_key: Optional[str] = Header(default=None)):
    fs = follower_from_key(x_follower_key)
    return await request_follower_account(fs, body, fs.id)


@app.post("/admin/followers/{follower_id}/account")
async def admin_follower_account(follower_id: str, body: dict = Body(...),
                                 x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    return await request_follower_account(get_follower(follower_id), body, "admin")


@app.post("/me/follower/stop")
async def my_follower_stop(flatten: bool = False, x_follower_key: Optional[str] = Header(default=None)):
    fs = follower_from_key(x_follower_key)
    fs.paused = True
    fs.pause_reason = "manual"
    if flatten:
        await send(fs, "flatten", "ALL")
    await alert(f"{fs.id} stopped copying from the follower page (closed positions: {flatten})")
    return {"ok": True}


@app.post("/me/follower/start")
async def my_follower_start(x_follower_key: Optional[str] = Header(default=None)):
    fs = follower_from_key(x_follower_key)
    fs.paused = False
    fs.pause_reason = None
    fs.strikes = 0
    await alert(f"{fs.id} resumed copying from the follower page")
    await check_loss_limit(fs)
    return {"ok": True, "paused": fs.paused}
