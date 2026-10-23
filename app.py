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


def market_open(now: Optional[datetime] = None) -> bool:
    """CME futures: Sunday 6 pm to Friday 5 pm New York time, with a daily break from 5 to 6 pm."""
    t = now or datetime.now(NEW_YORK)
    wd, hm = t.weekday(), t.hour * 60 + t.minute   # Monday = 0
    if wd == 5:                      # Saturday
        return False
    if wd == 6:                      # Sunday: opens at 6 pm
        return hm >= 18 * 60
    if wd == 4 and hm >= 17 * 60:    # Friday after 5 pm
        return False
    return not (17 * 60 <= hm < 18 * 60)


PNL_WRITE_EVERY_SEC = 15
_pnl_last_write: dict = {}


def record_pnl(kind: str, account_id: str, grp: str, nt_account: Optional[str], pnl: Optional[float]) -> None:
    """Keeps each account's latest P&L for the current trading day. Nothing is written while the market is
    closed, so the number saved at the 5 pm ET close is the day's final P&L even if NinjaTrader resets it later."""
    if pnl is None or not nt_account or not market_open():
        return
    now = time.time()
    cache_key = account_id + "|" + nt_account
    if now - _pnl_last_write.get(cache_key, 0) < PNL_WRITE_EVERY_SEC:
        return
    _pnl_last_write[cache_key] = now
    DB.execute("insert or replace into daily_pnl_v2 (day, nt_account, account_id, kind, grp, pnl, updated) "
               "values (?, ?, ?, ?, ?, ?, ?)",
               (trading_day(), nt_account, account_id, kind, grp, float(pnl), now))
    DB.commit()


def pnl_history(start: Optional[str], end: Optional[str], ids: Optional[list] = None) -> dict:
    today = trading_day()
    try:
        end_d = datetime.strptime(end, "%Y-%m-%d").date() if end else datetime.strptime(today, "%Y-%m-%d").date()
        start_d = datetime.strptime(start, "%Y-%m-%d").date() if start else end_d - timedelta(days=13)
    except ValueError:
        raise HTTPException(status_code=400, detail="Dates must look like 2026-10-20.")
    if start_d > end_d:
        start_d, end_d = end_d, start_d
    if (end_d - start_d).days > 400:
        raise HTTPException(status_code=400, detail="Pick a range of about a year or less.")
    q = "select day, account_id, kind, grp, nt_account, pnl from daily_pnl_v2 where day between ? and ?"
    args = [start_d.isoformat(), end_d.isoformat()]
    if ids is not None:
        if not ids:
            return {"start": args[0], "end": args[1], "today": today, "rows": []}
        q += " and account_id in (%s)" % ",".join("?" * len(ids))
        args += ids
    rows = [dict(zip(("day", "id", "kind", "group", "account", "pnl"), r))
            for r in DB.execute(q + " order by day, kind, grp, nt_account", args)]
    return {"start": args[0], "end": args[1], "today": today, "rows": rows}


RECON_STRIKES = 3               # consecutive 5s checks with mismatched positions before alerting

with open(CONFIG_PATH) as f:
    CFG = json.load(f)

DB = sqlite3.connect(DB_PATH, check_same_thread=False)
DB.execute("create table if not exists events (ts real, source text, who text, body text)")
DB.execute("create table if not exists settings (k text primary key, v text)")
# Daily P&L is kept per NinjaTrader account number, so each account has its own history even if a
# copier slot is switched to a different account.
DB.execute("""create table if not exists daily_pnl_v2 (
    day text, nt_account text, account_id text, kind text, grp text, pnl real, updated real,
    primary key (day, nt_account, account_id))""")
_old = DB.execute("select name from sqlite_master where type='table' and name='daily_pnl'").fetchone()
if _old and not DB.execute("select 1 from daily_pnl_v2 limit 1").fetchone():
    DB.execute("""insert or ignore into daily_pnl_v2 (day, nt_account, account_id, kind, grp, pnl, updated)
                  select day, nt_account, account_id, kind, grp, pnl, updated from daily_pnl where nt_account != ''""")
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


EDITABLE = ("multiplier", "max_contracts", "daily_loss_limit", "daily_profit_target", "master_id")
DAILY_STOP_REASONS = ("loss_limit", "profit_target")   # pauses that end automatically at the next session


class FollowerState:
    def __init__(self, cfg: dict):
        self.cfg = dict(cfg)
        saved = load_setting(f"follower:{cfg['id']}")
        if saved:  # changes made in the control panel override config.json
            self.cfg.update({k: v for k, v in saved.items() if k in EDITABLE})
        if self.cfg.get("master_id") not in MASTER_IDS:  # a saved master that no longer exists
            self.cfg["master_id"] = cfg.get("master_id") if cfg.get("master_id") in MASTER_IDS else MASTER_IDS[0]
        self.id = cfg["id"]
        self.ws: Optional[WebSocket] = None
        # A stop/pause made on the pages or admin page is saved, so it survives restarts and redeploys.
        self.paused = bool((saved or {}).get("paused", False))
        self.pause_reason: Optional[str] = "manual" if self.paused else None   # "loss_limit", "profit_target" or "manual"
        self.loss_day: Optional[str] = None       # trading day a daily limit was hit
        self.last_hb = 0.0
        self.pnl = 0.0
        self.open_pnl: Optional[float] = None   # open-position P&L (None until the add-on reports it)
        self.positions: dict = {}
        self.account: Optional[str] = None
        self.accounts: list = []
        self.last_error: Optional[str] = None
        self.last_error_at = 0.0
        self.strikes = 0
        self.stale_alerted = False


def set_manual_pause(fs: "FollowerState", paused: bool) -> None:
    """Pause or resume a follower and save it, so the choice survives restarts and redeploys.
    (Daily loss limit / max profit stops are not saved here: they end on their own at the next session.)"""
    fs.paused = paused
    fs.pause_reason = "manual" if paused else None
    saved = load_setting(f"follower:{fs.id}") or {}
    if saved.get("paused", False) != paused:
        saved["paused"] = paused
        save_setting(f"follower:{fs.id}", saved)


MASTERS = {m["key"]: m for m in CFG["masters"]}
MASTER_IDS = [m["id"] for m in CFG["masters"]]


def group_of(cfg: dict) -> str:
    """Accounts with the same "group" in config.json sign in together (one login shows all of them)."""
    return cfg.get("group") or cfg["id"]
MASTER_STATE = {
    m["id"]: {"last_hb": 0.0, "positions": {}, "offline_alerted": False,
              "account": None, "accounts": [], "commands": [], "orders": [],
              "pnl": None, "open_pnl": None, "locked_day": None, "armed": True, "last_flatten_at": 0.0,
              "paused": bool((load_setting(f"master:{m['id']}") or {}).get("paused", False)),
              "daily_profit_target": float((load_setting(f"master:{m['id']}") or {}).get(
                  "daily_profit_target", m.get("daily_profit_target", 0)) or 0)}
    for m in CFG["masters"]
}


def master_locked(ms: dict) -> bool:
    return ms["locked_day"] == trading_day()
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


ORDER_FIELDS = ("id", "instrument", "action", "type", "qty", "limit", "stop", "oco", "tif")


def parse_orders(text: str) -> list:
    out = []
    for spec in filter(None, text.split(";")):
        f = spec.split("~")
        if len(f) >= 9:
            o = dict(zip(ORDER_FIELDS, f[:9]))
            o["qty"] = int(o["qty"])
            out.append(o)
    return out


def scaled_orders(orders: list, fs: "FollowerState") -> list:
    return [dict(o, qty=scale_qty(o["qty"], fs)) for o in orders]


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
    ms = MASTER_STATE[master_id]
    if kind != "cancel" and (master_locked(ms) or ms["paused"]):
        return  # master paused, or hit its daily max profit: nothing new is copied
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


async def follow_master_flat(master_id: str, inst: str) -> None:
    if MASTER_STATE[master_id]["paused"]:
        return  # copying is paused from the master page: the master trades on its own
    # Master orders still working in this instrument (e.g. a new entry limit) must not be cleared.
    keep = ",".join(o["id"] for o in MASTER_STATE[master_id]["orders"] if o["instrument"] == inst)
    sent = []
    for fs in FOLLOWERS:
        if fs.cfg["master_id"] != master_id or fs.paused or fs.ws is None:
            continue
        await send(fs, "flatcheck", f"{inst}|{keep}")
        sent.append(fs.id)
    if sent:  # happens on every exit, so log it instead of sending an alert
        log.info("Master went flat in %s; flat check sent to %s", inst, sent)


async def check_master_profit(master_id: str, ms: dict) -> None:
    target = float(ms["daily_profit_target"] or 0)
    pnl = ms["pnl"]
    if pnl is None:
        return
    if target <= 0 or pnl < target:
        ms["armed"] = True  # P&L is below the target (e.g. after the daily reset), so it can trigger again
    if target > 0 and pnl >= target and ms["armed"] and not master_locked(ms):
        ms["locked_day"] = trading_day()
        ms["armed"] = False
        await alert(f"Master {master_id} hit its daily max profit (P&L {pnl:.2f}); flattening the master and all "
                    f"followers, copying stopped until the next session (6 pm ET)")
        for fs in FOLLOWERS:
            if fs.cfg["master_id"] == master_id and fs.ws is not None and not fs.paused:
                await send(fs, "flatten", "ALL")
    # While locked, keep the master flat (re-check at most every 10 seconds).
    if master_locked(ms) and ms["positions"] and time.time() - ms["last_flatten_at"] > 10:
        ms["last_flatten_at"] = time.time()
        if not any(c == "flatten|ALL" for _, c in ms["commands"]):
            queue_master_command(master_id, "flatten|ALL")


async def set_master_profit_target(master_id: str, body: dict, by: str) -> dict:
    try:
        target = float(body["daily_profit_target"])
    except (KeyError, TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Please enter a number.")
    if target < 0:
        raise HTTPException(status_code=400, detail="The daily max profit can't be negative (use 0 for no limit).")
    ms = MASTER_STATE[master_id]
    ms["daily_profit_target"] = target
    saved = load_setting(f"master:{master_id}") or {}
    saved["daily_profit_target"] = target
    save_setting(f"master:{master_id}", saved)
    await alert(f"Master {master_id} daily max profit set to {target:.0f} by {by}")
    await check_master_profit(master_id, ms)
    return {"ok": True}


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
    if not (fs.paused and fs.pause_reason in DAILY_STOP_REASONS) or trading_day() == fs.loss_day:
        return
    if fs.ws is None or now - fs.last_hb > FOLLOWER_TIMEOUT_SEC:
        return  # wait for a fresh heartbeat so the P&L is today's
    if daily_limit_hit(fs):
        return  # NinjaTrader hasn't reset the day's P&L yet; check again in 5 seconds
    reason = "loss limit" if fs.pause_reason == "loss_limit" else "max profit"
    set_manual_pause(fs, False)
    fs.strikes = 0
    await alert(f"{fs.id}: new trading session, copying resumed automatically after yesterday's daily {reason}")


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
        before = ms["positions"]
        ms["positions"] = parse_positions(parts[1] if len(parts) > 1 else "")
        switched = False
        if len(parts) > 2 and parts[2]:
            if ms["account"] and ms["account"] != parts[2]:
                switched = True
                await alert(f"Master {master['id']} now copying from account {parts[2]} (was {ms['account']})")
            ms["account"] = parts[2]
        if len(parts) > 3:
            ms["accounts"] = sorted(a for a in parts[3].split(",") if a)
        if len(parts) > 4:
            ms["orders"] = parse_orders(parts[4])
        # The master went flat in an instrument (including a broker/prop-firm auto-liquidation,
        # which never shows up as a copyable order): make sure followers are flat there too,
        # and clear their leftover copied stops/targets.
        if not switched:
            gone_flat = [i for i, q in before.items() if q and not ms["positions"].get(i)]
            for inst in gone_flat:
                await follow_master_flat(master["id"], inst)
        if len(parts) > 5 and parts[5]:
            try:
                ms["pnl"] = float(parts[5])
            except ValueError:
                pass
        if len(parts) > 6:
            try:
                ms["open_pnl"] = float(parts[6]) if parts[6] else None
            except ValueError:
                pass
        record_pnl("master", master["id"], group_of(master), ms["account"], ms["pnl"])
        await check_master_profit(master["id"], ms)
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
                if len(parts) > 5:
                    try:
                        fs.open_pnl = float(parts[5]) if parts[5] else None
                    except ValueError:
                        pass
                record_pnl("follower", fs.id, group_of(fs.cfg), fs.account, fs.pnl)
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


def daily_limit_hit(fs: FollowerState) -> Optional[str]:
    loss = float(fs.cfg.get("daily_loss_limit") or 0)
    profit = float(fs.cfg.get("daily_profit_target") or 0)
    if loss > 0 and fs.pnl <= -loss:
        return "loss_limit"
    if profit > 0 and fs.pnl >= profit:
        return "profit_target"
    return None


async def check_loss_limit(fs: FollowerState) -> None:
    """Checks both the daily loss limit and the daily max profit."""
    hit = daily_limit_hit(fs)
    if hit and not fs.paused:
        fs.paused = True
        fs.pause_reason = hit
        fs.loss_day = trading_day()
        await send(fs, "flatten", "ALL")
        what = "daily loss limit" if hit == "loss_limit" else "daily max profit"
        await alert(f"{fs.id} hit {what} (P&L {fs.pnl:.2f}); flattened and paused until the next session (6 pm ET)")


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
                "orders": ms["orders"],
                "pnl": ms["pnl"],
                "open_pnl": ms["open_pnl"],
                "daily_profit_target": ms["daily_profit_target"],
                "locked": master_locked(ms),
                "paused": ms["paused"],
            }
            for mid, ms in MASTER_STATE.items()
        },
        "followers": {
            fs.id: {
                "connected": fs.ws is not None,
                "paused": fs.paused,
                "pause_reason": fs.pause_reason,
                "pnl": fs.pnl,
                "open_pnl": fs.open_pnl,
                "positions": fs.positions,
                "master": fs.cfg["master_id"],
                "group": group_of(fs.cfg),
                "mismatch": fs.strikes >= RECON_STRIKES,
                "account": fs.account,
                "accounts": fs.accounts,
                "last_error": fs.last_error if time.time() - fs.last_error_at < 60 else None,
                "multiplier": float(fs.cfg.get("multiplier", 1.0)),
                "max_contracts": int(fs.cfg.get("max_contracts", 1)),
                "daily_loss_limit": float(fs.cfg.get("daily_loss_limit") or 0),
                "daily_profit_target": float(fs.cfg.get("daily_profit_target") or 0),
            }
            for fs in FOLLOWERS
        },
    }


def validate_settings(body: dict, fields: tuple) -> dict:
    """Checks only the settings present in the request, so a page can change one thing (e.g. the master)."""
    def has(name: str) -> bool:
        return name in fields and name in body and body[name] is not None and body[name] != ""
    out = {}
    try:
        if has("multiplier"):
            m = float(body["multiplier"])
            if not m.is_integer() or not 1 <= m <= 50:
                raise HTTPException(status_code=400, detail="The multiplier must be a whole number from 1 to 50.")
            out["multiplier"] = int(m)
        if has("max_contracts"):
            out["max_contracts"] = int(body["max_contracts"])
            if not 1 <= out["max_contracts"] <= 500:
                raise HTTPException(status_code=400, detail="Max contracts must be between 1 and 500.")
        if has("daily_loss_limit"):
            out["daily_loss_limit"] = float(body["daily_loss_limit"])
            if out["daily_loss_limit"] < 0:
                raise HTTPException(status_code=400, detail="The daily loss limit can't be negative (use 0 for no limit).")
        if has("daily_profit_target"):
            out["daily_profit_target"] = float(body["daily_profit_target"])
            if out["daily_profit_target"] < 0:
                raise HTTPException(status_code=400, detail="The daily max profit can't be negative (use 0 for no limit).")
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Please enter numbers only.")
    if has("master_id"):
        if body["master_id"] not in MASTER_IDS:
            raise HTTPException(status_code=400, detail="That master account doesn't exist.")
        out["master_id"] = body["master_id"]
    if not out:
        raise HTTPException(status_code=400, detail="Nothing to save.")
    return out


async def apply_settings(fs: FollowerState, new: dict, changed_by: str) -> None:
    if "master_id" in new and new["master_id"] != fs.cfg.get("master_id"):
        fs.strikes = 0
    elif "master_id" in new:
        del new["master_id"]  # unchanged
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
        set_manual_pause(fs, True)
        await send(fs, "flatten", "ALL")
    await alert("KILL SWITCH: all followers flattened and paused")
    return {"ok": True}


@app.post("/admin/kill-all")
async def kill_all(x_admin_key: Optional[str] = Header(default=None)):
    """Flatten and pause every follower, AND flatten the master account(s)."""
    require_admin(x_admin_key)
    for fs in FOLLOWERS:
        set_manual_pause(fs, True)
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
    set_manual_pause(fs, True)
    if flatten:
        await send(fs, "flatten", "ALL")
    await alert(f"{fs.id} paused (flatten={flatten})")
    return {"ok": True}


@app.post("/admin/resume/{follower_id}")
async def resume(follower_id: str, x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    fs = get_follower(follower_id)
    set_manual_pause(fs, False)
    fs.strikes = 0
    await alert(f"{fs.id} resumed")
    return {"ok": True}


# ---------------- self-service pages: /master and /follower ----------------
# Signing in with any key shows every account in that key's group. Each action names the account with ?id=.

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


def my_masters(key: Optional[str]) -> list:
    me = master_from_key(key)
    return [m for m in CFG["masters"] if group_of(m) == group_of(me)]


def my_master(key: Optional[str], master_id: Optional[str]) -> dict:
    mine = my_masters(key)
    if not master_id:
        return master_from_key(key)
    for m in mine:
        if m["id"] == master_id:
            return m
    raise HTTPException(status_code=404, detail="That account isn't one of yours.")


def my_followers(key: Optional[str]) -> list:
    me = follower_from_key(key)
    return [fs for fs in FOLLOWERS if group_of(fs.cfg) == group_of(me.cfg)]


def my_follower(key: Optional[str], follower_id: Optional[str]) -> FollowerState:
    if not follower_id:
        return follower_from_key(key)
    for fs in my_followers(key):
        if fs.id == follower_id:
            return fs
    raise HTTPException(status_code=404, detail="That account isn't one of yours.")


def master_slot_status(m: dict) -> dict:
    ms = MASTER_STATE[m["id"]]
    switching = next((c.split("|", 1)[1] for _, c in ms["commands"] if c.startswith("account|")), None)
    return {
        "id": m["id"],
        "online": master_online(ms),
        "account": ms["account"],
        "accounts": ms["accounts"],
        "switching_to": switching,
        "pnl": ms["pnl"],
        "open_pnl": ms["open_pnl"],
        "positions": ms["positions"],
        "daily_profit_target": ms["daily_profit_target"],
        "locked": master_locked(ms),
        "paused": ms["paused"],
        "followers_connected": any(fs.cfg["master_id"] == m["id"] and fs.ws is not None for fs in FOLLOWERS),
    }


@app.get("/me/master")
async def my_master_status(x_master_key: Optional[str] = Header(default=None)):
    return {"slots": [master_slot_status(m) for m in my_masters(x_master_key)]}


@app.post("/me/master/settings")
async def my_master_settings(body: dict = Body(...), id: Optional[str] = None,
                             x_master_key: Optional[str] = Header(default=None)):
    m = my_master(x_master_key, id)
    return await set_master_profit_target(m["id"], body, "master page")


@app.post("/admin/masters/{master_id}/settings")
async def admin_master_settings(master_id: str, body: dict = Body(...),
                                x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    if master_id not in MASTER_STATE:
        raise HTTPException(status_code=404, detail=f"no master {master_id}")
    return await set_master_profit_target(master_id, body, "admin")


def save_master_paused(master_id: str, paused: bool) -> None:
    MASTER_STATE[master_id]["paused"] = paused
    saved = load_setting(f"master:{master_id}") or {}
    saved["paused"] = paused
    save_setting(f"master:{master_id}", saved)


async def pause_master(master_id: str, flatten: bool, by: str) -> dict:
    ms = MASTER_STATE[master_id]
    save_master_paused(master_id, True)
    if flatten:
        queue_master_command(master_id, "flatten|ALL")
        for fs in FOLLOWERS:
            if fs.cfg["master_id"] == master_id and fs.ws is not None:
                await send(fs, "flatten", "ALL")
    note = "; master and followers flattened" if flatten else "; open positions left as they are"
    offline = flatten and not master_online(ms)
    await alert(f"Master {master_id}: copying paused by {by}{note}"
                + (" (master is offline, flatten it by hand)" if offline else ""))
    return {"ok": True, "master_offline": offline}


@app.post("/me/master/pause")
async def my_master_pause(flatten: bool = False, id: Optional[str] = None,
                          x_master_key: Optional[str] = Header(default=None)):
    m = my_master(x_master_key, id)
    return await pause_master(m["id"], flatten, "master page")


@app.post("/admin/masters/{master_id}/pause")
async def admin_master_pause(master_id: str, flatten: bool = False, x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    if master_id not in MASTER_STATE:
        raise HTTPException(status_code=404, detail=f"no master {master_id}")
    return await pause_master(master_id, flatten, "admin")


@app.post("/admin/masters/{master_id}/resume")
async def admin_master_resume(master_id: str, x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    if master_id not in MASTER_STATE:
        raise HTTPException(status_code=404, detail=f"no master {master_id}")
    save_master_paused(master_id, False)
    await alert(f"Master {master_id}: copying resumed by admin")
    return {"ok": True}


@app.post("/me/master/resume")
async def my_master_resume(id: Optional[str] = None, x_master_key: Optional[str] = Header(default=None)):
    m = my_master(x_master_key, id)
    save_master_paused(m["id"], False)
    await alert(f"Master {m['id']}: copying resumed from the master page")
    return {"ok": True}


@app.post("/me/master/account")
async def my_master_switch(body: dict = Body(...), id: Optional[str] = None,
                           x_master_key: Optional[str] = Header(default=None)):
    m = my_master(x_master_key, id)
    ms = MASTER_STATE[m["id"]]
    if not master_online(ms):
        raise HTTPException(status_code=409, detail="NinjaTrader on the master computer isn't connected, so the account can't be switched right now.")
    account = str(body.get("account", "")).strip()
    if not account or "|" in account:
        raise HTTPException(status_code=400, detail="Pick an account first.")
    if ms["accounts"] and account not in ms["accounts"]:
        raise HTTPException(status_code=400, detail=f"{account} isn't connected in NinjaTrader on the master computer.")
    in_use = [o for o in my_masters(x_master_key) if o["id"] != m["id"] and MASTER_STATE[o["id"]]["account"] == account]
    if in_use:
        raise HTTPException(status_code=400, detail=f"{account} is already used by {in_use[0]['id']}.")
    queue_master_command(m["id"], f"account|{account}")
    await alert(f"Master {m['id']}: switch to account {account} requested from the master page")
    return {"ok": True}


def follower_slot_status(fs: FollowerState) -> dict:
    ms = MASTER_STATE[fs.cfg["master_id"]]
    return {
        "id": fs.id,
        "ninjatrader_connected": fs.ws is not None,
        "copying": fs.ws is not None and not fs.paused,
        "paused": fs.paused,
        "pause_reason": fs.pause_reason,
        "pnl": fs.pnl,
        "open_pnl": fs.open_pnl,
        "positions": fs.positions,
        "account": fs.account,
        "accounts": fs.accounts,
        "last_error": fs.last_error if time.time() - fs.last_error_at < 60 else None,
        "multiplier": float(fs.cfg.get("multiplier", 1.0)),
        "max_contracts": int(fs.cfg.get("max_contracts", 1)),
        "daily_loss_limit": float(fs.cfg.get("daily_loss_limit") or 0),
        "daily_profit_target": float(fs.cfg.get("daily_profit_target") or 0),
        "master_id": fs.cfg["master_id"],
        "master": {
            "name": fs.cfg["master_id"],
            "online": master_online(ms),
            "locked": master_locked(ms),
            "paused": ms["paused"],
            "positions": {i: expected_position(p, fs) for i, p in ms["positions"].items() if p},
        },
        "mismatch": fs.strikes >= RECON_STRIKES,
    }


@app.get("/me/follower")
async def my_follower_status(x_follower_key: Optional[str] = Header(default=None)):
    return {
        "slots": [follower_slot_status(fs) for fs in my_followers(x_follower_key)],
        "masters": [{"id": mid, "online": master_online(MASTER_STATE[mid])} for mid in MASTER_IDS],
    }


@app.post("/me/follower/settings")
async def my_follower_settings(body: dict = Body(...), id: Optional[str] = None,
                               x_follower_key: Optional[str] = Header(default=None)):
    fs = my_follower(x_follower_key, id)
    fields = ("multiplier", "max_contracts", "daily_loss_limit", "daily_profit_target", "master_id")
    await apply_settings(fs, validate_settings(body, fields), fs.id)
    return {"ok": True}


async def request_follower_account(fs: FollowerState, body: dict, changed_by: str) -> dict:
    if fs.ws is None:
        raise HTTPException(status_code=409, detail="That follower's NinjaTrader isn't connected, so the account can't be changed right now.")
    account = str(body.get("account", "")).strip()
    if not account or "|" in account:
        raise HTTPException(status_code=400, detail="Pick an account first.")
    if fs.accounts and account not in fs.accounts:
        raise HTTPException(status_code=400, detail=f"{account} isn't connected in that follower's NinjaTrader.")
    in_use = [o for o in FOLLOWERS if o is not fs and group_of(o.cfg) == group_of(fs.cfg) and o.account == account]
    if in_use:
        raise HTTPException(status_code=400, detail=f"{account} is already used by {in_use[0].id}.")
    await send(fs, "account", account)
    await alert(f"{fs.id} switching follower account to {account} (was {fs.account}), by {changed_by}")
    return {"ok": True}


@app.post("/me/follower/account")
async def my_follower_account(body: dict = Body(...), id: Optional[str] = None,
                              x_follower_key: Optional[str] = Header(default=None)):
    fs = my_follower(x_follower_key, id)
    return await request_follower_account(fs, body, fs.id)


@app.post("/admin/followers/{follower_id}/account")
async def admin_follower_account(follower_id: str, body: dict = Body(...),
                                 x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    return await request_follower_account(get_follower(follower_id), body, "admin")


@app.post("/me/follower/stop")
async def my_follower_stop(flatten: bool = False, id: Optional[str] = None,
                           x_follower_key: Optional[str] = Header(default=None)):
    fs = my_follower(x_follower_key, id)
    set_manual_pause(fs, True)
    if flatten:
        await send(fs, "flatten", "ALL")
    await alert(f"{fs.id} stopped copying from the follower page (closed positions: {flatten})")
    return {"ok": True}


@app.post("/me/follower/start")
async def my_follower_start(id: Optional[str] = None, x_follower_key: Optional[str] = Header(default=None)):
    fs = my_follower(x_follower_key, id)
    set_manual_pause(fs, False)
    fs.strikes = 0
    await alert(f"{fs.id} resumed copying from the follower page")
    await check_loss_limit(fs)
    return {"ok": True, "paused": fs.paused}


# ---------- sync a follower to the master ----------

async def sync_follower(fs: FollowerState, by: str) -> dict:
    if fs.ws is None:
        raise HTTPException(status_code=409, detail="That follower's NinjaTrader isn't connected.")
    ms = MASTER_STATE[fs.cfg["master_id"]]
    if not master_online(ms):
        raise HTTPException(status_code=409, detail="The master is offline, so there's nothing reliable to sync to.")
    if fs.paused and fs.pause_reason in DAILY_STOP_REASONS:
        raise HTTPException(status_code=409, detail="Copying is stopped for today by a daily limit.")
    if master_locked(ms):
        raise HTTPException(status_code=409, detail="The master reached its daily max profit, so copying is stopped until the next session.")
    if ms["paused"]:
        raise HTTPException(status_code=409, detail="Copying is paused on the master.")
    targets = {i: expected_position(p, fs) for i, p in ms["positions"].items() if p}
    pos_text = ";".join(f"{i}={q}" for i, q in targets.items())
    orders_text = ";".join("~".join(str(o[k]) for k in ORDER_FIELDS) for o in scaled_orders(ms["orders"], fs))
    set_manual_pause(fs, False)
    fs.strikes = 0
    await send(fs, "sync", f"{pos_text}|{orders_text}")
    await alert(f"{fs.id} synced to master by {by}: target {targets or 'flat'}, {len(ms['orders'])} working order(s)")
    return {"ok": True}


@app.post("/admin/followers/{follower_id}/sync")
async def admin_sync(follower_id: str, x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    return await sync_follower(get_follower(follower_id), "admin")


@app.post("/me/follower/sync")
async def my_sync(id: Optional[str] = None, x_follower_key: Optional[str] = Header(default=None)):
    fs = my_follower(x_follower_key, id)
    return await sync_follower(fs, fs.id)


# ---------- daily P&L history ----------

@app.get("/admin/pnl")
async def admin_pnl(start: Optional[str] = None, end: Optional[str] = None,
                    x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    return pnl_history(start, end)


@app.get("/me/follower/pnl")
async def my_follower_pnl(start: Optional[str] = None, end: Optional[str] = None,
                          x_follower_key: Optional[str] = Header(default=None)):
    mine = [fs.id for fs in my_followers(x_follower_key)]
    return pnl_history(start, end, mine)
