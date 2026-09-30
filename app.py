"""
Trade copier relay server.

Master (NinjaTrader add-on)  --HTTP POST-->  this server  --WebSocket-->  Followers (NinjaTrader add-on)

Wire format is pipe-delimited plain text (easy to parse in NinjaScript):
  master -> server:
    order|new|<id>|<instrument>|<action>|<orderType>|<qty>|<limit>|<stop>|<oco>|<tif>
    order|change|<id>|<instrument>|<action>|<orderType>|<qty>|<limit>|<stop>|<oco>|<tif>
    order|cancel|<id>
    hb|<positions>                      positions = "NQ 12-26=1;ES 12-26=-2"
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
from typing import Optional

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("copier")

CONFIG_PATH = os.getenv("COPIER_CONFIG", "config.json")
DB_PATH = os.getenv("COPIER_DB", "copier.db")
ADMIN_KEY = os.getenv("ADMIN_KEY", "change-me")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
MASTER_TIMEOUT_SEC = 15      # no heartbeat for this long -> master offline alert
MASTER_FLATTEN_EXPIRY_SEC = 60  # a queued master flatten is dropped if the master doesn't pick it up in time
FOLLOWER_TIMEOUT_SEC = 15
RECON_STRIKES = 3            # consecutive 5s checks with mismatched positions before alerting

with open(CONFIG_PATH) as f:
    CFG = json.load(f)


class FollowerState:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.id = cfg["id"]
        self.ws: Optional[WebSocket] = None
        self.paused = False
        self.last_hb = 0.0
        self.pnl = 0.0
        self.positions: dict = {}
        self.strikes = 0
        self.stale_alerted = False


MASTERS = {m["key"]: m for m in CFG["masters"]}
MASTER_STATE = {m["id"]: {"last_hb": 0.0, "positions": {}, "offline_alerted": False, "flatten_requested_at": 0.0} for m in CFG["masters"]}
FOLLOWERS = [FollowerState(fc) for fc in CFG["followers"]]
FOLLOWER_BY_KEY = {fs.cfg["key"]: fs for fs in FOLLOWERS}
FOLLOWER_BY_ID = {fs.id: fs for fs in FOLLOWERS}
SEQ = itertools.count(1)

DB = sqlite3.connect(DB_PATH, check_same_thread=False)
DB.execute("create table if not exists events (ts real, source text, who text, body text)")
DB.commit()


def log_event(source: str, who: str, body: str) -> None:
    DB.execute("insert into events values (?, ?, ?, ?)", (time.time(), source, who, body))
    DB.commit()


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


async def monitor() -> None:
    while True:
        await asyncio.sleep(5)
        now = time.time()
        for mid, ms in MASTER_STATE.items():
            if ms["last_hb"] and now - ms["last_hb"] > MASTER_TIMEOUT_SEC and not ms["offline_alerted"]:
                ms["offline_alerted"] = True
                await alert(f"Master {mid} stopped sending heartbeats")
        for fs in FOLLOWERS:
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
        return master_reply(master["id"], ms)

    log_event("master", master["id"], body)
    if parts[0] == "order" and len(parts) >= 3 and parts[1] in ("new", "change", "cancel"):
        if parts[1] != "cancel" and len(parts) < 11:
            raise HTTPException(status_code=400, detail="malformed order message")
        await fan_out(master["id"], parts)
    return master_reply(master["id"], ms)


def master_reply(master_id: str, ms: dict) -> PlainTextResponse:
    """Commands for the master ride back on the response to its next POST (heartbeats come every 5s)."""
    requested = ms["flatten_requested_at"]
    if requested:
        ms["flatten_requested_at"] = 0.0
        if time.time() - requested <= MASTER_FLATTEN_EXPIRY_SEC:
            log_event("sent", master_id, "flatten|ALL")
            return PlainTextResponse("flatten|ALL")
        log_event("expired", master_id, "flatten|ALL")
    return PlainTextResponse("ok")


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
    try:
        while True:
            text = await ws.receive_text()
            parts = text.split("|")
            if parts[0] == "hb" and len(parts) >= 3:
                fs.last_hb = time.time()
                fs.pnl = float(parts[1])
                fs.positions = parse_positions(parts[2])
                limit = fs.cfg.get("daily_loss_limit")
                if limit and fs.pnl <= -abs(float(limit)) and not fs.paused:
                    fs.paused = True
                    await send(fs, "flatten", "ALL")
                    await alert(f"{fs.id} hit daily loss limit (P&L {fs.pnl:.2f}); flattened and paused")
            elif parts[0] == "ack":
                log_event("ack", fs.id, text)
                if len(parts) > 2 and parts[2] == "err":
                    await alert(f"{fs.id} rejected seq {parts[1]}: {'|'.join(parts[3:])}")
    except WebSocketDisconnect:
        pass
    finally:
        if fs.ws is ws:
            fs.ws = None
            await alert(f"Follower {fs.id} disconnected")


def require_admin(key: Optional[str]) -> None:
    if key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="bad admin key")


@app.get("/status")
async def status(x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    now = time.time()
    return {
        "masters": {
            mid: {"seconds_since_hb": round(now - ms["last_hb"], 1) if ms["last_hb"] else None,
                  "positions": ms["positions"]}
            for mid, ms in MASTER_STATE.items()
        },
        "followers": {
            fs.id: {"connected": fs.ws is not None, "paused": fs.paused, "pnl": fs.pnl,
                    "positions": fs.positions, "master": fs.cfg["master_id"]}
            for fs in FOLLOWERS
        },
    }


@app.post("/admin/kill")
async def kill_switch(x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    for fs in FOLLOWERS:
        fs.paused = True
        await send(fs, "flatten", "ALL")
    await alert("KILL SWITCH: all followers flattened and paused")
    return {"ok": True}


@app.post("/admin/kill-all")
async def kill_all(x_admin_key: Optional[str] = Header(default=None)):
    """Flatten and pause every follower, AND flatten the master account(s)."""
    require_admin(x_admin_key)
    for fs in FOLLOWERS:
        fs.paused = True
        await send(fs, "flatten", "ALL")
    now = time.time()
    offline = []
    for mid, ms in MASTER_STATE.items():
        ms["flatten_requested_at"] = now
        if not ms["last_hb"] or now - ms["last_hb"] > MASTER_TIMEOUT_SEC:
            offline.append(mid)
    note = f" WARNING: master(s) {offline} offline - flatten them by hand." if offline else ""
    await alert("KILL ALL: followers flattened and paused; master flatten sent (applies within ~5s)." + note)
    return {"ok": True, "masters_offline": offline}


@app.post("/admin/pause/{follower_id}")
async def pause(follower_id: str, flatten: bool = False, x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    fs = FOLLOWER_BY_ID.get(follower_id) or _missing(follower_id)
    fs.paused = True
    if flatten:
        await send(fs, "flatten", "ALL")
    await alert(f"{fs.id} paused (flatten={flatten})")
    return {"ok": True}


@app.post("/admin/resume/{follower_id}")
async def resume(follower_id: str, x_admin_key: Optional[str] = Header(default=None)):
    require_admin(x_admin_key)
    fs = FOLLOWER_BY_ID.get(follower_id) or _missing(follower_id)
    fs.paused = False
    fs.strikes = 0
    await alert(f"{fs.id} resumed")
    return {"ok": True}


def _missing(follower_id: str):
    raise HTTPException(status_code=404, detail=f"no follower {follower_id}")
