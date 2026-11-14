import asyncio
import sys
import time
import json
import os
import math
import re
from datetime import datetime
from typing import Dict, Any, Optional, List

# High-performance async I/O loop on Linux environments
if sys.platform != "win32":
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import httpx

from bot.config import settings, CONFIG_PATH
import bot.data as data
import bot.ws_data as ws_data
import bot.chainlink as chainlink
import bot.indicators as indicators
import bot.engines as engines
import bot.utils as utils
from bot.clob_trader import clob_trader
import bot.copy_fak as copy_fak            # PATCH: fill-and-kill copy
import bot.longshot as longshot          # PATCH: longshot
import bot.ls_maker as ls_maker          # PATCH: longshot maker
import bot.polybolt as polybolt          # PATCH: price stream
from types import SimpleNamespace        # PATCH: longshot maker

STATE_PATH = "state_data.json"
SIGNALS_PATH = os.path.join("logs", "signals.csv")
LOG_FILE_PATH = os.path.join("logs", "app.log")
TELEGRAM_SUBS_PATH = "telegram_subscribers.json"

# ── WebSocket broadcast clients for real-time live PnL & dashboard ────────────
_ws_clients = set()

# ── Event-driven early entries & wakeups ───────────────────────────────────────
MIN_EVAL_INTERVAL_S = 0.0   # was 0.25 (at most 4 checks a second, so a tick could wait up to 250 ms); every tick is checked now
CTX_MAX_AGE_S = 4.0        # 4.0s context validity from TOG-main
POLY_WS_MAX_AGE_MS = 2500
MARK_CAPTURE_WINDOW_MS = 20_000

_market_event = asyncio.Event()
_entry_lock = asyncio.Lock()
_trade_lock = asyncio.Lock()
_in_flight_markets = set()
_last_eval_ts = 0.0
_loop_wake = asyncio.Event()   # runs update_loop's next pass now (set when the new window's first Chainlink price arrives)
STRIKE_WAIT_MS = 2500          # a Chainlink price stamped before the window opened is only used as the strike after this
NEXT_MARKET_PREFETCH_MS = 60_000   # look up the next window's market this long before it opens
ORDER_CONN_PING_S = 15.0       # keep the order connection to Polymarket open (see order_conn_keeper)
_logged_once: Dict[str, float] = {}


def _log_once(key: str, msg: str):
    """log_message once per key (keys include the market, so once per window): lines that
    would otherwise repeat on every tick."""
    if key in _logged_once:
        return
    if len(_logged_once) > 500:
        _logged_once.clear()
    _logged_once[key] = time.time()
    log_message(msg)


# ── Long shots (PATCH: longshot) ──────────────────────────────────────────────
_ls_lock = asyncio.Lock()
_ls_entered_windows = set()   # window start_ms values where a long shot was entered


def _is_longshot(t: Dict[str, Any]) -> bool:
    return t.get("kind") == "longshot"


def _is_ls_maker(t: Dict[str, Any]) -> bool:
    """PATCH: longshot maker — a long shot bought by a resting maker bid (bot/ls_maker.py)."""
    return t.get("open_reason") == "longshot_maker"


def _slot_keys(market: Dict[str, Any], kind: str = "regular") -> List[str]:
    """In-flight keys for a market. Long shots use their own ("ls:"-prefixed) keys, so a
    long shot and a regular trade never block each other."""
    mid, slug = str(market.get("id") or ""), str(market.get("slug") or "")
    pre = "ls:" if kind == "longshot" else ""
    return [pre + k for k in (mid, slug) if k]


def _model_steps(minutes_left: float):
    if settings.CANDLE_WINDOW_MINUTES <= 5:
        return max(0.05, round(minutes_left / 5.0, 4))
    return max(1, math.ceil(minutes_left / 5))

def _sync_active_trades_to_latest_data():
    if "trading_state" in state.get("latest_data", {}):
        ts = state["latest_data"]["trading_state"]
        ts["candle_window_minutes"] = settings.CANDLE_WINDOW_MINUTES
        ts["active_trades"] = list(state["active_trades"])
        live_open_val = 0.0
        for t in state["active_trades"]:
            if t.get("live_status") == "FILLED" and t.get("live_shares"):
                try:
                    s = float(t.get("live_shares") or 0.0)
                    p = float(t.get("live_mark_price") or t.get("live_entry_price") or 0.0)
                    live_open_val += s * p
                except (ValueError, TypeError):
                    pass
        ts["open_value"] = live_open_val
        live_bal = float(state.get("live_balance") or 0.0)
        ts["equity"] = live_bal + live_open_val
        ts["balance"] = live_bal
        ts["has_live_creds"] = bool(settings.PRIVATE_KEY)

_last_spot_broadcast_ts = 0.0
_last_broadcast_spot_price = 0.0

def _wake_entry(data_payload=None):
    global _last_spot_broadcast_ts, _last_broadcast_spot_price
    _market_event.set()
    if data_payload and isinstance(data_payload, dict):
        source = str(data_payload.get("source", ""))
        p = data_payload.get("price")
        if p and p > 0:
            if "spot" in source or not state.get("last_spot_price"):
                state["last_spot_price"] = p
                prices = state.setdefault("latest_data", {}).setdefault("prices", {})
                prices["spot"] = p
                now = time.time()
                if (now - _last_spot_broadcast_ts >= 0.25 and _ws_clients
                        and abs(p - _last_broadcast_spot_price) >= 0.01):
                    _last_spot_broadcast_ts = now
                    _last_broadcast_spot_price = p
                    asyncio.create_task(broadcast_state())

def _on_poly_chainlink_tick(data_payload=None):
    """Polymarket's Chainlink feed (where the strike comes from). Checks entries on it, and
    when the window that just opened has no strike yet and this price is from the new
    window, runs update_loop's next pass now instead of up to POLL_INTERVAL_MS later.
    PATCH: longshot — every price also goes into the long-shot recorder (the 60-second
    averages), stamped with the price's own time (never later than now)."""
    _market_event.set()
    if not isinstance(data_payload, dict):
        return
    _p = data_payload.get("price")
    if _p and _p > 0:
        _ts = data_payload.get("updatedAt")
        longshot.record_price(min((float(_ts) / 1000.0) if _ts else time.time(), time.time()), _p)
    w_ms = int(settings.CANDLE_WINDOW_MINUTES * 60_000)
    start = int(time.time() * 1000) // w_ms * w_ms
    win = state.get("market_opens", {}).get(start)
    if (win is None or win.get("chainlink") is None) and float(data_payload.get("updatedAt") or 0) >= start:
        _loop_wake.set()


def _on_chainlink_tick(data_payload=None):
    if data_payload and isinstance(data_payload, dict):
        p = data_payload.get("price")
        if p and p > 0:
            state["last_chainlink_price"] = p
            prices = state.setdefault("latest_data", {}).setdefault("prices", {})
            prices["chainlink"] = p
            prices["chainlink_source"] = data_payload.get("source", "Chainlink RPC WS")
            _market_event.set()
            asyncio.create_task(broadcast_state())


# ── Global state ───────────────────────────────────────────────────────────────
state = {
        "latest_data": {
        "timestamp": datetime.now().isoformat(),
        "timing": {},
        "market": None,
        "trading_state": {
            "mode": "hybrid",
            "running": False,
            "balance": 1000.0,
            "equity": 1000.0,
            "open_value": 0.0,
            "active_trades": [],
            "history_count": 0,
            "risk": {"type": settings.RISK_TYPE, "value": settings.RISK_VALUE},
            "symbol": settings.SYMBOL,
            "has_live_creds": bool(settings.PRIVATE_KEY),
            "withdraw_state": "idle",
            "last_withdrawal": None,
            "withdraw": {
                "enabled": settings.AUTO_WITHDRAW_ENABLED,
                "state": "ARMED",
                "amount": settings.WITHDRAW_AMOUNT,
                "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE
            }
        },
        "prices": {
            "spot": None,
            "chainlink": None,
            "chainlink_source": "Connecting...",
            "poly_up": None,
            "poly_down": None,
            "window_open": None,
            "window_open_source": None,
            "model_open": None,
            "window_start_ms": None,
            "price_is_fresh": True,
            "book_source": None
        },
        "indicators": {
            "rsi": None,
            "heiken": {"color": None, "count": 0},
            "heiken_5m": {"color": None, "count": 0},
            "fair": None
        },
        "analysis": {
            "probability": {"adjustedUp": 0.5, "adjustedDown": 0.5},
            "edge": {"edgeUp": None, "edgeDown": None},
            "decision": {"action": "NO_TRADE", "reason": "initializing"}
        }
    },
    "last_update_ts": 0,
    "trading_mode": "hybrid",
    "paper_balance": 1000.0,
    "live_balance": 0.0,
    "precomputed_live_stake": 0.0,
    "active_trades": [],
    "trade_history": [],
    "logs": [],
    "log_seq": 0,
    "last_trade_side": None,
    "last_balance_refresh": 0,
    # Trading is OFF until user presses Start.
    "running": False,
    "market_opens": {},
    "last_window_start": None,
    "last_seen_price": None,
    # Capital extractor
    "withdraw_state": "idle",  # "idle" | "in_progress" | "cooldown"
    "last_withdrawal": None,
    "withdraw_flat_since": None,
    "withdraw_submitted_at": None,
    "withdraw_locked_market": None,
    # Telegram
    "telegram_subscribers": [],
    # Event-driven execution
    "trade_ctx": None,
    "event_exec": None,
}

def log_message(msg: str):
    now_dt = datetime.now()
    timestamp = now_dt.strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {msg}"
    print(formatted)
    state["logs"].append(formatted)
    state["log_seq"] = state.get("log_seq", 0) + 1
    if len(state["logs"]) > 200:
        state["logs"].pop(0)

    try:
        os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
        date_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{date_str}] {msg}\n")
    except Exception:
        pass

clob_trader.set_log_hook(log_message)  # LIVE WALLET / LIVE SIGNER lines go to the app log

def save_state():
    try:
        data_to_save = {
            "paper_balance": state["paper_balance"],
            "live_balance": state.get("live_balance", 0.0),
            "active_trades": state["active_trades"],
            "trade_history": state["trade_history"],
            "last_trade_side": state["last_trade_side"],
            "last_withdrawal": state.get("last_withdrawal"),
            "ls_maker_open": state.get("ls_maker_open", []),  # PATCH: longshot maker — cancelled at the next start
        }
        with open(STATE_PATH, "w") as f:
            json.dump(data_to_save, f, indent=2)
    except Exception as e:
        print(f"Error saving state: {e}")

def load_state():
    try:
        load_telegram_subscribers()
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r") as f:
                loaded = json.load(f)
                state["paper_balance"] = loaded.get("paper_balance", 1000.0)
                state["live_balance"] = loaded.get("live_balance", 0.0)
                state["active_trades"] = loaded.get("active_trades", [])
                state["trade_history"] = loaded.get("trade_history", [])
                state["last_trade_side"] = loaded.get("last_trade_side")
                state["last_withdrawal"] = loaded.get("last_withdrawal")
                state["ls_maker_open"] = loaded.get("ls_maker_open") or []  # PATCH: longshot maker
                log_message(f"State loaded from {STATE_PATH}")
    except Exception as e:
        print(f"Error loading state: {e}")

# ── Telegram notifications & poller ───────────────────────────────────────────
def _normalize_subscriber(s: Any) -> Optional[Dict[str, Any]]:
    if isinstance(s, dict):
        cid = s.get("chat_id")
        if cid is not None:
            try:
                return {
                    "chat_id": int(cid),
                    "name": str(s.get("name") or f"Chat {cid}"),
                    "type": str(s.get("type") or "chat"),
                    "subscribed_at": s.get("subscribed_at")
                }
            except (ValueError, TypeError):
                return None
    elif isinstance(s, (int, str)):
        try:
            cid = int(s)
            return {
                "chat_id": cid,
                "name": f"Chat {cid}",
                "type": "chat",
                "subscribed_at": None
            }
        except (ValueError, TypeError):
            return None
    return None

def load_telegram_subscribers() -> List[Dict[str, Any]]:
    try:
        if os.path.exists(TELEGRAM_SUBS_PATH):
            with open(TELEGRAM_SUBS_PATH, "r") as f:
                subs = json.load(f)
                if isinstance(subs, list):
                    normalized = []
                    for s in subs:
                        norm = _normalize_subscriber(s)
                        if norm and not any(x["chat_id"] == norm["chat_id"] for x in normalized):
                            normalized.append(norm)
                    state["telegram_subscribers"] = normalized
                    return state["telegram_subscribers"]
    except Exception as e:
        print(f"Error loading telegram subscribers: {e}")
    state["telegram_subscribers"] = []
    return []

def save_telegram_subscribers():
    try:
        with open(TELEGRAM_SUBS_PATH, "w") as f:
            json.dump(state["telegram_subscribers"], f, indent=2)
    except Exception as e:
        print(f"Error saving telegram subscribers: {e}")

def add_telegram_subscriber(chat_id: int, name: str = "", chat_type: str = "chat"):
    subs = state["telegram_subscribers"]
    for s in subs:
        if s.get("chat_id") == chat_id:
            if name and name != f"Chat {chat_id}":
                s["name"] = name
            if chat_type:
                s["type"] = chat_type
            save_telegram_subscribers()
            return
    subs.append({
        "chat_id": chat_id,
        "name": name or f"Chat {chat_id}",
        "type": chat_type or "chat",
        "subscribed_at": datetime.now().isoformat()
    })
    save_telegram_subscribers()

def remove_telegram_subscriber(chat_id: int):
    subs = state["telegram_subscribers"]
    state["telegram_subscribers"] = [s for s in subs if s.get("chat_id") != chat_id]
    save_telegram_subscribers()

async def send_telegram(text: str):
    if not settings.TELEGRAM_ENABLED or not settings.TELEGRAM_BOT_TOKEN:
        return
    subs = state.get("telegram_subscribers", [])
    if not subs:
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    proxy = ws_data.get_proxy_url_for(url)
    for sub in subs:
        chat_id = sub.get("chat_id") if isinstance(sub, dict) else sub
        if not chat_id:
            continue
        try:
            async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=5.0) as client:
                await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
        except Exception as e:
            print(f"Failed to send telegram to {chat_id}: {e}")

async def send_telegram_to(chat_id: int, text: str, bot_token: Optional[str] = None):
    tok = bot_token or settings.TELEGRAM_BOT_TOKEN
    if not tok:
        return
    url = f"https://api.telegram.org/bot{tok}/sendMessage"
    proxy = ws_data.get_proxy_url_for(url)
    try:
        async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=5.0) as client:
            await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
    except Exception as e:
        print(f"Failed to send telegram to {chat_id}: {e}")

async def telegram_poller():
    offset = 0
    while True:
        tok = settings.TELEGRAM_BOT_TOKEN
        if not tok:
            await asyncio.sleep(5)
            continue
        try:
            url = f"https://api.telegram.org/bot{tok}/getUpdates"
            proxy = ws_data.get_proxy_url_for(url)
            async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=10.0) as client:
                resp = await client.get(url, params={"offset": offset, "timeout": 5})
                if resp.status_code == 200:
                    data_updates = resp.json().get("result", [])
                    for update in data_updates:
                        offset = max(offset, update.get("update_id", 0) + 1)
                        msg = update.get("message") or update.get("channel_post") or {}
                        chat = msg.get("chat", {})
                        chat_id = chat.get("id")
                        if not chat_id:
                            continue
                        chat_type = chat.get("type", "chat")
                        first_name = chat.get("first_name", "")
                        last_name = chat.get("last_name", "")
                        username = chat.get("username", "")
                        title = chat.get("title", "")
                        name = title or f"{first_name} {last_name}".strip() or (f"@{username}" if username else f"Chat {chat_id}")
                        text = (msg.get("text") or "").strip()
                        
                        cmd = text.split()[0].lower() if text else ""
                        if cmd == "/stop":
                            remove_telegram_subscriber(chat_id)
                            await send_telegram_to(chat_id, "👋 *Unsubscribed from alerts.*", bot_token=tok)
                        elif cmd == "/status":
                            add_telegram_subscriber(chat_id, name, chat_type)
                            running = "🟢 RUNNING" if state["running"] else "🔴 STOPPED"
                            bal_str = f"${state.get('live_balance', 0.0):.2f}" if settings.PRIVATE_KEY else "Paper (No Wallet)"
                            active_cnt = len(state["active_trades"])
                            msg_txt = f"📊 *Bot Status*\n• Status: {running}\n• Mode: `HYBRID (Paper + Live Copy)`\n• Live Balance: `{bal_str}`\n• Active Trades: `{active_cnt}`"
                            await send_telegram_to(chat_id, msg_txt, bot_token=tok)
                        elif cmd == "/balance":
                            add_telegram_subscriber(chat_id, name, chat_type)
                            bal_str = f"${state.get('live_balance', 0.0):.2f}" if settings.PRIVATE_KEY else "No Live Key"
                            funder = clob_trader.get_funder() or "N/A"
                            await send_telegram_to(chat_id, f"💰 *Current Live Balance*\n• pUSD Balance: `{bal_str}`\n• Wallet: `{funder}`", bot_token=tok)
                        elif cmd == "/help":
                            add_telegram_subscriber(chat_id, name, chat_type)
                            await send_telegram_to(chat_id, "ℹ️ *Commands*\n/start - Subscribe\n/stop - Unsubscribe\n/status - Bot status\n/balance - Wallet balance", bot_token=tok)
                        else:
                            add_telegram_subscriber(chat_id, name, chat_type)
                            if cmd == "/start":
                                await send_telegram_to(chat_id, f"🤖 *Subscribed to {settings.CANDLE_WINDOW_MINUTES}m Polymarket Bot alerts!* Use /status or /balance to check current status.", bot_token=tok)
                elif resp.status_code == 401:
                    await asyncio.sleep(30)
                    continue
        except Exception:
            pass
        await asyncio.sleep(2)

# ── Live PnL and State WebSocket broadcast ────────────────────────────────────
def _json_default(obj):
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "hex"):
        return obj.hex()
    if isinstance(obj, set):
        return list(obj)
    return str(obj)

async def broadcast_state():
    if not _ws_clients:
        return
    try:
        msg = json.dumps({
            "type": "state_update",
            "data": state.get("latest_data") or {},
            "logs": state.get("logs", [])[-20:],
            "log_seq": state.get("log_seq", 0),
        }, default=_json_default)
    except Exception as e:
        print(f"broadcast_state json serialization error: {e}")
        return

    dead = set()
    for ws in list(_ws_clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    for ws in dead:
        _ws_clients.discard(ws)

# ── Background task streams ───────────────────────────────────────────────────
def get_ws_symbol_filter(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        return s[:-4].lower()
    return s.lower()

binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL, on_update=_wake_entry)
binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)

polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
    ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
    symbol_includes=get_ws_symbol_filter(settings.SYMBOL),
    on_update=_on_poly_chainlink_tick
)
polymarket_clob_ws = ws_data.PolymarketClobMarketStream(on_update=lambda data=None: _on_book_update())  # PATCH: longshot (ws_data passes the message; the wake doesn't need it)
chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL), on_update=_on_chainlink_tick)

# ── PATCH: price stream (config.json "price_stream"; bot/polybolt.py; DO_IT_YOURSELVES.md part C) ──────
# Polymarket's own reference prices over its price socket: the live Chainlink price and the 60-second
# average the 15-minute markets are decided on. "enabled" connects and logs each window's price to
# beat; three more switches say what the bot uses the numbers for. Whenever a number isn't there (not
# connected, not published yet, too old), the bot does what it did before.
def _ps_creds():
    """The CLOB API credentials the bot already derived from its key, for the socket's login. None without a key."""
    if not settings.PRIVATE_KEY or not clob_trader.ensure_ready():
        return None
    c = getattr(clob_trader.clob, "creds", None)
    if not c or not getattr(c, "key", None):
        return None
    return {"apiKey": str(c.key), "secret": str(c.secret), "passphrase": str(c.passphrase)}


def _ps_on_live(ts: float, price: float) -> None:
    """Every live Chainlink price of the stream as it arrives. With "prices" on it is treated like a price
    of the old Polymarket feed: long-shot recorder, entry check, and the loop's next pass at once when the
    window that just opened has no strike yet."""
    if _ps_use("prices"):
        _on_poly_chainlink_tick({"price": price, "updatedAt": ts * 1000.0})


polybolt_feed = polybolt.PolyBolt(get_creds=_ps_creds, enabled=lambda: polybolt.on(settings.PRICE_STREAM),
                                  log=lambda m: log_message(m), symbol=polybolt.symbol_for(settings.SYMBOL),
                                  proxy=ws_data.get_proxy_url_for(polybolt.URL) or None, on_live=_ps_on_live)
_ps_noted: set = set()            # window starts whose price-to-beat line is in the log
_ps_lag_said = [0.0]              # when the "stamped behind this computer's clock" line was last logged


def _ps_use(what: str) -> bool:
    return polybolt.use(settings.PRICE_STREAM, what)


def _ps_tol() -> float:
    return float((settings.PRICE_STREAM or {}).get("pick_tolerance_s") or 2.0)


def _ps_live_as_poly_ws(poly_ws: Dict[str, Any]) -> Dict[str, Any]:
    """update_loop: with "prices" on, Polymarket's live Chainlink price from the price socket, in the shape of
    the old Polymarket feed's last price — while it is fresh: it arrived, and is stamped, within
    POLY_WS_MAX_AGE_MS. Otherwise poly_ws as it is (the bot then goes on with its other feeds, as before)."""
    if not _ps_use("prices"):
        return poly_ws
    live = polybolt_feed.live(POLY_WS_MAX_AGE_MS / 1000.0)
    if not live:
        return poly_ws
    behind_ms = (time.time() - live[0]) * 1000.0
    if behind_ms > POLY_WS_MAX_AGE_MS:
        if time.time() - _ps_lag_said[0] > 600:
            _ps_lag_said[0] = time.time()
            log_message(f"PRICE STREAM: its live price is stamped {behind_ms / 1000.0:.1f}s behind this computer's clock — "
                        "not used as the bot's price (the bot keeps its own feed)")
        return poly_ws
    return {"price": live[1], "updatedAt": live[0] * 1000.0}


def _ps_strike(start_ms: int, strike_open: Optional[float], strike_source: str):
    """update_loop: the window's strike. With "prices" on and Polymarket's 60-second average for the window's
    start published: that average, which is the price to beat. Else the bot's own, as before."""
    if not _ps_use("prices"):
        return strike_open, strike_source
    ptb = polybolt_feed.twap_at(start_ms / 1000.0, _ps_tol())
    return (ptb, "polymarket_twap") if ptb is not None else (strike_open, strike_source)


def _ps_window_note(start_ms: int, window_ms: int, own_strike: Optional[float]) -> None:
    """update_loop: one log line per window once Polymarket's average for its start is in: the window's price
    to beat next to the bot's own strike, and how the window before it closed on these numbers."""
    if start_ms in _ps_noted or not polybolt.on(settings.PRICE_STREAM):
        return
    pt = polybolt_feed.twap_point_at(start_ms / 1000.0, _ps_tol())
    if pt is None or (own_strike is None and time.time() * 1000 - start_ms < MARK_CAPTURE_WINDOW_MS):
        return
    if len(_ps_noted) > 200:
        _ps_noted.clear()
    _ps_noted.add(start_ms)
    avg, late = pt[1], pt[0] - start_ms / 1000.0
    label = datetime.fromtimestamp(start_ms / 1000).strftime('%H:%M')
    text = (f"PRICE STREAM: Polymarket's 60-second average at {label}" + (f" (stamped {late:.1f}s after it)" if late >= 0.05 else "")
            + f" is {avg:,.2f}: the {label} window's price to beat.")
    if own_strike:
        gap = own_strike - avg
        text += f" The bot's own strike: {own_strike:,.2f} (${abs(gap):,.2f} {'higher' if gap >= 0 else 'lower'})."
    before = polybolt_feed.twap_at((start_ms - window_ms) / 1000.0, _ps_tol())
    if before is not None:
        prev = datetime.fromtimestamp((start_ms - window_ms) / 1000).strftime('%H:%M')
        text += (f" On these numbers the {prev} window closed {'UP' if avg >= before else 'DOWN'}: "
                 f"${abs(avg - before):,.2f} {'above' if avg >= before else 'below'} its price to beat {before:,.2f}.")
    log_message(text)


def _ps_chance(ctx: Dict[str, Any], now: float, end_ts: float) -> Optional[Dict[str, Any]]:
    """evaluate_longshot: with "chance" on, the chance on Polymarket's own numbers — that the closing
    60-second average ends at or above the window's price to beat (longshot.prob_up_twap). None when a number
    is missing (the price to beat, or a live price that arrived within POLY_WS_MAX_AGE_MS): the chance then
    stays the one worked out on Binance."""
    if not _ps_use("chance"):
        return None
    ptb = polybolt_feed.twap_at(int(ctx["start_ms"]) / 1000.0, _ps_tol())
    live = polybolt_feed.live(POLY_WS_MAX_AGE_MS / 1000.0)
    if ptb is None or live is None:
        return None
    vol = longshot.vol_usd_per_sqrt_s(settings.LONGSHOT, live[1], ctx.get("sigma_5m"))
    r = longshot.prob_up_twap(now, end_ts, ptb, live[1], vol, float(settings.PRICE_STREAM.get("sd_floor_usd") or 0.0),
                              pts=polybolt_feed.ticks())
    return {"p_up": r["p_up"], "chance_from": "polymarket_twap", "price_to_beat": round(ptb, 2), "price_now": live[1],
            "expected_avg": round(r["expected_avg"], 2), "sd": round(r["sd"], 3)}


def _ps_settle_call(trade: Dict[str, Any]):
    """update_trades: (winner, closing average, opening average) on Polymarket's own 60-second averages, with
    "settle" on and both published; None otherwise (the bot then compares its own two prices, as before)."""
    start, end = trade.get("window_start_ms"), trade.get("end_ts")
    if not _ps_use("settle") or not start or not end:
        return None
    start, end = float(start) / 1000.0, float(end)
    dur = end - start
    if not (240 <= dur <= 360 or 840 <= dur <= 960 or abs(dur - settings.CANDLE_WINDOW_MINUTES * 60) <= 2.0):
        return None          # not one whole window: left to the bot's own rule
    opened = polybolt_feed.twap_at(start, _ps_tol())
    closed = polybolt_feed.twap_at(end, _ps_tol())
    if opened is None or closed is None:
        return None
    return ("UP" if closed >= opened else "DOWN"), closed, opened


def get_candle_window_timing(window_minutes: Optional[int] = None) -> Dict[str, float]:
    wm = window_minutes if window_minutes is not None else settings.CANDLE_WINDOW_MINUTES
    now_ms = time.time() * 1000
    window_ms = wm * 60_000
    start_ms = (now_ms // window_ms) * window_ms
    end_ms = start_ms + window_ms
    elapsed_ms = now_ms - start_ms
    remaining_ms = end_ms - now_ms
    return {
        "startMs": start_ms,
        "endMs": end_ms,
        "elapsedMs": elapsed_ms,
        "remainingMs": remaining_ms,
        "elapsedMinutes": elapsed_ms / 60_000,
        "remainingMinutes": remaining_ms / 60_000
    }

_cached_market = None
_cached_market_ts = 0.0
_cached_market_series = ""
_prefetch_tries: Dict[int, float] = {}

_next_market: Dict[str, Any] = {"start_ms": None, "market": None, "tokens": [], "series": None}
_next_market_busy = False


def _token_ids_of(market: Dict[str, Any]):
    """(up token, down token) of a market."""
    outcomes = market.get("outcomes", [])
    ids = market.get("clobTokenIds", [])
    try:
        outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
        ids = json.loads(ids) if isinstance(ids, str) else ids
    except Exception:
        return None, None
    up = down = None
    for i, o in enumerate(outcomes or []):
        tid = ids[i] if i < len(ids or []) else None
        if not tid:
            continue
        if str(o).lower() == settings.POLYMARKET_UP_LABEL.lower():
            up = tid
        elif str(o).lower() == settings.POLYMARKET_DOWN_LABEL.lower():
            down = tid
    return up, down


def _next_window_tokens(current_market: Optional[Dict[str, Any]]) -> List[str]:
    """Token ids of the next window's market when it was looked up already (its order
    books are followed alongside the current ones)."""
    nm = _next_market
    if not nm.get("market") or not nm.get("tokens") or nm.get("start_ms") is None:
        return []
    if current_market and str(current_market.get("id")) == str(nm["market"].get("id")):
        return []
    if nm["start_ms"] < time.time() * 1000 - 60_000:
        return []
    return list(nm["tokens"])


async def _prefetch_next_market(next_start_ms: int):
    """Look up the next window's market before it opens, follow its order books on the
    websocket and get its tokens ready for orders, so nothing has to be fetched when the
    window opens (that took a REST round trip or two at every window open)."""
    global _next_market_busy
    if _next_market_busy:
        return
    _next_market_busy = True
    try:
        series = settings.POLYMARKET_SLUG or settings.POLYMARKET_SERIES_ID
        market = await _discover_market(next_start_ms, strict=True)
        if not market:
            return
        up, down = _token_ids_of(market)
        if not up or not down:
            return
        _next_market.update(start_ms=int(next_start_ms), market=market, tokens=[up, down], series=series)
        polymarket_clob_ws.update_assets([a for a in polymarket_clob_ws.asset_ids if a not in (up, down)] + [up, down])
        if settings.PRIVATE_KEY:
            clob_trader.warm_token(up)
            clob_trader.warm_token(down)
    except Exception as e:
        print(f"Warning: next market lookup error: {e}")
    finally:
        _next_market_busy = False


def _series_for_tf(tf: int) -> str:
    sym = (settings.SYMBOL or "BTCUSDT").upper()
    asset = sym.replace("USDT", "").replace("USD", "").lower()
    series_15m_to_5m = {
        "10192": "10684",  # BTC
        "10212": "10683",  # ETH
        "10191": "10683",  # ETH
        "10423": "10686",  # SOL
        "10422": "10685",  # XRP
        "11330": "11326",  # BNB
        "11328": "11325",  # DOGE
        "11329": "11327",  # HYPE
        "12518": "12517",  # ZEC
    }
    series_5m_to_15m = {v: k for k, v in series_15m_to_5m.items()}
    asset_defaults_15m = {
        "btc": "10192", "eth": "10191", "sol": "10423", "xrp": "10422",
        "bnb": "11330", "doge": "11328", "hype": "11329", "zec": "12518"
    }
    cur_sid = str(settings.POLYMARKET_SERIES_ID or "")
    if tf == 5:
        if cur_sid in series_15m_to_5m:
            return series_15m_to_5m[cur_sid]
        if cur_sid in series_5m_to_15m:
            return cur_sid
        b15 = asset_defaults_15m.get(asset, "10192")
        return series_15m_to_5m.get(b15, "10684")
    else:
        if cur_sid in series_5m_to_15m:
            return series_5m_to_15m[cur_sid]
        if cur_sid in series_15m_to_5m:
            return cur_sid
        return asset_defaults_15m.get(asset, "10192")


async def _discover_market(target_start_ms: float, strict: bool = False,
                           series_id: Optional[str] = None, window_minutes: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """The market of the window starting at target_start_ms (strict: only that window)."""
    now_ms = time.time() * 1000
    market = None
    cwm = window_minutes or settings.CANDLE_WINDOW_MINUTES
    sid = series_id or settings.POLYMARKET_SERIES_ID
    if settings.POLYMARKET_SLUG and not series_id:
        return None if strict else await data.fetch_market_by_slug(settings.POLYMARKET_SLUG)
    if settings.POLYMARKET_AUTO_SELECT_LATEST and sid:
        events = await data.fetch_live_events_by_series_id(sid)
        markets = data.flatten_event_markets(events)
        live_markets = []
        for m in markets:
            end_str = m.get("endDate")
            if not end_str:
                continue
            try:
                end_m_ts = datetime.fromisoformat(end_str.replace('Z', '+00:00')).timestamp() * 1000
            except Exception:
                continue
            if end_m_ts <= now_ms + 15_000:
                continue
            esr = m.get("eventStartTime") or m.get("gameStartTime")
            start_m_ts = None
            if esr:
                try:
                    start_m_ts = datetime.fromisoformat(str(esr).replace('Z', '+00:00')).timestamp() * 1000
                except Exception:
                    pass
            if start_m_ts is None:
                start_m_ts = end_m_ts - (cwm * 60_000)
            if abs(start_m_ts - target_start_ms) < 120_000 or (not strict and start_m_ts <= now_ms < end_m_ts):
                live_markets.append((m, end_m_ts, abs(start_m_ts - target_start_ms)))
        if live_markets:
            live_markets.sort(key=lambda x: (x[2], x[1]))
            market = live_markets[0][0]
    return market


_cached_markets_by_tf = {15: {"market": None, "ts": 0.0, "series": None}, 5: {"market": None, "ts": 0.0, "series": None}}

async def get_active_market_for_tf(tf: int, force: bool = False) -> Optional[Dict[str, Any]]:
    global _cached_markets_by_tf, _cached_market, _cached_market_ts, _cached_market_series
    now = time.time()
    timing = get_candle_window_timing(tf)
    cur_start_ms = timing["startMs"]
    sid = _series_for_tf(tf)
    curr_target = sid

    entry = _cached_markets_by_tf.setdefault(tf, {"market": None, "ts": 0.0, "series": None})

    nm = _next_market
    if (tf == settings.CANDLE_WINDOW_MINUTES and not force and nm.get("market") and nm.get("series") == curr_target
            and nm.get("start_ms") is not None and abs(nm["start_ms"] - cur_start_ms) < 1000):
        entry["market"] = nm["market"]
        entry["ts"] = now
        entry["series"] = curr_target
        _cached_market, _cached_market_ts, _cached_market_series = nm["market"], now, curr_target
        nm.update(start_ms=None, market=None, tokens=[], series=None)
        return entry["market"]

    if not force and entry.get("market") and entry.get("series") == curr_target:
        end_date = entry["market"].get("endDate")
        if end_date:
            try:
                end_ts = datetime.fromisoformat(end_date.replace('Z', '+00:00')).timestamp()
                if end_ts > now + 15 and (now - entry.get("ts", 0.0)) < 30.0:
                    return entry["market"]
            except Exception:
                pass

    market = None
    try:
        market = await _discover_market(cur_start_ms, strict=False, series_id=sid, window_minutes=tf)
    except Exception as e:
        print(f"Warning: Market discovery error for {tf}m: {e}")

    if market:
        entry["market"] = market
        entry["ts"] = now
        entry["series"] = curr_target
        if tf == settings.CANDLE_WINDOW_MINUTES:
            _cached_market = market
            _cached_market_ts = now
            _cached_market_series = curr_target
        return market

    return entry.get("market")


async def get_active_market(force: bool = False) -> Optional[Dict[str, Any]]:
    return await get_active_market_for_tf(settings.CANDLE_WINDOW_MINUTES, force=force)


async def fetch_snapshot_for_market(market: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not market:
        return {"ok": False, "reason": "market_not_found"}

    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    clob_token_ids = market.get("clobTokenIds", [])
    if isinstance(clob_token_ids, str):
        clob_token_ids = json.loads(clob_token_ids)

    outcome_prices = market.get("outcomePrices", [])
    if isinstance(outcome_prices, str):
        outcome_prices = json.loads(outcome_prices)

    up_token_id = None
    down_token_id = None

    for i, outcome in enumerate(outcomes):
        token_id = clob_token_ids[i] if i < len(clob_token_ids) else None
        if not token_id: continue
        if outcome.lower() == settings.POLYMARKET_UP_LABEL.lower():
            up_token_id = token_id
        elif outcome.lower() == settings.POLYMARKET_DOWN_LABEL.lower():
            down_token_id = token_id

    up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), -1)
    down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), -1)

    gamma_yes = float(outcome_prices[up_index]) if up_index >= 0 and up_index < len(outcome_prices) else None
    gamma_no = float(outcome_prices[down_index]) if down_index >= 0 and down_index < len(outcome_prices) else None

    if not up_token_id or not down_token_id:
        return {"ok": False, "reason": "missing_token_ids"}

    # Update active tokens in the WebSocket stream (+ all cached markets)
    _all_toks = [up_token_id, down_token_id] + _next_window_tokens(market)
    for _c in _cached_markets_by_tf.values():
        _cm = _c.get("market")
        if _cm and _cm is not market:
            _raw_toks = _cm.get("clobTokenIds") or []
            if isinstance(_raw_toks, str):
                try:
                    _raw_toks = json.loads(_raw_toks)
                except Exception:
                    _raw_toks = []
            for _tok in _raw_toks:
                if _tok and isinstance(_tok, str) and len(_tok) > 10 and _tok not in _all_toks:
                    _all_toks.append(_tok)
    _all_toks = [t for t in _all_toks if isinstance(t, str) and len(t) > 10]
    polymarket_clob_ws.update_assets(_all_toks)

    up_ws = polymarket_clob_ws.get_token_market(up_token_id)
    down_ws = polymarket_clob_ws.get_token_market(down_token_id)

    # Use WS best_ask if available, else fallback to REST
    up_buy = up_ws.get("best_ask")
    down_buy = down_ws.get("best_ask")

    if up_buy is None or down_buy is None:
        try:
            up_rest_price, down_rest_price = await asyncio.gather(
                data.fetch_clob_price(up_token_id, "buy") if up_buy is None else asyncio.sleep(0, result=up_buy),
                data.fetch_clob_price(down_token_id, "buy") if down_buy is None else asyncio.sleep(0, result=down_buy)
            )
            if up_buy is None: up_buy = up_rest_price
            if down_buy is None: down_buy = down_rest_price
        except Exception:
            pass

    # Build orderbook summaries using WS orderbooks if available, fallback to REST
    up_book_summary = None
    down_book_summary = None

    if up_ws.get("bids") or up_ws.get("asks"):
        up_book_summary = data.summarize_order_book(up_ws)
    if down_ws.get("bids") or down_ws.get("asks"):
        down_book_summary = data.summarize_order_book(down_ws)

    if not up_book_summary or not down_book_summary:
        try:
            up_book, down_book = await asyncio.gather(
                data.fetch_order_book(up_token_id) if not up_book_summary else asyncio.sleep(0, result={}),
                data.fetch_order_book(down_token_id) if not down_book_summary else asyncio.sleep(0, result={})
            )
            if not up_book_summary: up_book_summary = data.summarize_order_book(up_book)
            if not down_book_summary: down_book_summary = data.summarize_order_book(down_book)
        except Exception:
            if not up_book_summary:
                up_book_summary = {"bestBid": None, "bestAsk": up_buy, "spread": None, "bidLiquidity": None, "askLiquidity": None}
            if not down_book_summary:
                down_book_summary = {"bestBid": None, "bestAsk": down_buy, "spread": None, "bidLiquidity": None, "askLiquidity": None}

    # Track whether the book data came from WS or REST
    book_source = "ws" if ((up_ws.get("bids") or up_ws.get("asks")) and (down_ws.get("bids") or down_ws.get("asks"))) else "rest"
    final_up = up_buy if up_buy is not None else gamma_yes
    final_down = down_buy if down_buy is not None else gamma_no
    if final_up is not None:
        state["last_poly_up"] = final_up
    else:
        final_up = state.get("last_poly_up")

    if final_down is not None:
        state["last_poly_down"] = final_down
    else:
        final_down = state.get("last_poly_down")

    # Pre-warm active market tokens in CLOB client so order creation executes in <10ms
    if settings.PRIVATE_KEY:
        if up_token_id:
            clob_trader.warm_token(up_token_id)
        if down_token_id:
            clob_trader.warm_token(down_token_id)

    return {
        "ok": True,
        "market": market,
        "prices": {
            "up": final_up,
            "down": final_down
        },
        "token_ids": {
            "up": up_token_id,
            "down": down_token_id
        },
        "orderbook": {
            "up": up_book_summary,
            "down": down_book_summary
        },
        "book_source": book_source
    }


_other_tf_strikes: Dict[int, float] = {}

async def fetch_polymarket_snapshot() -> Dict[str, Any]:
    m = await get_active_market_for_tf(settings.CANDLE_WINDOW_MINUTES)
    return await fetch_snapshot_for_market(m)

async def fetch_other_tf_snapshot(tf: int) -> Dict[str, Any]:
    m = await get_active_market_for_tf(tf)
    return await fetch_snapshot_for_market(m)


async def handle_order_gone_quiet(order_id: str, trade: Dict[str, Any], token_id: str,
                                  current_ask: float, limit_price: float, otype: str) -> bool:
    """Never assume an unindexed order terminated without fill.
    1. Explicitly cancel by ID on Polymarket.
    2. Sum all partial fills for THIS order ID across taker and maker records.
    3. If fills exist: record live fill (shares, real weighted average price, cost).
    4. Else: log confirmed cancelled, no fill.
    """
    side = trade.get("side", "UNKNOWN")
    # 1. Never assume. Cancel explicitly by ID.
    cancel_success = False
    try:
        c_res = await asyncio.to_thread(clob_trader.cancel_order, order_id)
        cancel_success = bool(c_res and c_res.get("ok"))
    except Exception as e:
        log_message(f"cancel {order_id} failed: {e}")

    # Brief buffer for indexer to catch any matching engine fills
    await asyncio.sleep(0.5)

    # Double-check: if cancel call failed or unconfirmed, ensure emergency cancel
    if not cancel_success:
        try:
            ord_check = await asyncio.to_thread(clob_trader.check_order_status, order_id)
            if ord_check.get("status") == "OPEN":
                log_message(f"Warning: order {order_id} still open on book; sending emergency cancel_order")
                await asyncio.to_thread(clob_trader.cancel_order, order_id)
        except Exception:
            pass

    # 2. Check what actually happened to THIS order (sum all partial fills).
    fills = await asyncio.to_thread(clob_trader.get_order_fills, order_id, token_id)
    if fills:
        total_shares = sum(float(f["size"]) for f in fills)
        total_usd = sum(float(f["size"]) * float(f["price"]) for f in fills)
        avg_price = total_usd / total_shares if total_shares > 0 else limit_price

        trade["live_status"] = "FILLED"
        trade["live_entry_price"] = avg_price
        trade["live_shares"] = total_shares
        trade["live_amount"] = total_usd
        trade["live_entry_time"] = datetime.now().isoformat()
        trade["live_slippage"] = avg_price - current_ask

        # Deduct spent funds in RAM
        if total_usd > 0:
            new_bal = max(0.0, (state.get("live_balance") or 0.0) - total_usd)
            state["live_balance"] = new_bal
            risk_type = (settings.RISK_TYPE or "percent").lower()
            if risk_type == "fixed":
                new_stake = float(settings.RISK_VALUE)
            else:
                new_stake = (float(settings.RISK_VALUE) / 100.0) * new_bal
            state["precomputed_live_stake"] = round(max(1.0, min(new_stake, new_bal)) if new_bal >= 1.0 else 0.0, 2)

        save_state()
        _sync_active_trades_to_latest_data()
        await broadcast_state()

        msg = (f"LIVE {otype} fill confirmed across {len(fills)} piece(s): {side} {total_shares:.2f} shares @ {avg_price:.4f} "
               f"(${total_usd:.2f}, order {order_id})")
        log_message(msg)
        await send_telegram(f"🚀 *LIVE Copy FILLED ({otype} Limit)*\n• Side: `{side}`\n• Fill Price: `{avg_price:.4f}`\n• Shares: `{total_shares:.2f}`\n• Amount: `${total_usd:.2f}`\n• Pieces: `{len(fills)}`\n• Market: `{trade['market_slug']}`")
        return True
    else:
        log_message(f"{otype} order {order_id} confirmed cancelled on exchange, no fill")
        trade["live_status"] = "CANCELLED"
        save_state()
        _sync_active_trades_to_latest_data()
        await broadcast_state()
        return False

# ── Long-shot fills (PATCH: longshot) ─────────────────────────────────────────
def _ls_add_fill(trade: Dict[str, Any], shares: float, usd: float, quoted_ask: float):
    """Adds one fill to a long shot."""
    trade["live_shares"] = float(trade.get("live_shares") or 0.0) + shares
    trade["live_amount"] = float(trade.get("live_amount") or 0.0) + usd
    trade["live_entry_price"] = trade["live_amount"] / trade["live_shares"] if trade["live_shares"] > 0 else quoted_ask
    trade["live_entry_time"] = trade.get("live_entry_time") or datetime.now().isoformat()
    trade["live_slippage"] = trade["live_entry_price"] - quoted_ask
    if usd > 0:                                   # spent funds come off the balance in RAM
        new_bal = max(0.0, (state.get("live_balance") or 0.0) - usd)
        state["live_balance"] = new_bal
        risk_type = (settings.RISK_TYPE or "percent").lower()
        new_stake = float(settings.RISK_VALUE) if risk_type == "fixed" else (float(settings.RISK_VALUE) / 100.0) * new_bal
        state["precomputed_live_stake"] = round(max(1.0, min(new_stake, new_bal)) if new_bal >= 1.0 else 0.0, 2)


# ── Maker long shots (PATCH: longshot maker) ──────────────────────────────────
# bot/ls_maker.py keeps the resting bids; these are its links to the rest of the bot.
def _mk_is_live() -> bool:
    """Real orders with a private key when bot is running."""
    return bool(settings.PRIVATE_KEY) and bool(state.get("running"))


def _mk_running() -> bool:
    return bool(state.get("running")) and state.get("withdraw_state") != "in_progress"


async def _mk_place(token_id: str, shares: float, price: float) -> Dict[str, Any]:
    """A post-only resting bid on book.
    Uses maker (post_only=True) for any selected order type:
    - If GTD: use GTD maker.
    - If FOK or FAK: use maker if available; if not supported on CLOB, fall back to GTD maker.
    """
    ls_ot = str((settings.LONGSHOT or {}).get("order_type") or "GTD").upper()
    if ls_ot not in ("FAK", "FOK", "GTD", "GTC"):
        ls_ot = "GTD"
    tick = longshot.tick_size_str(price)

    if ls_ot in ("FAK", "FOK"):
        try:
            res = await clob_trader.place_limit_buy_async(
                token_id, shares, price, expiration=0, tick_size=tick, order_type=ls_ot, post_only=True
            )
            if res.get("ok") and (res.get("order_id") or res.get("status")):
                return res
            err_msg = str(res.get("error") or res.get("response", {}).get("errorMsg") or "")
            log_message(f"LONGSHOT MAKER: {ls_ot} maker unavailable on CLOB ({err_msg or 'unsupported'}), falling back to GTD maker")
        except Exception as e:
            log_message(f"LONGSHOT MAKER: {ls_ot} maker attempt failed ({e}), falling back to GTD maker")

    otype = "GTC" if ls_ot == "GTC" else "GTD"
    expiration = 0 if otype == "GTC" else int(time.time() + 300)
    return await clob_trader.place_limit_buy_async(token_id, shares, price, expiration,
                                                   tick, otype, post_only=True)


async def _mk_cancel(order_id: str) -> Dict[str, Any]:
    return await asyncio.to_thread(clob_trader.cancel_order, order_id)


async def _mk_get_order(order_id: str) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(clob_trader.get_order, order_id)


def _mk_book(token_id: str) -> Optional[Dict[str, Any]]:
    """Best ask and ask levels from the order-book stream (a quiet book is still a valid book)."""
    s = polymarket_clob_ws.get_summary(token_id, max_age_s=60.0) if token_id else None
    if not s:
        return None
    asks = []
    for a in s.get("asks") or []:
        try:
            asks.append((float(a.get("px", a.get("price"))), float(a.get("size") or 0.0)))
        except (TypeError, ValueError):
            pass
    return {"best_ask": s.get("bestAsk"), "asks": asks}


def _mk_balance(live: bool) -> float:
    if not live:
        return float(state.get("paper_balance") or 0.0)
    bal = float(state.get("live_balance") or 0.0)
    if bal <= 0 and settings.PRIVATE_KEY and clob_trader:
        try:
            b = clob_trader.get_usdc_balance()
            if b is not None and b > 0:
                bal = float(b)
                state["live_balance"] = bal
        except Exception:
            pass
    return bal


def _mk_persist(ids: List[str]) -> None:
    """Live bids on the book, saved so the next start can cancel them if the bot stopped meanwhile."""
    if list(state.get("ls_maker_open") or []) != list(ids):
        state["ls_maker_open"] = list(ids)
        try:
            asyncio.get_running_loop().create_task(asyncio.to_thread(save_state))
        except RuntimeError:
            save_state()


async def _mk_on_fill(bid: "ls_maker.Bid", shares: float, usd: float, via: str) -> None:
    """A maker bid bought `shares` for `usd` (always at its own price). The first fill makes a
    long-shot trade (open_reason "longshot_maker"); later fills add to it. It settles, redeems
    and shows like any long shot. Paper bids only move the paper balance."""
    t = bid.trade
    first = t is None
    if first:
        m = bid.meta or {}
        market = m.get("market") or {}
        prob = float(bid.last_prob)
        t = {
            "trade_id": f"{market.get('id')}_{int(time.time() * 1000)}_lsm",
            "market_id": market.get("id"),
            "market_slug": market.get("slug"),
            "side": bid.side,
            "token_id": bid.token_id,
            "paper_entry_price": bid.price,
            "paper_amount": 0.0,
            "paper_shares": 0.0,
            "paper_entry_time": datetime.now().isoformat(),
            "paper_close_price": None,
            "paper_closed_time": None,
            "paper_profit_loss": None,
            "live_status": "FILLED" if bid.live else ("PAPER_ONLY" if settings.PRIVATE_KEY else "NO_CREDS"),
            "live_attempts": 1 if bid.live else 0,
            "live_entry_price": None,
            "live_amount": None,
            "live_shares": None,
            "live_entry_time": None,
            "live_order_id": bid.order_id if bid.live else None,
            "live_mark_price": None,
            "live_unrealized_pl": None,
            "live_close_price": None,
            "live_closed_time": None,
            "live_profit_loss": None,
            "status": "OPEN",
            "awaiting_resolution": False,
            "strike_price": m.get("strike"),
            "strike_source": m.get("strike_src"),
            "window_start_ms": bid.start_ms,
            "open_reason": "longshot_maker",
            "end_ts": m.get("end_ts"),
            "mode": "hybrid",
            "kind": "longshot",
            "longshot": dict(m.get("info") or {}, band="maker", order_type="MAKER", prob=prob,
                             multiple=(prob / bid.price) if bid.price > 0 else None, max_price=bid.price,
                             bid_shares=bid.shares, prob_at_bid=bid.prob_at_place,
                             secs_left=round(float(m.get("end_ts") or 0.0) - time.time(), 2), fill_via=via),
        }
        if bid.live:
            t["live_order_ids"] = [bid.order_id]
        bid.trade = t
        state["active_trades"].append(t)
        if longshot.one_at_a_time_on(settings.LONGSHOT):
            _ls_entered_windows.add(bid.start_ms)
    t["paper_shares"] = float(t.get("paper_shares") or 0.0) + shares
    t["paper_amount"] = float(t.get("paper_amount") or 0.0) + usd
    state["paper_balance"] = float(state.get("paper_balance") or 0.0) - usd
    if bid.live:
        _ls_add_fill(t, shares, usd, bid.price)       # live shares / amount / entry price, live balance
        t["live_status"] = "FILLED"
    vias = str(t["longshot"].get("fill_via") or "")
    if via not in vias.split("+"):
        t["longshot"]["fill_via"] = f"{vias}+{via}" if vias else via
    mode = "LIVE" if bid.live else "PAPER"
    log_message(f"LONGSHOT MAKER {mode} fill: {bid.side} {shares:g} shares @ {bid.price:.3f} (${usd:.2f}, via {via}) "
                f"on {t['market_slug']} — bid {bid.filled:g}/{bid.shares:g} filled, chance {bid.last_prob:.3f}")
    if first:
        asyncio.create_task(send_telegram(
            f"{'🚀' if bid.live else '🟢'} *{mode} Maker Long Shot Filled*\n• Side: `{bid.side}`\n• Price: `{bid.price:.3f}`\n"
            f"• Shares: `{shares:g}`\n• Amount: `${usd:.2f}`\n• Market: `{t['market_slug']}`"))
    asyncio.create_task(asyncio.to_thread(save_state))
    _sync_active_trades_to_latest_data()
    asyncio.create_task(broadcast_state())


def _mk_has_other_longshot(start_ms: int) -> bool:
    """True if another long shot (engine or sniper) is active or entered for this window."""
    if not longshot.one_at_a_time_on(settings.LONGSHOT):
        return False
    sms = int(start_ms)
    for t in state.get("active_trades", []):
        if _is_longshot(t) and not _is_ls_maker(t):
            if t.get("window_start_ms") == sms:
                return True
    if sms in _ls_entered_windows:
        return True
    return False


ls_maker_mgr = ls_maker.MakerManager(SimpleNamespace(
    get_cfg=lambda: settings.LONGSHOT, is_live=_mk_is_live, running=_mk_running,
    place=_mk_place, cancel=_mk_cancel, get_order=_mk_get_order, book=_mk_book,
    trades_since=lambda tok, ts: polymarket_clob_ws.recent_trades(tok, ts) if tok else [],
    balance=_mk_balance, on_fill=_mk_on_fill, log=log_message, persist=_mk_persist,
    has_other_longshot=_mk_has_other_longshot,
    orphans=lambda: list(state.get("ls_maker_open") or []) if settings.PRIVATE_KEY else []))



async def _continuous_live_copy_fill(trade: Dict[str, Any], market: Dict[str, Any], token_ids: Dict[str, Any], evaluated_price: Optional[float] = None, ask_liq_usd: Optional[float] = None,
                                     order_sent: Optional[asyncio.Event] = None):
    token_id = trade.get("token_id")
    side = trade.get("side")
    # PATCH: longshot — long shots use their own slot keys, stake, price cap, tick, order type and timings
    is_ls = _is_longshot(trade)
    ls_cfg = settings.LONGSHOT if is_ls else {}
    slot_keys = _slot_keys(market, "longshot" if is_ls else "regular")
    if is_ls and _is_ls_engine(trade):   # PATCH: longshot engine — one fill-and-kill order, never re-quoted
        return await _le_live_fak(trade, slot_keys, order_sent, evaluated_price)

    if not token_id:
        if order_sent is not None:
            order_sent.set()
        trade["live_status"] = "FAILED"
        log_message(f"LIVE copy aborted: missing token_id for side {side}")
        for k in slot_keys:
            _in_flight_markets.discard(k)
        return

    # PATCH: fill-and-kill copy (Settings -> Fill-and-Kill Copy, config.json "copy_fak"). On: a
    # regular trade's live copy is ONE fill-and-kill order at the paper entry + the cap, sized in
    # dollars. Nothing is left on the book and nothing is re-quoted (bot/copy_fak.py). Long shots
    # keep their own orders. Off: the GTD loop below, as before.
    if not is_ls and copy_fak.on(settings.COPY_FAK):
        try:
            await copy_fak.live_copy(trade, evaluated_price, settings=settings, state=state, trader=clob_trader,
                                     log=log_message, save=save_state, sync=_sync_active_trades_to_latest_data,
                                     broadcast=broadcast_state, telegram=send_telegram, order_sent=order_sent,
                                     ws_client=polymarket_clob_ws)
        finally:
            if order_sent is not None:
                order_sent.set()
            for k in slot_keys:
                _in_flight_markets.discard(k)
        return

    # (the "LIVE copy started" line is written after the first order went out: no file write before it)
    retry_interval = max(0.01, float(getattr(settings, "COPY_RETRY_INTERVAL_MS", 300) or 300) / 1000.0)
    max_attempts = int(getattr(settings, "COPY_MAX_RETRIES", 5) or 5)
    resting_s = float(getattr(settings, "COPY_RESTING_TIMEOUT_S", 2.0) or 2.0)

    try:
        while trade["status"] == "OPEN" and trade.get("live_status") == "FILLING":
            now_ts = time.time()
            # 1. Check if window is close to expiry (<15s)
            if trade.get("end_ts") and now_ts >= trade["end_ts"]:
                trade["live_status"] = "TIMEOUT"
                log_message(f"LIVE copy stopped for {trade['market_slug']}: window expired")
                await asyncio.to_thread(save_state)
                _sync_active_trades_to_latest_data()
                await broadcast_state()
                break

            attempt_num = trade.get("live_attempts", 0) + 1
            trade["live_attempts"] = attempt_num

            if attempt_num > max_attempts:
                trade["live_status"] = "TIMEOUT"
                log_message(f"LIVE copy max attempts reached ({max_attempts}) for {trade['market_slug']} — halting to prevent duplicate orders")
                await asyncio.to_thread(save_state)
                _sync_active_trades_to_latest_data()
                await broadcast_state()
                break

            # 2. Get freshest book ask for this token:
            # Attempt #1 uses evaluated_price instantly with 0ms RAM lookup.
            # Subsequent attempts (re-quotes) MUST refresh the live ask from WebSocket and live CLOB REST API!
            if attempt_num == 1 and evaluated_price and evaluated_price > 0:
                current_ask = evaluated_price
                cur_ask_liq_usd = ask_liq_usd
            else:
                # Priority 1: WebSocket cache with relaxed age window
                summary = polymarket_clob_ws.get_summary(token_id, max_age_s=15.0)
                current_ask = summary.get("bestAsk") if summary else None
                ask_liq_sh = summary.get("askLiquidity") if summary else None
                cur_ask_liq_usd = (ask_liq_sh * current_ask) if (ask_liq_sh is not None and current_ask and current_ask > 0) else None

                # Priority 2: Direct query to CLOB API to guarantee 100% fresh on-exchange live price
                if not current_ask or current_ask <= 0:
                    try:
                        from bot.data import fetch_clob_price
                        clob_p = await fetch_clob_price(token_id, "buy")
                        if clob_p and clob_p > 0:
                            current_ask = float(clob_p)
                    except Exception:
                        pass

                # Priority 3: Check raw WebSocket book without age filtering
                if not current_ask or current_ask <= 0:
                    raw_book = polymarket_clob_ws.get_token_market(token_id)
                    asks = raw_book.get("asks", [])
                    if asks:
                        try:
                            current_ask = float(asks[0].get("px", asks[0]["price"]))
                        except Exception:
                            pass

                # Priority 4: Fallback to initial price only if all real-time network/cache sources failed
                if not current_ask or current_ask <= 0:
                    current_ask = evaluated_price or trade.get("paper_entry_price") or 0.50

                if cur_ask_liq_usd is None or cur_ask_liq_usd <= 0:
                    cur_ask_liq_usd = ask_liq_usd

            # 3. Sizing: Instant RAM lookup of precomputed stake from background updater (0ms latency)
            live_balance = state.get("live_balance") or 0.0
            live_amount = state.get("precomputed_live_stake") or 0.0

            # Safety fallback if background updater hasn't populated yet
            if live_balance <= 0 or live_amount <= 0:
                live_balance = await asyncio.to_thread(clob_trader.get_usdc_balance) or 0.0
                state["live_balance"] = live_balance
                risk_type = (settings.RISK_TYPE or "percent").lower()
                if risk_type == "fixed":
                    live_amount = float(settings.RISK_VALUE)
                else:
                    live_amount = (float(settings.RISK_VALUE) / 100.0) * live_balance
                live_amount = max(1.0, min(live_amount, live_balance)) if live_balance >= 1.0 else 0.0
                state["precomputed_live_stake"] = round(live_amount, 2)

            if is_ls:
                if longshot.stake_pct(ls_cfg, "sniper_stake_pct") > 0:   # PATCH: longshot stake — the sniper's own % of the live balance
                    live_amount = longshot.stake_for(live_balance, ls_cfg)
                lsd = trade.get("longshot") or {}
                max_px = float(lsd.get("max_price") or 0.04)
                if current_ask > max_px + 1e-9:
                    trade["live_status"] = "PRICE_ABOVE_CAP"
                    log_message(f"LONGSHOT live copy stopped for {trade['market_slug']}: ask {current_ask:.3f} is above the {max_px:.3f} cap")
                    await asyncio.to_thread(save_state)
                    _sync_active_trades_to_latest_data()
                    await broadcast_state()
                    break

            # Polymarket order size minimum: at least 5 shares and at least $1.00
            min_order_cost = round(max(1.00, math.ceil(5.0 * current_ask * 100.0) / 100.0), 2)
            if live_balance < min_order_cost:
                trade["live_status"] = "INSUFFICIENT_FUNDS"
                log_message(f"LIVE copy halted for {trade['market_slug']}: deposit balance too low (${live_balance:.2f} < ${min_order_cost:.2f} minimum for 5 shares @ {current_ask:.4f})")
                await asyncio.to_thread(save_state)
                _sync_active_trades_to_latest_data()
                await broadcast_state()
                break

            if live_amount < min_order_cost:
                live_amount = min_order_cost

            if cur_ask_liq_usd is not None and cur_ask_liq_usd >= min_order_cost and live_amount > cur_ask_liq_usd:
                live_amount = round(cur_ask_liq_usd, 2)

            # Place GTD limit BUY order directly with progressive sweep
            extra_slip = min(0.08, (attempt_num - 1) * 0.015)
            limit_cap = float((trade.get("longshot") or {}).get("max_price") or 0.99) if is_ls else 0.99
            gtd_price = min(limit_cap, round(current_ask + settings.CLOB_MAX_SLIPPAGE + extra_slip, 4))
            safe_usd = max(1.00, live_amount)
            # Guarantee shares >= 5.0 and shares * price >= 1.00 (Polymarket min order size rule: min 5 shares)
            min_shares = 5.0
            gtd_shares = max(min_shares, math.ceil((safe_usd / gtd_price) * 100.0) / 100.0)
            while round(gtd_shares * gtd_price, 4) < 1.00:
                gtd_shares = round(gtd_shares + 0.01, 2)
            gtd_tick = "0.001" if gtd_price < 0.04 else "0.01"
            if is_ls:
                ls_ot = str((ls_cfg or {}).get("order_type") or "GTD").upper()
                order_type = ls_ot if ls_ot in ("FAK", "FOK", "GTD", "GTC") else "GTD"
            else:
                order_type = "GTD"

            market_end = trade.get("end_ts") or 0
            expiration = int(max(time.time() + 300, market_end)) if order_type == "GTD" else 0

            _t_send = time.perf_counter()
            gtd_res = await clob_trader.place_limit_buy_async(token_id, gtd_shares, gtd_price, expiration, gtd_tick, order_type)
            _send_ms = (time.perf_counter() - _t_send) * 1000.0
            if order_sent is not None:
                order_sent.set()
            if attempt_num == 1:
                log_message(f"LIVE copy started for {trade['market_slug']} ({side}{', long shot' if is_ls else ''}) — continuous GTD fill loop initiated")
            # (same start as before, so anything that reads these lines still matches; now written
            # once the order is out, with how long Polymarket took to answer)
            log_message(f"LIVE copy attempt #{attempt_num} for {side}{' long shot' if is_ls else ''}: placing GTD limit order for {gtd_shares:.2f} shares @ {gtd_price:.4f} "
                        + f"(exp: {expiration})... sent, Polymarket answered in {_send_ms:.0f} ms")

            if gtd_res.get("ok") and gtd_res.get("order_id"):
                order_id = gtd_res["order_id"]
                order_placed_ts = time.time()
                trade["live_status"] = "GTD_OPEN"
                trade["live_order_id"] = order_id
                trade["live_limit_price"] = gtd_price
                trade["live_quoted_price"] = current_ask
                trade["live_expiration"] = expiration
                save_state()
                _sync_active_trades_to_latest_data()
                await broadcast_state()
                log_message(f"LIVE GTD limit order placed for {side}: {gtd_shares:.2f} shares @ {gtd_price:.4f} (order {order_id}) — resting on book")

                # Monitor GTD order until filled, window expires (<15s), or 4s re-quote timeout
                while trade["status"] == "OPEN" and trade.get("live_status") == "GTD_OPEN":
                    now_ts = time.time()
                    if trade.get("end_ts") and now_ts >= trade["end_ts"]:
                        log_message(f"Cancelling resting GTD order {order_id}: window expired")
                        filled = await handle_order_gone_quiet(order_id, trade, token_id, current_ask, gtd_price, "GTD")
                        if not filled:
                            trade["live_status"] = "TIMEOUT"
                            save_state()
                            _sync_active_trades_to_latest_data()
                            await broadcast_state()
                        return

                    # Check status on CLOB
                    ord_status = await asyncio.to_thread(clob_trader.check_order_status, order_id)
                    st = ord_status.get("status")

                    if st == "FILLED":
                        # Never assume fills or prices: verify exact trade fills directly from CLOB API!
                        fills = await asyncio.to_thread(clob_trader.get_order_fills, order_id, token_id)
                        if fills:
                            total_shares = sum(float(f["size"]) for f in fills)
                            total_usd = sum(float(f["size"]) * float(f["price"]) for f in fills)
                            fill_size = total_shares
                            fill_price = total_usd / total_shares if total_shares > 0 else float(ord_status.get("price") or gtd_price)
                            fill_usd = total_usd
                        else:
                            fill_size = float(ord_status.get("size_matched") or gtd_shares)
                            fill_price = float(ord_status.get("price") or gtd_price)
                            fill_usd = fill_size * fill_price

                        trade["live_status"] = "FILLED"
                        trade["live_entry_price"] = fill_price
                        trade["live_shares"] = fill_size
                        trade["live_amount"] = fill_usd
                        trade["live_entry_time"] = datetime.now().isoformat()
                        trade["live_slippage"] = fill_price - current_ask

                        # Immediately deduct spent funds in RAM
                        if fill_usd > 0:
                            new_bal = max(0.0, (state.get("live_balance") or 0.0) - fill_usd)
                            state["live_balance"] = new_bal
                            risk_type = (settings.RISK_TYPE or "percent").lower()
                            if risk_type == "fixed":
                                new_stake = float(settings.RISK_VALUE)
                            else:
                                new_stake = (float(settings.RISK_VALUE) / 100.0) * new_bal
                            state["precomputed_live_stake"] = round(max(1.0, min(new_stake, new_bal)) if new_bal >= 1.0 else 0.0, 2)

                        save_state()
                        _sync_active_trades_to_latest_data()
                        await broadcast_state()

                        msg = (f"LIVE trade FILLED via GTD limit (attempt #{attempt_num}): {side} ${fill_usd:.2f} on {trade['market_slug']} "
                               f"— {fill_size:.2f} shares @ {fill_price:.4f} (paper entry {trade['paper_entry_price']:.4f}, order {order_id})")
                        log_message(msg)
                        await send_telegram(f"🚀 *LIVE Copy FILLED (GTD Limit)*\n• Side: `{side}`\n• Fill Price: `{fill_price:.4f}`\n• Shares: `{fill_size:.2f}`\n• Amount: `${fill_usd:.2f}`\n• Market: `{trade['market_slug']}`")
                        return

                    # Check if partial fill happened while resting on book
                    matched = ord_status.get("size_matched", 0.0)
                    if matched > 0 and matched != trade.get("live_shares"):
                        trade["live_shares"] = matched
                        trade["live_entry_price"] = float(ord_status.get("price") or gtd_price)
                        save_state()
                        _sync_active_trades_to_latest_data()
                        await broadcast_state()

                    # Explicit cancellation confirmed from exchange
                    if st == "CANCELLED":
                        await handle_order_gone_quiet(order_id, trade, token_id, current_ask, gtd_price, "GTD")
                        return

                    # Check if market ask has surged above our resting limit price
                    # If market jumped away, cancel early to re-quote at the higher price instead of waiting 4s passively
                    if now_ts - order_placed_ts >= 1.5:
                        ws_sum = polymarket_clob_ws.get_summary(token_id, max_age_s=5.0)
                        ws_ask = ws_sum.get("bestAsk") if ws_sum else None
                        if ws_ask and ws_ask > gtd_price:
                            log_message(f"Market ask surged ({ws_ask:.4f} > limit {gtd_price:.4f}) — cancelling resting GTD early to re-quote at fresh ask...")
                            filled = await handle_order_gone_quiet(order_id, trade, token_id, current_ask, gtd_price, "GTD")
                            if filled:
                                return
                            if attempt_num < max_attempts:
                                trade["live_status"] = "FILLING"
                                break

                    # 4-Second Resting Lifetime (long shots: settings longshot.resting_s):
                    # Enforce the full resting window. Once reached, cancel and verify all partial fills!
                    if now_ts - order_placed_ts >= resting_s:
                        log_message(f"Resting GTD order {order_id} reached {resting_s:g}s timeout — verifying fills and cancelling on exchange...")
                        filled = await handle_order_gone_quiet(order_id, trade, token_id, current_ask, gtd_price, "GTD")
                        if filled:
                            return

                        if attempt_num < max_attempts:
                            log_message(f"Resting GTD order {order_id} cancelled on exchange — re-quoting at freshest ask (attempt #{attempt_num + 1})...")
                            trade["live_status"] = "FILLING"
                            break
                        else:
                            log_message(f"GTD order {order_id} cancelled on exchange after max attempts ({max_attempts}) — stopping order placement for this window to prevent duplicate entry.")
                            trade["live_status"] = "CANCELLED"
                            save_state()
                            _sync_active_trades_to_latest_data()
                            await broadcast_state()
                            return

                    await asyncio.sleep(0.5)

                if trade.get("live_status") == "FILLED":
                    return
            else:
                err = clob_trader.enrich_order_error(str(gtd_res.get("error", "gtd_placement_failed")))
                if any(k in err for k in ("setup_failed", "client_not_ready", "invalid_private_key", "not enough balance", "balance is not enough")):
                    trade["live_status"] = "FAILED"
                    log_message(f"LIVE copy aborted: {err}")
                    save_state()
                    _sync_active_trades_to_latest_data()
                    await broadcast_state()
                    return

                log_message(f"LIVE copy attempt #{attempt_num} for {side} GTD placement failed: {err} — retrying in {int(retry_interval * 1000)}ms...")

            await asyncio.sleep(retry_interval)
    finally:
        if order_sent is not None:
            order_sent.set()
        for k in slot_keys:
            _in_flight_markets.discard(k)


_BG_FUTURES: set = set()  # keeps fire-and-forget order tasks referenced until they finish


# ═══ PATCH: longshot engine ════════════════════════════════════════════════════════════════
# Settings -> Long Shots -> "Engine long shots" (config.json longshot.engine_enabled). Off (the
# default): nothing here runs and the bot trades exactly as before. On: the kind of long shot the
# original paper bot took — a regular GTD entry (same model, same gates, same vetoes) at an ask of
# longshot.engine_max_price or less is a LONG SHOT instead of a regular trade:
#   * execute_trade hands it on as kind="longshot", band "engine", open_reason "longshot_engine": the
#     long shots' own slot (one per window together with the sniper's, longshot.max_per_window), so
#     a window can hold a regular trade AND a long shot; it is not the window's GTD bet (Follow),
#     and Follow does not apply to it,
#   * its stake: longshot.engine_stake_pct % of the balance, 0 = the GTD stake (as the paper bot),
#   * its live copy (_le_live_fak): ONE fill-and-kill buy at the paper entry + longshot.engine_cap_cents
#     (rounded down to the book's tick): it takes what's offered at that price or better at once and
#     is never re-quoted,
#   * a window whose regular slot is taken is still checked while its long-shot slot is free
#     (entry_block_reason); a regular-price entry there is dropped (_le_block_regular). With
#     longshot.engine_own_slot off it's TOG's rule instead: an engine long shot only when the window
#     has no open position at all, and it is the window's position (no regular trade after it),
#   * the window's final seconds: evaluate_entry stops 5 s before the end and has no context under
#     6 s; the engine keeps the last context it saw (_le_keep_ctx) and keeps checking for a long shot
#     from it and the live book (_le_final_check) down to longshot.engine_min_seconds_left, as the
#     paper bot did (its latest long shot: 2.1 s before the end),
#   * logs/longshots.csv: every engine long shot (ENTRY), every one skipped once per window and
#     reason (SKIP), and its result when the window is booked (RESULT).
LE_LOG_PATH = os.path.join("logs", "longshots.csv")
LE_FINAL_CTX_MAX_AGE_S = 10.0   # the kept context may be this old in the final seconds (it's built up to 6 s before the end)
LE_RESEND_GAP_S = 0.5           # PATCH: engine re-sends — an order is sent again no sooner than this after its last send was answered
LE_RESEND_POLL_S = 0.05         # PATCH: engine re-sends — how often the live book is looked at while an order waits to be sent again
LE_LOG_HEADER = ["time", "event", "trade_id", "market", "side", "seconds_left", "model_prob", "ask",
                 "order_price", "order_shares", "order_usd", "answer_ms", "live_status", "filled_shares",
                 "fill_price", "fill_usd", "result", "paper_pl", "live_pl", "note"]
_le_last_ctx = None        # the last entry context seen (for the window's final seconds)
_le_skips_logged = set()   # (window, reason) already in the log


def _le_on() -> bool:
    return longshot.engine_on(settings.LONGSHOT)


def _le_own_slot() -> bool:
    """True: an engine long shot sits next to the window's regular trade (the long shots' slot).
    False: TOG's rule — only when the window has no open position, and no regular trade after it."""
    return bool(settings.LONGSHOT.get("engine_own_slot", True))


def _is_ls_engine(t: Dict[str, Any]) -> bool:
    """An engine long shot: a GTD entry taken as a long shot (open_reason "longshot_engine")."""
    return t.get("open_reason") == "longshot_engine"


def _le_end_ts(market: Dict[str, Any]) -> Optional[float]:
    try:
        return datetime.fromisoformat(str(market.get("endDate")).replace('Z', '+00:00')).timestamp()
    except Exception:
        return None


def _le_kind(decision: Dict[str, Any], market_prices: Dict[str, Any], market: Dict[str, Any]) -> Optional[str]:
    """What the engine does with a regular entry: None = not the engine's (engine off, no entry, or a
    regular price), "longshot" = take it as a long shot, any other text = skip it (the reason)."""
    cfg = settings.LONGSHOT
    if not longshot.engine_on(cfg) or (decision or {}).get("action") != "ENTER":
        return None
    side = decision.get("side")
    try:
        price = float((market_prices or {}).get("up" if side == "UP" else "down"))
    except (TypeError, ValueError):
        return None
    if not (0.0 < price <= float(cfg.get("engine_max_price", 0.10)) + 1e-9):
        return None
    end_ts = _le_end_ts(market or {})
    min_s = float(cfg.get("engine_min_seconds_left", 0.0) if cfg.get("engine_min_seconds_left") is not None else 0.0)
    if end_ts is not None and (end_ts - time.time()) <= min_s:
        return "le_too_late"
    return "longshot"


def _le_decision(decision: Dict[str, Any], market_prices: Dict[str, Any], market: Dict[str, Any]) -> Dict[str, Any]:
    """The engine's ENTER as a long-shot decision: band "engine", the engine's chance for the side,
    the ask, the multiple, the live order's price (the cap) and the long-shot info (seconds left)."""
    side = decision["side"]
    price = float(market_prices["up" if side == "UP" else "down"])
    prob = decision.get("prob")
    end_ts = _le_end_ts(market or {})
    return dict(decision, band="engine", price=price, prob=prob,
                multiple=(float(prob) / price) if (prob is not None and price > 0) else None,
                max_price=longshot.engine_order_price(price, settings.LONGSHOT), phase="LONGSHOT", strength="LONGSHOT",
                longshot={"secs_left": round(end_ts - time.time(), 2) if end_ts else None, "ev": decision.get("ev"),
                          "engine_reason": decision.get("reason")})


def _le_window_busy(mid: str, slug: str, sms) -> Dict[str, bool]:
    """Which of a window's two slots are taken (in flight or in active_trades): the regular trade's,
    and the long shots' (longshot.max_per_window of them, the sniper's and the engine's together);
    "engine": the window holds an engine long shot."""
    keys = [k for k in (mid, slug) if k]
    busy = {"regular": any(k in _in_flight_markets for k in keys),
            "longshot": any(("ls:" + k) in _in_flight_markets for k in keys), "engine": False}
    n_ls = 0
    one_at_a_time = longshot.one_at_a_time_on(settings.LONGSHOT)
    for t in state["active_trades"]:
        if ((mid and str(t.get("market_id")) == mid) or (slug and t.get("market_slug") == slug)
                or (sms is not None and t.get("window_start_ms") == int(sms))):
            if _is_longshot(t):
                if one_at_a_time:
                    n_ls += 1
                    busy["longshot"] = True
                    busy["engine"] = True
                else:
                    n_ls += 0 if _is_ls_maker(t) else 1
                    busy["engine"] = busy["engine"] or _is_ls_engine(t)
            else:
                busy["regular"] = True
    if one_at_a_time and sms is not None:
        if hasattr(ls_maker_mgr, "has_active_bid") and ls_maker_mgr.has_active_bid(int(sms)):
            busy["longshot"] = True
            busy["engine"] = True
        if int(sms) in _ls_entered_windows:
            busy["longshot"] = True
            busy["engine"] = True
    if n_ls >= int(settings.LONGSHOT.get("max_per_window", 1)):
        busy["longshot"] = True
    return busy


def _le_block_regular(decision: Dict[str, Any], ctx: Dict[str, Any], up, down) -> bool:
    """evaluate_entry: True = don't send this entry on. A window whose regular slot is taken may only
    enter a long shot (a cheap entry goes on: execute_trade hands it to the engine)."""
    if not _le_on() or (decision or {}).get("action") != "ENTER":
        return False
    if not ((ctx or {}).get("le_busy") or {}).get("regular"):
        return False
    return _le_kind(decision, {"up": up, "down": down}, (ctx or {}).get("market") or {}) is None


def _le_keep_ctx(ctx: Optional[Dict[str, Any]]):
    """evaluate_entry: remember the context it just used (a copy), for the window's final seconds."""
    global _le_last_ctx
    if ctx and _le_on():
        _le_last_ctx = dict(ctx)


def _le_final_ctx(ctx: Optional[Dict[str, Any]], now: float) -> Optional[Dict[str, Any]]:
    """evaluate_entry: the context to check for a long shot with in the window's final seconds, or
    None when this isn't that time (the regular check carries on as before). The final seconds
    start where the regular check stops, 5 s before the end (or where it runs out of context: the
    update loop publishes none under 6 s), and end engine_min_seconds_left before the end."""
    cfg = settings.LONGSHOT
    if not longshot.engine_on(cfg):
        return None
    c_min = cfg.get("engine_min_seconds_left")
    floor_s = float(c_min if c_min is not None else 0.0)
    if floor_s >= 5.0:
        return None
    use = ctx or _le_last_ctx
    if not use:
        return None
    end_ts = _le_end_ts(use.get("market") or {})
    if end_ts is None or now >= end_ts - floor_s:
        return None
    if ctx:
        return use if now >= end_ts - 5.0 else None          # the regular check would stop here
    return use if now >= end_ts - 6.5 and now - use.get("built_at", 0) <= LE_FINAL_CTX_MAX_AGE_S else None


async def _le_final_check(ctx: Dict[str, Any], reason: str, now: float):
    """evaluate_entry for the window's final seconds, engine long shots only: the same model inputs
    (the kept context, the live BTC price, the live book) and the same engine; an entry at a regular
    price is left alone (the regular check stopped 5 s before the end, as before)."""
    mkt = ctx.get("market") or {}
    if not ctx.get("strike_open") or not ctx.get("target_open"):
        return
    if entry_block_reason(ctx):
        return
    spot_price = binance_stream.get_last().get("price")
    if not spot_price:
        return
    mc_steps = _model_steps(ctx["time_left_min"])
    fair_up = indicators.fair_prob_up(spot_price, ctx["target_open"], mc_steps, ctx["sigma_5m"], drift_per_step=ctx["drift_5m"])
    up_ws = polymarket_clob_ws.get_summary(ctx["token_ids"].get("up"), max_age_s=settings.MAX_BOOK_AGE_S)
    down_ws = polymarket_clob_ws.get_summary(ctx["token_ids"].get("down"), max_age_s=settings.MAX_BOOK_AGE_S)
    market_up = up_ws.get("bestAsk") if up_ws else None
    market_down = down_ws.get("bestAsk") if down_ws else None
    book = dict(ctx.get("orderbook") or {})
    if up_ws:
        book["up"] = {"bestBid": up_ws.get("bestBid"), "bestAsk": market_up, "askLiquidity": up_ws.get("askLiquidity")}
    if down_ws:
        book["down"] = {"bestBid": down_ws.get("bestBid"), "bestAsk": market_down, "askLiquidity": down_ws.get("askLiquidity")}
    if market_up is None or market_down is None:
        # no live book: the kept context's prices only while they're as fresh as the book may be
        if now - ctx.get("built_at", 0) > settings.MAX_BOOK_AGE_S:
            return
        market_up = market_up if market_up is not None else ctx["prices"]["up"]
        market_down = market_down if market_down is not None else ctx["prices"]["down"]
    decision = engines.decide_ev({
        "mcProbUp": fair_up,
        "priceUp": market_up,
        "priceDown": market_down,
        "minProb": settings.MIN_PROB_EV,
        "evThreshold": settings.EV_THRESHOLD,
        "rsi": ctx["rsi"],
        "haExhaustedGreen": ctx["ha_exhausted_green"],
        "haExhaustedRed": ctx["ha_exhausted_red"],
    })
    market_prices = {"up": market_up, "down": market_down}
    if decision.get("action") != "ENTER" or _le_kind(decision, market_prices, mkt) != "longshot":
        return
    exec_res = await execute_trade(
        decision, market_prices, mkt, ctx["strike_open"], ctx["token_ids"], book,
        strike_source=ctx["strike_source"], window_start_ms=ctx["start_ms"],
        open_reason=f"event_entry_{reason}"
    )
    state["event_exec"] = {"ts": datetime.now().isoformat(), "side": decision["side"], "reason": reason, "result": exec_res}
    if exec_res == "entered":
        await broadcast_state()


def _le_log(event: str, trade: Optional[Dict[str, Any]] = None, **fields):
    """One row of logs/longshots.csv (a failure to write never stops the bot)."""
    row = {"time": datetime.now().isoformat(timespec="milliseconds"), "event": event}
    if trade:
        lsd = trade.get("longshot") or {}
        row.update(trade_id=trade.get("trade_id"), market=trade.get("market_slug"), side=trade.get("side"),
                   seconds_left=lsd.get("secs_left"), model_prob=lsd.get("prob"), ask=trade.get("paper_entry_price"),
                   order_price=lsd.get("max_price"), order_shares=trade.get("le_order_shares"),
                   order_usd=trade.get("le_order_usd"), answer_ms=trade.get("le_answer_ms"),
                   live_status=trade.get("live_status"), filled_shares=trade.get("live_shares"),
                   fill_price=trade.get("live_entry_price"), fill_usd=trade.get("live_amount"))
    row.update(fields)
    try:
        utils.append_csv_row(LE_LOG_PATH, LE_LOG_HEADER,
                             [round(v, 6) if isinstance(v, float) else ("" if v is None else v)
                              for v in (row.get(k) for k in LE_LOG_HEADER)])
    except Exception:
        pass


def _le_log_skip(market: Dict[str, Any], decision: Dict[str, Any], market_prices: Dict[str, Any], reason: str):
    """An engine long shot not taken: in the log and logs/longshots.csv, once per window and reason."""
    key = (str(market.get("id") or market.get("slug") or ""), reason)
    if key in _le_skips_logged:
        return
    if len(_le_skips_logged) > 300:
        _le_skips_logged.clear()
    _le_skips_logged.add(key)
    side = decision.get("side")
    ask = (market_prices or {}).get("up" if side == "UP" else "down")
    end_ts = _le_end_ts(market)
    left = round(end_ts - time.time(), 1) if end_ts else None
    c_min = settings.LONGSHOT.get("engine_min_seconds_left")
    min_s = float(c_min if c_min is not None else 0.0)
    why = {"le_too_late": f"window closed ({min_s:g}s left)" if min_s > 0 else "window closed",
           "le_slot_busy": "this window already has its long shot"}.get(reason, reason)
    log_message(f"LONGSHOT engine: {side} @ {ask} on {market.get('slug')} skipped ({why}; {left}s left)")
    _le_log("SKIP", market=market.get("slug"), side=side, seconds_left=left, model_prob=decision.get("prob"),
            ask=ask, note=reason)


def _le_log_result(trade: Dict[str, Any]):
    """update_trades: an engine long shot's result (WIN / LOSS / VOID) in logs/longshots.csv."""
    if not _is_ls_engine(trade):
        return
    result = "VOID" if trade.get("status") == "VOID" else ("WIN" if (trade.get("paper_profit_loss") or 0) > 0 else "LOSS")
    _le_log("RESULT", trade, result=result, paper_pl=trade.get("paper_profit_loss"),
            live_pl=trade.get("live_profit_loss"), note=trade.get("resolution"))


def _le_fak_fill(res: Dict[str, Any], order_shares: float):
    """Polymarket's answer to the fill-and-kill (place_limit_buy_async's result) -> (shares, dollars)
    when it says it matched, all or part; None when it carries no fill amounts."""
    data = res.get("response") if isinstance(res.get("response"), dict) else {}
    status = str(data.get("status") or res.get("status") or "").lower()
    try:
        making = float(data.get("making_amount") or data.get("makingAmount") or 0.0)   # dollars (a BUY makes dollars)
        taking = float(data.get("taking_amount") or data.get("takingAmount") or 0.0)   # shares
    except (TypeError, ValueError):
        return None
    if taking > float(order_shares) * 1000.0:        # given in 6-decimal units
        making, taking = making / 1e6, taking / 1e6
    if status in ("matched", "filled", "closed") and making > 0 and taking > 0:
        return taking, making
    return None


async def _le_order_fills(order_id: Optional[str], token_id: str, status: Optional[str]):
    """An accepted fill-and-kill whose answer has no fill amounts: look the order's fills up (the
    trades list can lag a second or two). A fill-and-kill never rests; if it says it does, cancel it."""
    if not order_id:
        return None
    if str(status or "").lower() in ("live", "unmatched", "open"):
        try:
            await asyncio.to_thread(clob_trader.cancel_order, order_id)
        except Exception:
            pass
    for wait in (0.5, 1.0, 1.5):
        await asyncio.sleep(wait)
        try:
            fills = await asyncio.to_thread(clob_trader.get_order_fills, order_id, token_id)
        except Exception:
            fills = []
        if fills:
            return (sum(float(f["size"]) for f in fills), sum(float(f["size"]) * float(f["price"]) for f in fills))
    return None


# PATCH: engine re-sends (longshot.engine_resends, 0 = off). After Polymarket's definite answer that nothing,
# or only part, was there to match, the SAME order goes out again when the live book shows the ask back at
# its price or under.
def _le_bg(coro):
    """Run a notification in the background, kept referenced until it is done: it must not hold up
    an order that is waiting to go out again."""
    fut = asyncio.ensure_future(coro)
    _BG_FUTURES.add(fut)
    fut.add_done_callback(_BG_FUTURES.discard)


def _le_rest_shares(trade: Dict[str, Any], price: float) -> float:
    """What is still unfilled of an engine long shot's order, in whole order steps; 0 when that is
    under Polymarket's minimum order (5 shares and $1)."""
    step = longshot.share_step(price)
    rest = float(trade.get("le_order_shares") or 0.0) - float(trade.get("live_shares") or 0.0)
    rest = math.floor(rest / step + 1e-9) * step
    return float(rest) if rest >= longshot.order_shares(0.0, price) else 0.0


async def _le_resend(trade: Dict[str, Any], answered: Optional[float] = None):
    """The re-sends of an engine long shot's live order (longshot.engine_resends of them at most). It
    runs after Polymarket's definite answer (at the time `answered`) that nothing, or only part, was
    there to match: the SAME order — the same price, never higher, for what is still unfilled — goes
    out again whenever the live book shows the ask back at that price or under, no sooner than
    LE_RESEND_GAP_S after the last answer, until max(engine_min_seconds_left, 1 s) before the end.
    Every answer is booked like the first; an answer that is neither a fill with its amounts nor
    "nothing to match" ends it."""
    side, token_id, slug = trade.get("side"), trade.get("token_id"), trade.get("market_slug")
    price = float((trade.get("longshot") or {}).get("max_price") or 0.0)
    quoted = float(trade.get("live_quoted_price") or trade.get("paper_entry_price") or price)
    end_ts = float(trade.get("end_ts") or 0.0)
    if not token_id or price <= 0 or end_ts <= 0 or _le_rest_shares(trade, price) <= 0:
        return
    sent, last_answer, why = 0, float(answered or time.time()), ""
    try:
        while not why:
            cfg = settings.LONGSHOT
            limit = int(cfg.get("engine_resends", 0) or 0)
            rest = _le_rest_shares(trade, price)
            now = time.time()
            if rest <= 0:
                break                                           # filled (or what's left is under the minimum order)
            if sent >= limit:
                why = f"sent again {sent} time{'s' if sent != 1 else ''}, the most allowed" if limit > 0 else "re-sends are switched off"
            elif now >= end_ts - max(1.0, float(cfg.get("engine_min_seconds_left", 2.0) or 0.0)):
                why = "the window is closing"
            elif (trade.get("status") != "OPEN" or not state.get("running") or state.get("withdraw_state") == "in_progress"
                    or not longshot.engine_on(cfg)):
                why = "stopped"
            if why:
                break
            book = (polymarket_clob_ws.get_summary(token_id, max_age_s=settings.MAX_BOOK_AGE_S)
                    if now - last_answer >= LE_RESEND_GAP_S else None)
            ask = (book or {}).get("bestAsk")
            if ask is None or float(ask) > price + 1e-9:        # nothing at the order's price: wait
                await asyncio.sleep(LE_RESEND_POLL_S)
                continue
            balance = float(state.get("live_balance") or 0.0)
            if rest * price > balance + 1e-9:                   # what the balance still covers
                step = longshot.share_step(price)
                rest = math.floor(balance / price / step + 1e-9) * step
                if rest < longshot.order_shares(0.0, price):
                    why = f"the balance (${balance:.2f}) is under the minimum order"
                    break
            usd = round(rest * price, 2)
            sent += 1
            trade["le_resends"] = sent
            trade["live_attempts"] = int(trade.get("live_attempts") or 1) + 1
            had_fill = trade.get("live_status") == "FILLED"
            left = end_ts - now
            t_send = time.perf_counter()
            try:
                res = await clob_trader.place_limit_buy_async(token_id, rest, price, 0, longshot.tick_size_str(price), "FAK")
            except Exception as e:                              # the call itself failed: recorded like a failed answer
                res = {"ok": False, "error": f"{type(e).__name__}: {e}"}
            ms = (time.perf_counter() - t_send) * 1000.0
            last_answer = time.time()
            oid = res.get("order_id")
            if oid:
                trade["live_order_id"] = oid
                trade["live_order_ids"] = list(trade.get("live_order_ids") or []) + [oid]
            head = (f"LONGSHOT engine LIVE order for {side} on {slug}: sent again ({sent} of {limit}) with the ask at "
                    f"{float(ask):.3f}, {left:.1f}s left: fill-and-kill {rest:g} shares at {price:.3f} or better (${usd:.2f}), "
                    f"Polymarket answered in {ms:.0f} ms: ")
            fill = _le_fak_fill(res, rest)
            if fill is None and res.get("ok"):                  # accepted without amounts: its fills, and no more sends
                fill = await _le_order_fills(oid, token_id, res.get("status"))
                why = "the last answer came without its amounts"
            note = f"sent again with the ask at {float(ask):.3f}"
            if fill and not had_fill and trade.get("live_status") == "FILLED":
                why = "the wallet check has booked the fills"    # update_trades got there first (after the end)
                log_message(head + "filled, already booked by the wallet check")
            elif fill:
                _ls_add_fill(trade, fill[0], fill[1], quoted)
                trade["live_status"] = "FILLED"
                trade["ls_partial"] = float(trade["live_shares"]) < float(trade.get("le_order_shares") or 0.0) * 0.999
                log_message(head + f"FILLED {fill[0]:.2f} shares @ {fill[1] / fill[0]:.4f} (${fill[1]:.2f}); in all "
                            f"{trade['live_shares']:.2f} shares @ {trade['live_entry_price']:.4f} (${trade['live_amount']:.2f})"
                            + (", part of the order" if trade.get("ls_partial") else ""))
            elif res.get("ok"):
                note = f"accepted ({res.get('status')}), no fill found"
                log_message(head + note)
            else:
                err = clob_trader.enrich_order_error(str(res.get("error") or "order_failed"))
                if "no orders found to match" in err.lower() or "no matching orders" in err.lower():
                    note = f"nothing offered at {price:.3f} or better"
                    log_message(head + f"MISSED ({note})")
                else:
                    note, why = err, "the last send failed"
                    if not had_fill:
                        trade["live_error"] = err
                    log_message(head + f"FAILED ({err})")
            _le_log("RESEND", trade, seconds_left=round(left, 1), ask=float(ask), order_shares=rest, order_usd=usd,
                    answer_ms=round(ms, 1), note=note)
            try:
                await asyncio.to_thread(save_state)
                _sync_active_trades_to_latest_data()
                await broadcast_state()
                if fill and trade.get("live_status") == "FILLED" and not why.startswith("the wallet"):
                    _le_bg(send_telegram(f"🎯 *LIVE Engine Long Shot FILLED* (sent again)\n• Side: `{side}`\n• Fill Price: `{trade['live_entry_price']:.4f}`"
                                         f"\n• Shares: `{trade['live_shares']:.2f}`\n• Amount: `${trade['live_amount']:.2f}`"
                                         f"\n• Market: `{slug}`"))
            except Exception:
                pass
    except Exception as e:
        why = f"{type(e).__name__}: {e}"
    if why and _le_rest_shares(trade, price) > 0:
        got = (f"{float(trade.get('live_shares') or 0.0):.2f} of {float(trade.get('le_order_shares') or 0.0):g} shares filled"
               if trade.get("live_status") == "FILLED" else "nothing filled")
        log_message(f"LONGSHOT engine LIVE order for {side} on {slug}: not sent again ({why}); {got}")


async def _le_live_fak(trade: Dict[str, Any], keys: List[str], order_sent: Optional[asyncio.Event] = None,
                       evaluated_price: Optional[float] = None):
    """The live copy of an engine long shot: ONE fill-and-kill buy of the engine's stake (whole shares,
    cost in whole cents, Polymarket's minimums) at trade["longshot"]["max_price"] — the paper entry +
    engine_cap_cents — or better. live_status afterwards: FILLED (all or part: ls_partial), MISSED
    (nothing offered at that price), INSUFFICIENT_FUNDS or FAILED (live_error)."""
    side, token_id, note = trade.get("side"), trade.get("token_id"), ""
    cfg = settings.LONGSHOT
    shares = usd = send_ms = answered = None
    again = False              # PATCH: engine re-sends — Polymarket's answer says exactly what is still unfilled: it may be sent again
    t_decision = time.perf_counter()
    try:
        price = float((trade.get("longshot") or {}).get("max_price") or 0.0)
        quoted = float(evaluated_price or trade.get("paper_entry_price") or price)
        live_balance = float(state.get("live_balance") or 0.0)
        if live_balance <= 0:
            live_balance = float(await asyncio.to_thread(clob_trader.get_usdc_balance) or 0.0)
            state["live_balance"] = live_balance
        stake = longshot.engine_stake_for(live_balance, cfg)
        shares = longshot.order_shares(stake, price, max_usd=live_balance) if price > 0 else 0.0
        usd = round(shares * price, 2)
        if not token_id or price <= 0:
            trade["live_status"], trade["live_error"] = "FAILED", ("missing token_id" if not token_id else "no order price")
            note = trade["live_error"]
        elif usd <= 0 or live_balance < usd:
            trade["live_status"] = "INSUFFICIENT_FUNDS"
            note = f"balance ${live_balance:.2f} is under the ${usd:.2f} minimum ({shares:g} shares @ {price:.3f})"
        else:
            ls_ot = str((cfg or {}).get("order_type") or "FAK").upper()
            if ls_ot not in ("FAK", "FOK", "GTC", "GTD"):
                ls_ot = "FAK"
            trade["live_status"] = f"{ls_ot}_SENT"
            trade["live_attempts"] = 1
            trade["live_limit_price"] = price
            trade["live_quoted_price"] = quoted
            trade["le_order_usd"] = usd
            trade["le_order_shares"] = shares
            t_send = time.perf_counter()
            exp = int(time.time() + 300) if ls_ot == "GTD" else 0
            res = await clob_trader.place_limit_buy_async(token_id, shares, price, exp, longshot.tick_size_str(price), ls_ot)
            t_answer = time.perf_counter()
            answered = time.time()                                # PATCH: engine re-sends
            send_ms = (t_answer - t_send) * 1000.0
            trade["le_answer_ms"] = round((t_answer - t_decision) * 1000.0, 1)
            if order_sent is not None:
                order_sent.set()
            oid = res.get("order_id")
            if oid:
                trade["live_order_id"] = oid
                trade["live_order_ids"] = [oid]
            fill = _le_fak_fill(res, shares)
            again = fill is not None                              # PATCH: engine re-sends — an answer with its amounts
            if fill is None and res.get("ok"):
                fill = await _le_order_fills(oid, token_id, res.get("status"))
            if fill:
                f_shares, f_usd = fill
                _ls_add_fill(trade, f_shares, f_usd, quoted)
                trade["live_status"] = "FILLED"
                trade["ls_partial"] = f_shares < float(shares) * 0.999
            elif res.get("ok") and ls_ot in ("GTC", "GTD"):
                trade["live_status"] = "OPEN"
                note = f"resting {ls_ot} placed on book (order {oid})"
            elif res.get("ok"):
                trade["live_status"] = "MISSED"
                note = f"accepted ({res.get('status')}), no fill found"
            else:
                err = clob_trader.enrich_order_error(str(res.get("error") or "order_failed"))
                if "no orders found to match" in err.lower() or "no matching orders" in err.lower():
                    trade["live_status"] = "MISSED"
                    note = f"nothing offered at {price:.3f} or better"
                    again = True                                  # PATCH: engine re-sends — a definite "nothing to match"
                else:
                    trade["live_status"], trade["live_error"] = "FAILED", err
                    note = err
    except Exception as e:
        trade["live_status"], trade["live_error"] = "FAILED", f"{type(e).__name__}: {e}"
        note = trade["live_error"]
    finally:
        if order_sent is not None:
            order_sent.set()
        for k in keys:
            _in_flight_markets.discard(k)

    st = trade.get("live_status")
    resends = int(settings.LONGSHOT.get("engine_resends", 0) or 0)   # PATCH: engine re-sends
    again = (again and resends > 0 and st in ("FILLED", "MISSED")
             and _le_rest_shares(trade, float((trade.get("longshot") or {}).get("max_price") or 0.0)) > 0)
    more = (f"; it goes out again when the ask is back at {float(trade['longshot']['max_price']):.3f} or under "
            f"(up to {resends} time{'s' if resends != 1 else ''})") if again else ""
    ls_ot = str((cfg or {}).get("order_type") or "FAK").upper()
    head = (f"LONGSHOT engine LIVE order for {side} on {trade.get('market_slug')}: {ls_ot.lower()}"
            + (f" {shares:g} shares at {float(trade['longshot']['max_price']):.3f} or better (${usd:.2f})" if usd else "")
            + f" (paper entry {float(trade.get('paper_entry_price') or 0.0):.4f})"
            + (f", Polymarket answered in {send_ms:.0f} ms" if send_ms is not None else "") + ": ")
    if st == "FILLED":
        log_message(head + f"FILLED {trade['live_shares']:.2f} shares @ {trade['live_entry_price']:.4f} "
                    f"(${trade['live_amount']:.2f}{', part of the order' if trade.get('ls_partial') else ''})" + more)
    else:
        log_message(head + str(st) + (f" ({note})" if note else "") + more)
    _le_log("ENTRY", trade, note=note)
    # PATCH: engine re-sends — the watch for a re-send starts now, next to the saving and the messages below
    watch = asyncio.ensure_future(_le_resend(trade, answered)) if again else None
    try:
        await asyncio.to_thread(save_state)
        _sync_active_trades_to_latest_data()
        await broadcast_state()
        if st == "FILLED":
            await send_telegram(f"🎯 *LIVE Engine Long Shot FILLED*\n• Side: `{side}`\n• Fill Price: `{trade['live_entry_price']:.4f}`"
                                f"\n• Shares: `{trade['live_shares']:.2f}`\n• Amount: `${trade['live_amount']:.2f}`"
                                f"\n• Market: `{trade['market_slug']}`")
    except Exception:
        pass
    if watch is not None:                                         # PATCH: engine re-sends
        await watch


async def execute_trade(decision: Dict[str, Any], market_prices: Dict[str, Any], market: Dict[str, Any], strike_open: Optional[float], token_ids: Dict[str, Any], orderbook: Optional[Dict[str, Any]] = None,
                        strike_source: str = "chainlink_ws", window_start_ms: Optional[int] = None,
                        open_reason: str = "ev_entry", kind: str = "regular"):
    if decision["action"] != "ENTER":
        return decision.get("reason", "no_trade")

    cur_mkt_id = str(market.get("id") or "")
    cur_slug = str(market.get("slug") or "")
    # PATCH: longshot engine — a regular entry at an ask of longshot.engine_max_price or less is a
    # long shot (band "engine", the long shots' own slot and stake, one fill-and-kill live order); one
    # with too little time left is skipped. Engine off: _le_kind() is None and nothing changes.
    if kind == "regular":
        _le_k = _le_kind(decision, market_prices, market)
        if _le_k == "longshot":
            decision, kind, open_reason = _le_decision(decision, market_prices, market), "longshot", "longshot_engine"
        elif _le_k is not None:
            _le_log_skip(market, decision, market_prices, _le_k)
            return _le_k
    is_ls = kind == "longshot"  # PATCH: longshot
    ls_cfg = settings.LONGSHOT if is_ls else {}
    slot_keys = _slot_keys(market, kind)

    async with _trade_lock:
        if any(k in _in_flight_markets for k in slot_keys):
            return "slot_busy"

        def _same_window(t):
            return ((cur_mkt_id and str(t.get("market_id")) == cur_mkt_id)
                    or (cur_slug and t.get("market_slug") == cur_slug)
                    or (window_start_ms is not None and t.get("window_start_ms") == int(window_start_ms)))

        if is_ls:
            if longshot.one_at_a_time_on(ls_cfg):
                # Exclusive mode: strictly ONE longshot strategy per window
                has_any_ls = any(_is_longshot(t) and _same_window(t) for t in state["active_trades"])
                maker_active = (ls_maker_mgr.has_active_bid(int(window_start_ms))
                                if (window_start_ms is not None and hasattr(ls_maker_mgr, "has_active_bid")) else False)
                ls_entered = (window_start_ms is not None and int(window_start_ms) in _ls_entered_windows)
                if has_any_ls or ls_entered or (open_reason != "longshot_maker" and maker_active):
                    if open_reason == "longshot_engine":
                        _le_log_skip(market, decision, market_prices, "one_ls_at_a_time")
                    log_message(f"LONGSHOT skipped: Exclusive mode active and another longshot is already placed in this window.")
                    return "slot_busy"
            else:
                # PATCH: longshot — up to longshot.max_per_window long shots per window;
                # the regular trade (if any) doesn't count.
                # (PATCH: longshot maker — maker fills have their own cap, maker.max_fills_per_window)
                n_ls = sum(1 for t in state["active_trades"] if _is_longshot(t) and not _is_ls_maker(t) and _same_window(t))
                if n_ls >= int(ls_cfg.get("max_per_window", 1)):
                    if open_reason == "longshot_engine":   # PATCH: longshot engine — once per window in the log
                        _le_log_skip(market, decision, market_prices, "le_slot_busy")
                    return "slot_busy"
            if open_reason == "longshot_engine" and not _le_own_slot() and (   # PATCH: longshot engine — own slot off: only with no open position
                    any(k in _in_flight_markets for k in _slot_keys(market, "regular"))
                    or any(not _is_longshot(t) and _same_window(t) for t in state["active_trades"])):
                _le_log_skip(market, decision, market_prices, "le_slot_busy")
                return "slot_busy"
        else:
            # CONSTRAINT: Only one position per market window (do NOT block entering a new window while an old one resolves)
            # PATCH: longshot — long-shot trades don't take the regular slot.
            if not _le_own_slot() and any(_is_ls_engine(t) and _same_window(t) for t in state["active_trades"]):
                return "slot_busy"   # PATCH: longshot engine — own slot off: the engine long shot is the window's position
            for t in state["active_trades"]:
                if not _is_longshot(t) and _same_window(t):
                    return "slot_busy"

        if state["withdraw_state"] == "in_progress":
            return "withdraw_in_progress"

        if strike_open is None:
            return "no_strike"

        side = decision["side"]
        price = market_prices["up"] if side == "UP" else market_prices["down"]
        if price is None:
            return "no_price"

        # Internal paper balance risk sizing
        balance = state["paper_balance"]
        if balance <= 0 and settings.PRIVATE_KEY:
            balance = state["paper_balance"] = float(settings.PAPER_BALANCE_USD or 1000.0)
        risk_type = (settings.RISK_TYPE or "percent").lower()
        if is_ls and decision.get("band") == "engine":   # PATCH: longshot engine — its own stake (0 = the GTD stake)
            paper_amount = longshot.engine_stake_for(balance, ls_cfg)
        elif is_ls:  # PATCH: longshot — fixed, % of balance or growth-max (per band if the band has its own)
            # (PATCH: longshot maker — a growth stake prices the share with the taker fee)
            paper_amount = longshot.stake_for(balance, ls_cfg, decision.get("band"), decision.get("prob"),
                                              longshot.taker_price(ls_cfg, price))
        elif risk_type == "fixed":
            paper_amount = float(settings.RISK_VALUE)
        else:
            paper_amount = (float(settings.RISK_VALUE) / 100.0) * balance

        if paper_amount <= 0:
            return "stake_zero"

        ob = (orderbook or {}).get("up" if side == "UP" else "down") or {}
        ask_liq_shares = ob.get("askLiquidity")
        ask_liq_usd = None
        min_liq = float(ls_cfg.get("min_ask_liquidity_usd", 0.0)) if is_ls else settings.MIN_BOOK_LIQUIDITY_USD
        if is_ls and decision.get("band") == "engine":   # PATCH: longshot engine — the GTD book check, as the paper bot
            min_liq = settings.MIN_BOOK_LIQUIDITY_USD
        if ask_liq_shares is not None and price > 0:
            ask_liq_usd = ask_liq_shares * price
            if ask_liq_usd < min_liq:
                _log_once(f"thin:{'ls:' if is_ls else ''}{cur_slug or cur_mkt_id}:{side}",
                          f"Skip {side}{' long shot' if is_ls else ''}: thin book (${ask_liq_usd:.2f} ask liquidity)")
                return "thin_book"
            paper_amount = min(paper_amount, ask_liq_usd)

        if settings.PRIVATE_KEY and balance < paper_amount and balance > 0:
            paper_amount = balance

        if balance < paper_amount or paper_amount <= 0:
            if f"bal:{cur_slug}" not in _logged_once:
                _logged_once[f"bal:{cur_slug}"] = time.time()
                print(f"Insufficient internal paper balance ({balance})")
            return "insufficient_balance"

        end_date_str = market.get("endDate")
        end_ts = 0
        if end_date_str:
            try:
                end_ts = datetime.fromisoformat(end_date_str.replace('Z', '+00:00')).timestamp()
            except Exception:
                pass
        if not end_ts:
            end_ts = time.time() + settings.CANDLE_WINDOW_MINUTES * 60

        token_id = token_ids.get("up") if side == "UP" else token_ids.get("down")
        trade_id = f"{cur_mkt_id}_{int(time.time() * 1000)}{'_ls' if is_ls else ''}"
        live_on = bool(settings.PRIVATE_KEY) and (not is_ls or bool(ls_cfg.get("live", True)))

        trade = {
            "trade_id": trade_id,
            "market_id": market["id"],
            "market_slug": market.get("slug"),
            "side": side,
            "token_id": token_id,

            # ── Paper execution (immediate) ──────────────────────────────────
            "paper_entry_price": price,
            "paper_amount": paper_amount,
            "paper_shares": paper_amount / price,
            "paper_entry_time": datetime.now().isoformat(),
            "paper_close_price": None,
            "paper_closed_time": None,
            "paper_profit_loss": None,

            # ── Live execution (continuous GTD copy worker) ───────────────────
            "live_status": "FILLING" if live_on else ("PAPER_ONLY" if settings.PRIVATE_KEY else "NO_CREDS"),
            "live_attempts": 0,
            "live_entry_price": None,
            "live_amount": None,
            "live_shares": None,
            "live_entry_time": None,
            "live_order_id": None,
            "live_mark_price": None,
            "live_unrealized_pl": None,
            "live_close_price": None,
            "live_closed_time": None,
            "live_profit_loss": None,

            # ── Lifecycle ───────────────────────────────────────────────────
            "status": "OPEN",
            "awaiting_resolution": False,
            "strike_price": strike_open,
            "strike_source": strike_source,
            "window_start_ms": int(window_start_ms) if window_start_ms is not None else None,
            "open_reason": open_reason,
            "end_ts": end_ts,
            "mode": "hybrid",
            "kind": kind,  # PATCH: longshot — "regular" | "longshot"
        }
        if is_ls:
            trade["longshot"] = dict(decision.get("longshot") or {}, band=decision.get("band"),
                                     prob=decision.get("prob"), multiple=decision.get("multiple"),
                                     max_price=decision.get("max_price"))

        for k in slot_keys:
            _in_flight_markets.add(k)

        trade_dispatched = False
        try:
            state["paper_balance"] -= paper_amount
            state["active_trades"].append(trade)
            state["last_trade_side"] = side
            if is_ls:
                lsd = trade["longshot"]
                msg = (f"Executed PAPER long shot ({lsd.get('band')}): {side} @ {price:.4f} for {market.get('slug')} "
                       f"(Amount: ${paper_amount:.2f}, chance {float(lsd.get('prob') or 0):.3f}, ×{float(lsd.get('multiple') or 0):.1f}, "
                       f"{float(lsd.get('secs_left') or 0):.0f}s left)")
            else:
                msg = f"Executed PAPER trade: {side} @ {price:.4f} for {market.get('slug')} (Amount: ${paper_amount:.2f})"
            order_sent = asyncio.Event()

            # Parallel dispatch: Never wait for Paper disk writes or Telegram alerts before firing Live!
            async def _bg_paper_post_actions():
                if live_on:
                    # Write the state file once the live order is out: json-dumping the state holds
                    # Python's GIL for milliseconds, which would slow the order's own Python steps.
                    try:
                        await asyncio.wait_for(order_sent.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pass
                await asyncio.to_thread(save_state)
                await send_telegram(f"🟢 *PAPER {'Long Shot' if is_ls else 'Trade'} Entered*\n• Side: `{side}`\n• Price: `{price:.4f}`\n• Stake: `${paper_amount:.2f}`\n• Market: `{market.get('slug')}`")

            if live_on:
                # Fire LIVE trade order and paper background tasks concurrently via asyncio.gather
                trade_dispatched = True
                # ensure_future (not create_task): gather() returns a future, and create_task()
                # raised TypeError on it after the orders had already started.
                _fut = asyncio.ensure_future(asyncio.gather(
                    _continuous_live_copy_fill(trade, market, token_ids, evaluated_price=price, ask_liq_usd=ask_liq_usd,
                                               order_sent=order_sent),
                    _bg_paper_post_actions()
                ))
                _BG_FUTURES.add(_fut)
                _fut.add_done_callback(_BG_FUTURES.discard)
                # Yield once: the live order is signed and handed to the sender right now,
                # before the bookkeeping and log lines below.
                await asyncio.sleep(0)

            _sync_active_trades_to_latest_data()
            log_message(msg)
            if not live_on:
                if settings.PRIVATE_KEY:
                    log_message("LIVE copy skipped: long shots are set to paper only")
                else:
                    log_message("LIVE copy skipped: no private key set in settings")
                if is_ls and open_reason == "longshot_engine":   # PATCH: longshot engine — in logs/longshots.csv
                    _le_log("ENTRY", trade, note="paper only")
                asyncio.create_task(_bg_paper_post_actions())

            if is_ls and window_start_ms is not None and longshot.one_at_a_time_on(ls_cfg):
                _ls_entered_windows.add(int(window_start_ms))
                if hasattr(ls_maker_mgr, "cancel_window"):
                    asyncio.create_task(ls_maker_mgr.cancel_window(int(window_start_ms), "longshot_entered"))

            return "entered"
        finally:
            if not trade_dispatched:
                for k in slot_keys:
                    _in_flight_markets.discard(k)


async def maybe_flip_position(decision: Dict[str, Any], poly_snapshot: Dict[str, Any], time_left_min: Optional[float]):
    if not settings.FLIP_ENABLED:
        return
    if decision.get("action") != "ENTER" or not state["active_trades"]:
        return

    new_side = decision["side"]
    new_prob = decision.get("prob", 0) or 0
    if new_prob < settings.FLIP_MIN_CONVICTION:
        return
    effective_min_left = min(settings.FLIP_MIN_MINUTES_LEFT, settings.CANDLE_WINDOW_MINUTES * 0.6)
    if time_left_min is not None and time_left_min < effective_min_left:
        return

    market = poly_snapshot["market"]
    prices = poly_snapshot["prices"]
    token_ids = poly_snapshot.get("token_ids", {})
    orderbook = poly_snapshot.get("orderbook", {})

    cur_trades = [t for t in state["active_trades"] if str(t.get("market_id")) == str(market.get("id"))
                  and not _is_longshot(t)]  # PATCH: longshot — long shots are never flipped
    if not cur_trades:
        return
    trade = cur_trades[0]
    if trade["side"] == new_side:
        return

    held_key = "up" if trade["side"] == "UP" else "down"
    ob = orderbook.get(held_key) or {}
    exit_price = ob.get("bestBid") or prices.get(held_key)
    if not exit_price or exit_price <= 0:
        log_message(f"FLIP aborted: no exit price for {trade['side']}")
        return

    now_iso = datetime.now().isoformat()

    # 1. Close live trade first if filled. If sell fails, ABORT flip to avoid self-hedging!
    if trade.get("live_status") == "FILLED" and trade.get("live_shares"):
        token_id = token_ids.get(held_key)
        sell_shares = float(trade["live_shares"])
        exp = int(max(time.time() + 300, trade.get("end_ts") or 0))
        result = await asyncio.to_thread(clob_trader.place_limit_sell, token_id, sell_shares, exit_price, exp)
        if not result.get("ok"):
            log_message(f"FLIP ABORTED: Live sell of {trade['side']} failed ({result.get('error')}) — holding position to prevent self-hedging")
            return None
        live_fill_px = float(result.get("fill_price") or exit_price)
        trade["live_close_price"] = live_fill_px
        trade["live_closed_time"] = now_iso
        trade["live_exit_order_id"] = result.get("order_id")
        trade["live_profit_loss"] = (sell_shares * live_fill_px) - float(trade.get("live_amount") or 0.0)
        log_message(f"FLIP: Live {trade['side']} sold @ {live_fill_px:.4f} (Live P/L ${trade['live_profit_loss']:.2f})")
    elif trade.get("live_status") in ("FILLING", "GTD_OPEN"):
        order_id = trade.get("live_order_id")
        token_id = token_ids.get(held_key)
        if order_id:
            log_message(f"FLIP: Cancelling resting GTD order {order_id}")
            await asyncio.to_thread(clob_trader.cancel_order, order_id)
            fills = await asyncio.to_thread(clob_trader.get_order_fills, order_id, token_id) if token_id else []
            if fills:
                filled_shares = sum(float(f["size"]) for f in fills)
                if filled_shares > 0:
                    log_message(f"FLIP: Resting GTD had {filled_shares:.2f} filled shares — selling before flip")
                    sell_res = await asyncio.to_thread(clob_trader.place_limit_sell, token_id, filled_shares, exit_price)
                    if not sell_res.get("ok"):
                        log_message(f"FLIP ABORTED: Live sell of partial fills failed ({sell_res.get('error')}) — holding to prevent self-hedging")
                        return None
                    live_fill_px = float(sell_res.get("fill_price") or exit_price)
                    trade["live_shares"] = filled_shares
                    trade["live_close_price"] = live_fill_px
                    trade["live_closed_time"] = now_iso
                    trade["live_exit_order_id"] = sell_res.get("order_id")
                    trade["live_status"] = "FILLED"
                    trade["live_profit_loss"] = (filled_shares * live_fill_px) - float(trade.get("live_amount") or 0.0)
                    log_message(f"FLIP: Sold partial fills @ {live_fill_px:.4f} (Live P/L ${trade['live_profit_loss']:.2f})")
        if trade.get("live_status") != "FILLED":
            trade["live_status"] = "CANCELLED_BY_FLIP"
            trade["live_closed_time"] = now_iso
            trade["live_profit_loss"] = 0.0

    # 2. Close paper trade only after live position exit is confirmed
    state["paper_balance"] += trade["paper_shares"] * exit_price
    trade["paper_close_price"] = exit_price
    trade["paper_closed_time"] = now_iso
    trade["paper_profit_loss"] = (trade["paper_shares"] * exit_price) - trade["paper_amount"]

    trade["status"] = "CLOSED"
    trade["exit_reason"] = "flip"
    trade["resolution"] = "flip_exit"
    trade["settlement_price_at_expiry"] = exit_price
    trade["open_price"] = trade.get("strike_price")
    trade["close_price"] = state.get("last_seen_price")

    state["trade_history"].append(_archive(trade))
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    save_state()
    _sync_active_trades_to_latest_data()
    log_message(f"FLIP: Closed {trade['side']} @ {exit_price:.2f} (Paper P/L ${trade['paper_profit_loss']:.2f}); opening {new_side}")
    return new_side

def mark_window_open(start_ms: int, window_ms: int, current_price: Optional[float],
                      spot_price: Optional[float], price_source: Optional[str],
                      price_is_fresh: bool = True, price_ts_ms: Optional[float] = None) -> Dict[str, Any]:
    opens = state["market_opens"]
    prev_ws = state.get("last_window_start")

    # On rollover, freeze the PRIOR window's close
    if prev_ws is not None and prev_ws != start_ms and prev_ws in opens:
        if opens[prev_ws].get("close") is None and state.get("last_seen_price"):
            opens[prev_ws]["close"] = state["last_seen_price"]
    if current_price:
        state["last_seen_price"] = current_price

    observed_prev = prev_ws is not None and abs((start_ms - window_ms) - prev_ws) < 2000
    if start_ms not in opens:
        opens[start_ms] = {"chainlink": None, "binance": None,
                           "close": None, "genuine": observed_prev}
        for k in list(opens.keys()):
            if k < start_ms - 4 * window_ms:
                del opens[k]

    win = opens[start_ms]
    since_start = time.time() * 1000 - start_ms
    # The strike is the first Chainlink price of the new window. Now that the loop runs right
    # at the open, the latest price can still be the one stamped just before it: that one is
    # only used if nothing newer has arrived STRIKE_WAIT_MS after the open.
    ts_ok = price_ts_ms is None or price_ts_ms >= start_ms or since_start >= STRIKE_WAIT_MS
    if (win["chainlink"] is None and current_price and price_is_fresh and ts_ok
            and 0 <= since_start < MARK_CAPTURE_WINDOW_MS):
        win["chainlink"] = current_price
        win["binance"] = spot_price
        win["genuine"] = True
        log_message(f"Window open marked @ eventStartTime: Chainlink {current_price:.2f} "
                    f"({price_source}) / Binance {spot_price if spot_price else '-'}")
    elif win["chainlink"] is None and since_start >= MARK_CAPTURE_WINDOW_MS:
        if not win.get("skip_logged"):
            win["skip_logged"] = True
            log_message(f"Window open not captured (started late mid-window): No strike @ eventStartTime — skipping trading this window")
    state["last_window_start"] = start_ms
    return win

# ═══ PATCH: official results ═══════════════════════════════════════════════════════════════
# Settings -> "Results & Redeem" (config.json settlement.redeem_after_resolution and
# settlement.confirm_official). Both off (the default): nothing here runs and the bot books and
# redeems exactly as before.
#
# What the bot does on its own: update_trades books a trade from Polymarket's PRICE at the close
# (any outcome price over 0.9: "polymarket_settled") or, when no price is that decisive for
# settlement.authoritative_settle_wait_s, from its own BTC price against its own strike
# ("close_vs_open"). And _redeem_win tries to redeem a win once, at that moment, which is before
# Polymarket has resolved the market on-chain: the relayer refuses, and nothing tries again.
# Neither is Polymarket's own result. That is the market CLOSED with its outcome prices at exactly
# 1 and 0, a few minutes after the window (it resolves on Chainlink's 60-second average, not on a
# single price). Reading it takes more than the bot's own market read: Polymarket's market list
# (GET /markets?slug=) gives open markets unless closed ones are asked for, so a market that has
# closed is no longer in that answer. _or_read_market asks for it with closed=true then, by the
# market's own address (/markets/slug/<slug>) if that gives nothing, and last from Polymarket's
# order-book service by its conditionId, where a resolved market's winning token is marked.
#
# With either setting on, every trade booked at the end of its window is followed afterwards
# (_or_pass, every OR_TICK_S, in its own background task) until that result can be read:
#   * redeem_after_resolution: _redeem_win makes no relayer call at booking. Once the result is
#     read, the winning side's live shares are redeemed with ONE call per market (it pays out every
#     share of that market), and a refusal is tried again after OR_REDEEM_WAITS_S. If the result
#     can't be read, the trades the bot booked as wins are still tried at OR_BLIND_AT_S after the
#     close. The side redeemed is Polymarket's winner, also when the bot had booked it as a loss.
#   * confirm_official: a trade whose booked result differs from Polymarket's is corrected — its
#     result, paper and live P/L, and the paper balance — and logged as RESULT CORRECTED. One that
#     matches is marked confirmed.
# Kept on a trade: trade["official"] = {"booked", "winner", "result", "done", ...}, and the bot's
# own trade["redeem_status"] with three more values: "SENT" (redeemed before the result was read),
# "NOT_WON" (booked as a win, lost on Polymarket) and "GAVE_UP".
OR_TICK_S = 5.0                 # how often the followed trades are looked at
OR_FIRST_READ_S = 30.0          # Polymarket's market data isn't read sooner than this after the window ended...
OR_POLL_S = 20.0                # ...then this often per market...
OR_POLL_SLOW_S = 60.0           # ...and this often once the window ended more than 10 minutes ago
OR_WATCH_MAX_S = 7200.0         # a market is followed this long after its window ended
OR_REDEEM_WAITS_S = (20, 40, 60, 120, 300, 600, 1200, 1800)   # after each refusal, once Polymarket has resolved
OR_BLIND_AT_S = (240, 600, 1500, 3000, 5400)                  # tries without the result read: this long after the close
OR_PENDING = ("PENDING_ONCHAIN_RESOLUTION", "PENDING_RELAYER_KEY")
_or_markets: Dict[str, Dict[str, Any]] = {}   # per followed market: last read, redeem tries (in memory)
_or_last_error_ts = 0.0
_or_bg: set = set()             # Telegram messages on their way (kept referenced until sent)


def _or_redeem_on() -> bool:
    return bool(getattr(settings, "REDEEM_AFTER_RESOLUTION", False))


def _or_confirm_on() -> bool:
    return bool(getattr(settings, "CONFIRM_OFFICIAL", False))


def _or_status_line() -> str:
    """The line logged at startup and whenever the settings are saved."""
    return ("RESULTS: redeem after Polymarket's resolution " + ("on" if _or_redeem_on() else "off")
            + " | results checked against Polymarket's " + ("on" if _or_confirm_on() else "off"))


def _or_money(v) -> str:
    return "—" if v is None else f"{'-' if float(v) < 0 else '+'}${abs(float(v)):.2f}"


def _or_winner(market: Optional[Dict[str, Any]]) -> Optional[str]:
    """Polymarket's own result from its market data: "UP" or "DOWN" once the market is closed and
    its outcome prices are exactly 1 and 0. None before that: the prices are then the market's last
    prices (a book can't trade at 1 or 0), however close to 1 they are."""
    if not isinstance(market, dict) or market.get("closed") is not True:
        return None
    try:
        outcomes = market.get("outcomes") or []
        prices = market.get("outcomePrices") or []
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if isinstance(prices, str):
            prices = json.loads(prices)
        prices = [float(p) for p in prices]
    except Exception:
        return None
    if len(outcomes) != 2 or len(prices) != 2 or sorted(prices) != [0.0, 1.0]:
        return None
    label = str(outcomes[prices.index(1.0)]).strip().lower()
    if label == settings.POLYMARKET_UP_LABEL.lower():
        return "UP"
    if label == settings.POLYMARKET_DOWN_LABEL.lower():
        return "DOWN"
    return None


async def _or_get(base: str, path: str, params: Optional[Dict[str, str]] = None) -> Any:
    """One read of Polymarket's data, made the way bot/data.py makes its own (same client, same
    proxy rule). The parsed answer, or None when the answer isn't a 200."""
    url = f"{base}{path}"
    client = data.get_client(data.get_proxy_url_for(url))
    res = await client.get(url, params=params)
    return res.json() if res.status_code == 200 else None


async def _or_read_market(slug: str, cid: Optional[str] = None) -> tuple:
    """Polymarket's data for a followed market, and where it was read from: (market, via). In this
    order, stopping at the first answer that is this market:
      "list"         the bot's own read, GET /markets?slug=. That list gives open markets unless
                     closed ones are asked for, so once the market has closed it is no longer there
      "closed=true"  the same list, asked for closed markets
      "address"      the market's own address, GET /markets/slug/<slug>
      "clob"         Polymarket's order-book service, GET /markets/<conditionId>: once resolved, one
                     of its two tokens is marked "winner". Given back as the fields _or_winner reads
    (None, None) when none of them has it."""
    try:
        market = await data.fetch_market_by_slug(slug)
    except Exception:
        market = None
    if isinstance(market, dict):
        return market, "list"                  # still open (or closed, where the list keeps closed markets)
    from urllib.parse import quote
    for via, path, params in (("closed=true", "/markets", {"slug": slug, "closed": "true"}),
                              ("address", "/markets/slug/" + quote(slug, safe=""), None)):
        try:
            got = await _or_get(settings.GAMMA_BASE_URL, path, params)
        except Exception:
            continue
        if isinstance(got, list):
            got = next((m for m in got if isinstance(m, dict) and m.get("slug") == slug), None)
        if isinstance(got, dict) and got.get("slug") == slug:
            return got, via
    if cid:
        try:
            got = await _or_get(settings.CLOB_BASE_URL, "/markets/" + quote(str(cid), safe=""))
        except Exception:
            got = None
        mine = isinstance(got, dict) and str(got.get("condition_id")).lower() == str(cid).lower() and got.get("closed") is True
        toks = got.get("tokens") if mine else None
        if (isinstance(toks, list) and len(toks) == 2 and all(isinstance(t, dict) for t in toks)
                and sorted(t.get("winner") is True for t in toks) == [False, True]):
            return {"slug": slug, "closed": True, "conditionId": cid, "outcomes": [str(t.get("outcome")) for t in toks],
                    "outcomePrices": [1 if t.get("winner") is True else 0 for t in toks]}, "clob"
    return None, None


def _or_note(trade: Dict[str, Any], market: Optional[Dict[str, Any]], up_index: int, down_index: int, winning_index: int) -> None:
    """update_trades, as a trade is booked at the end of its window: what the later check against
    Polymarket's own result needs. Nothing when both settings are off."""
    if not (_or_redeem_on() or _or_confirm_on()):
        return
    m = market or {}
    trade["official"] = {
        "booked": "UP" if winning_index == up_index else "DOWN",   # the side the bot booked as the winner
        "winner": None,                                            # Polymarket's, once read
        "result": None,                                            # "confirmed" / "corrected" (confirm_official)
        "done": False,
        "condition_id": m.get("conditionId") or m.get("condition_id"),
        "neg_risk": bool(m.get("negRisk") or m.get("neg_risk") or False),
        "up_index": up_index, "down_index": down_index,
        "noted_ts": time.time(),                                   # when it was booked (the clock, should a trade carry no end_ts)
    }


def _or_live_shares(trade: Dict[str, Any]) -> float:
    """The live shares a trade holds to the end of its window (0 without a live fill)."""
    if trade.get("live_status") != "FILLED":
        return 0.0
    try:
        return max(0.0, float(trade.get("live_shares") or 0.0))
    except (TypeError, ValueError):
        return 0.0


def _or_owes_redeem(trade: Dict[str, Any], winner: Optional[str]) -> bool:
    """Is a redeem still to come for this trade? With Polymarket's winner known: its side won and
    it isn't redeemed (or given up on). Not known yet: the bot booked it as a win and it waits."""
    if _or_live_shares(trade) <= 0:
        return False
    status = trade.get("redeem_status")
    if winner is None:
        return status in OR_PENDING or status == "SENT"
    return trade.get("side") == winner and status not in ("CONFIRMED", "GAVE_UP")


async def _or_check(trade: Dict[str, Any], winner: str) -> bool:
    """confirm_official: Polymarket's own result against the one the bot booked for this trade. The
    same: marked confirmed. Different: corrected — the result, the paper and live P/L, the paper
    balance (the live balance is the wallet's cash and needs nothing). True when it was corrected."""
    o = trade["official"]
    if o.get("result"):
        return False
    booked = o.get("booked")
    if winner == booked:
        o["result"] = "confirmed"
        return False
    side = trade.get("side")
    won = side == winner
    # everything is worked out first, then written: a trade is either corrected in full or left as it was
    old_paper, old_live = trade.get("paper_profit_loss"), trade.get("live_profit_loss")
    paper_shares = float(trade.get("paper_shares") or 0.0)
    paper_amt = float(trade.get("paper_amount") or 0.0)
    new_paper = (paper_shares - paper_amt) if won else -paper_amt
    shares = _or_live_shares(trade)
    live_amt = float(trade.get("live_amount") or 0.0)
    new_live = ((shares - live_amt) if won else -live_amt) if shares > 0 else old_live
    paper_balance = float(state["paper_balance"]) + (paper_shares if won else -paper_shares)   # the payout goes in, or comes back out

    state["paper_balance"] = paper_balance
    trade["paper_profit_loss"] = new_paper
    trade["live_profit_loss"] = new_live
    trade["resolution_booked"] = trade.get("resolution")
    trade["resolution"] = "polymarket_official"
    trade["winning_outcome"] = settings.POLYMARKET_UP_LABEL if winner == "UP" else settings.POLYMARKET_DOWN_LABEL
    o["result"] = "corrected"
    log_message(f"RESULT CORRECTED for {trade.get('market_slug')}: the bot booked {booked} WIN [{trade['resolution_booked']}], "
                f"Polymarket resolved {winner}. {side} is a {'WIN' if won else 'LOSS'}: live P/L {_or_money(old_live)} -> "
                f"{_or_money(new_live)}, paper P/L {_or_money(old_paper)} -> {_or_money(new_paper)}"
                + (f" — its {shares:.2f} shares are to claim on polymarket.com (\"Redeem winners after Polymarket's resolution\" is off)"
                   if won and shares > 0 and not _or_redeem_on() else ""))
    _row = globals().get("_le_log_result")    # the engine long shots' logs/longshots.csv, when that patch is in: the corrected RESULT row
    if _row is not None:
        try:
            _row(trade)
        except Exception:
            pass
    try:                                      # in the background: a slow Telegram must not hold up the redeem
        fut = asyncio.ensure_future(send_telegram(
            f"🔁 *RESULT CORRECTED: {side}*\n• Polymarket resolved: `{winner}` (the bot had booked `{booked}`)"
            f"\n• Live P/L: `{_or_money(old_live)}` → `{_or_money(new_live)}`\n• Market: `{trade.get('market_slug')}`"))
        _or_bg.add(fut)
        fut.add_done_callback(_or_bg.discard)
    except Exception:
        pass
    return True


async def _or_redeem(slug: str, trades: List[Dict[str, Any]], winner: Optional[str], mk: Dict[str, Any],
                     now: float, end_ts: float) -> bool:
    """redeem_after_resolution, one market: redeem the winning side's live shares once Polymarket
    has resolved (one relayer call for the market), again after each refusal (OR_REDEEM_WAITS_S).
    While the result can't be read, the trades booked as wins are tried at OR_BLIND_AT_S after the
    close. True when something changed."""
    live = [t for t in trades if _or_live_shares(t) > 0]
    if not live:
        return False
    changed = False
    if winner is not None:
        for t in live:
            if t.get("side") != winner and (t.get("redeem_status") in OR_PENDING or t.get("redeem_status") in ("SENT", "FAILED")):
                t["redeem_status"] = "NOT_WON"
                changed = True
                log_message(f"REDEEM dropped for {slug}: {t.get('side')} did not win (Polymarket resolved {winner})")
        won = [t for t in live if t.get("side") == winner]
        for t in won:
            # sent before the result was read, or this market's redeem already went through (it paid every share of it)
            if t.get("redeem_status") == "SENT" or (mk.get("tx") is not None and t.get("redeem_status") != "CONFIRMED"):
                t["redeem_status"] = "CONFIRMED"
                if mk.get("tx") is not None:
                    t["redeem"] = mk["tx"]
                changed = True
        todo = [t for t in won if t.get("redeem_status") not in ("CONFIRMED", "GAVE_UP")]
        if not todo or now < mk["next_ts"]:
            return changed
    else:
        todo = [t for t in live if t["official"].get("booked") == t.get("side") and t.get("redeem_status") in OR_PENDING]
        if not todo or mk["blind"] >= len(OR_BLIND_AT_S) or now < end_ts + OR_BLIND_AT_S[mk["blind"]]:
            return changed
        mk["blind"] += 1

    cid = next((t["official"].get("condition_id") for t in todo if t["official"].get("condition_id")), None)
    if not cid:
        if not mk.get("no_cid"):
            mk["no_cid"] = True
            log_message(f"REDEEM waits for {slug}: Polymarket's market data hasn't given its conditionId yet")
        return changed
    side = winner or todo[0]["official"].get("booked")
    neg = bool(todo[0]["official"].get("neg_risk"))
    try:
        idx = int(todo[0]["official"].get("up_index" if side == "UP" else "down_index"))
    except (TypeError, ValueError):
        idx = 0 if side == "UP" else 1
    shares = sum(_or_live_shares(t) for t in todo)
    amounts = [0.0, 0.0]
    amounts[idx if idx in (0, 1) else (0 if side == "UP" else 1)] = shares
    try:
        res = await asyncio.to_thread(clob_trader.redeem, cid, amounts, neg)
    except Exception as e:
        res = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    after = f"{max(0.0, now - end_ts):.0f}s after the close"
    if res.get("ok"):
        if not neg:
            mk["tx"] = res
        for t in todo:
            t["redeem"] = res
            t["redeem_status"] = "CONFIRMED" if winner is not None else "SENT"
        late = [t for t in todo if t["official"].get("booked") != t.get("side")]
        if winner is not None:
            log_message(f"REDEEM ok for {slug}: {shares:.2f} live shares (tx {res.get('tx')}), {after}"
                        + ((" — booked as a loss by the bot" if len(late) == len(todo) else f" — {len(late)} of its {len(todo)} trades booked as a loss by the bot")
                           + f", Polymarket resolved {winner}" if late else ""))
        else:
            log_message(f"REDEEM sent for {slug}: {shares:.2f} live shares (tx {res.get('tx')}), {after} — Polymarket's result "
                        f"has not been read yet, so the payout isn't confirmed")
        return True
    err = str(res.get("error") or "redeem_failed")
    status = "PENDING_RELAYER_KEY" if ("401" in err or "Unauthorized" in err) else "PENDING_ONCHAIN_RESOLUTION"
    for t in todo:
        t["redeem"] = res
        t["redeem_status"] = status
    if winner is None:
        log_message(f"REDEEM not yet for {slug} ({after}, Polymarket's result not read yet): {err}")
        return True
    mk["tries"] += 1
    if mk["tries"] > len(OR_REDEEM_WAITS_S):
        for t in todo:
            t["redeem_status"] = "GAVE_UP"
        log_message(f"REDEEM gave up for {slug} after {mk['tries']} tries ({err}): {shares:.2f} shares are still to claim on polymarket.com")
    else:
        wait = OR_REDEEM_WAITS_S[mk["tries"] - 1]
        mk["next_ts"] = now + wait
        log_message(f"REDEEM not yet for {slug} ({after}): {err} — next try in {wait}s ({mk['tries']} of {len(OR_REDEEM_WAITS_S)})"
                    + (" — add the Relayer API Key on the Settings page, or claim on polymarket.com" if status == "PENDING_RELAYER_KEY" else ""))
    return True


async def _or_market(slug: str, trades: List[Dict[str, Any]], now: float) -> bool:
    """One followed market: read Polymarket's result if it isn't known yet, check the trades against
    it (confirm_official), redeem (redeem_after_resolution), and stop following what is finished.
    True when something changed."""
    mk = _or_markets.setdefault(slug, {"read_ts": 0.0, "tries": 0, "next_ts": 0.0, "blind": 0, "tx": None})
    end_ts = max(float(t.get("end_ts") or t["official"].get("noted_ts") or 0.0) for t in trades) or now
    age = now - end_ts
    changed = False
    winner = next((t["official"]["winner"] for t in trades if t["official"].get("winner")), None)
    via = None

    if winner is None and age >= OR_FIRST_READ_S and now - mk["read_ts"] >= (OR_POLL_S if age < 600 else OR_POLL_SLOW_S):
        mk["read_ts"] = now
        known = next((t["official"].get("condition_id") for t in trades if t["official"].get("condition_id")), None)
        try:
            market, via = await _or_read_market(slug, known)
        except Exception:
            market = None
        if isinstance(market, dict):
            cid = market.get("conditionId") or market.get("condition_id")
            for t in trades:
                if cid and not t["official"].get("condition_id"):
                    t["official"]["condition_id"] = cid
                    t["official"]["neg_risk"] = bool(market.get("negRisk") or market.get("neg_risk") or False)
                    changed = True
            winner = _or_winner(market)

    fresh = [t for t in trades if winner is not None and t["official"].get("winner") is None]
    if fresh:
        changed = True
        fixed = 0
        for t in fresh:
            t["official"]["winner"] = winner
            t["official"]["read_s"] = int(max(0.0, age))
            t["official"]["read_via"] = via
            if _or_confirm_on():
                try:
                    if await _or_check(t, winner):
                        fixed += 1
                except Exception as e:        # this trade is left as booked; the others and the redeem go on
                    t["official"]["result"] = "error"
                    fixed += 1
                    log_message(f"RESULT check failed for {slug} ({t.get('side')}): {type(e).__name__}: {e} — it stays as booked")
        differ = [t for t in fresh if t["official"].get("booked") != winner]
        if _or_confirm_on():
            if not fixed:
                log_message(f"RESULT confirmed for {slug}: Polymarket resolved {winner}, as booked "
                            f"({len(fresh)} trade{'s' if len(fresh) != 1 else ''}, read {age:.0f}s after the close)")
        elif differ:
            log_message(f"RESULT differs for {slug}: the bot booked {differ[0]['official'].get('booked')} WIN, Polymarket resolved "
                        f"{winner} (the list is not corrected: \"Check results against Polymarket's\" is off)")

    if _or_redeem_on() and settings.PRIVATE_KEY:
        changed = await _or_redeem(slug, trades, winner, mk, now, end_ts) or changed

    over = age > OR_WATCH_MAX_S
    redeeming = _or_redeem_on() and bool(settings.PRIVATE_KEY)
    for t in trades:
        o = t["official"]
        owes = redeeming and _or_owes_redeem(t, o.get("winner"))
        if over:
            if owes and t.get("redeem_status") != "SENT":      # a SENT one went through; only its result was never read
                t["redeem_status"] = "GAVE_UP"
                log_message(f"REDEEM gave up for {slug}: not redeemed {OR_WATCH_MAX_S / 60:.0f} min after the close — "
                            f"{_or_live_shares(t):.2f} {t.get('side')} shares may still be to claim on polymarket.com")
            if o.get("winner") is None and not mk.get("unread"):
                mk["unread"] = True
                log_message(f"RESULT not read for {slug}: Polymarket's market data showed no resolution in "
                            f"{OR_WATCH_MAX_S / 60:.0f} min — the booked result stands unchecked")
            o["done"] = True
            changed = True
        elif o.get("winner") is not None and not owes:
            o["done"] = True
            changed = True
    return changed


async def _or_pass(now: Optional[float] = None) -> None:
    """One look at every trade booked in the last OR_WATCH_MAX_S that is still being followed."""
    if not (_or_redeem_on() or _or_confirm_on()):
        return
    now = time.time() if now is None else now
    by_market: Dict[str, List[Dict[str, Any]]] = {}
    for t in state["trade_history"][-400:]:
        o = t.get("official")
        if isinstance(o, dict) and not o.get("done") and t.get("status") == "CLOSED" and t.get("market_slug"):
            by_market.setdefault(str(t["market_slug"]), []).append(t)
    for slug in [s for s in _or_markets if s not in by_market]:
        _or_markets.pop(slug, None)
    changed = False
    for slug, trades in by_market.items():
        try:
            changed = await _or_market(slug, trades, now) or changed
        except Exception as e:
            global _or_last_error_ts
            if now - _or_last_error_ts > 300:
                _or_last_error_ts = now
                log_message(f"RESULTS check error for {slug}: {type(e).__name__}: {e}")
    if changed:
        await asyncio.to_thread(save_state)
        _sync_active_trades_to_latest_data()
        await broadcast_state()


async def _or_watch_forever():
    """The background task (started in lifespan): _or_pass every OR_TICK_S."""
    global _or_last_error_ts
    while True:
        try:
            await _or_pass()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if time.time() - _or_last_error_ts > 300:
                _or_last_error_ts = time.time()
                log_message(f"RESULTS check error: {type(e).__name__}: {e}")
        await asyncio.sleep(OR_TICK_S)


async def _redeem_win(trade: Dict[str, Any], market: Optional[Dict[str, Any]],
                      up_index: int, down_index: int, winning_index: int):
    if _or_redeem_on():   # PATCH: official results — no relayer call at booking: _or_pass redeems once Polymarket has resolved
        trade["redeem_status"] = "PENDING_ONCHAIN_RESOLUTION"
        log_message(f"REDEEM for {trade['market_slug']} waits for Polymarket's resolution")
        return
    condition_id = (market or {}).get("conditionId") or (market or {}).get("condition_id")
    if not condition_id:
        trade["redeem"] = {"ok": False, "error": "missing_condition_id"}
        log_message(f"REDEEM skipped for {trade['market_slug']}: no conditionId on the market")
        return

    amounts = [0.0, 0.0]
    idx = up_index if winning_index == up_index else down_index
    shares = float(trade.get("live_shares") or trade.get("shares") or 0.0)
    if shares <= 0:
        return
    if 0 <= idx < len(amounts):
        amounts[idx] = shares

    neg_risk = bool((market or {}).get("negRisk") or (market or {}).get("neg_risk") or False)
    trade["condition_id"], trade["redeem_amounts"], trade["neg_risk"] = condition_id, amounts, neg_risk   # for retry_pending_redeems

    # Defer on-chain redemption if market was settled via local strike fallback and on-chain outcome is still pending
    if trade.get("resolution") == "close_vs_open":
        trade["redeem_status"] = "PENDING_ONCHAIN_RESOLUTION"
        log_message(f"REDEEM deferred for {trade['market_slug']}: Position won via local fallback; waiting for on-chain Polymarket resolution before redeeming.")
        return
    try:
        res = await asyncio.to_thread(clob_trader.redeem, condition_id, amounts, neg_risk)
    except Exception as e:
        res = {"ok": False, "error": f"{type(e).__name__}: {e}"}

    trade["redeem"] = res
    if res.get("ok"):
        trade["redeem_status"] = "CONFIRMED"
        log_message(f"REDEEM ok for {trade['market_slug']}: {amounts[idx]:.2f} live shares (tx {res.get('tx')})")
    else:
        err = str(res.get("error") or "")
        if "401" in err or "Unauthorized" in err:
            trade["redeem_status"] = "PENDING_RELAYER_KEY"
            log_message(f"REDEEM PENDING for {trade['market_slug']}: 401 Unauthorized — Polymarket Relayer API Key is required for gasless redemption. Add your API key in config.json -> relayer -> api_key, or claim on polymarket.com.")
        else:
            trade["redeem_status"] = "FAILED"
            log_message(f"REDEEM FAILED for {trade['market_slug']}: {err}")

def _archive(trade: Dict[str, Any]) -> Dict[str, Any]:
    for k in ("_market", "_market_closed", "order_response"):
        trade.pop(k, None)
    return trade

async def update_trades(current_prices: Dict[str, Any]):
    # Snapshot pattern to prevent mid-pass mutations from dropping concurrent trades
    active_snapshot = list(state["active_trades"])
    remaining_active = []
    trades_changed = False
    now_ts = time.time()

    cur_price = current_prices.get("chainlink")
    SETTLEMENT_GRACE_SECONDS = 300

    for trade in active_snapshot:
        if cur_price:
            trade["last_price"] = cur_price

        end_ts = trade.get("end_ts", 0)
        if not end_ts:
            try:
                end_ts = datetime.fromisoformat(trade.get("paper_entry_time") or trade.get("entry_time")).timestamp() + settings.CANDLE_WINDOW_MINUTES * 60
            except Exception:
                end_ts = now_ts
        expired = now_ts >= end_ts
        trade["awaiting_resolution"] = expired and trade.get("status") == "OPEN"

        # Hard cancel on expiry: Never let a resting order bleed into the next window!
        if expired and trade.get("live_order_id") and trade.get("live_status") in ("FILLING", "GTD_OPEN", "GTC_OPEN"):
            try:
                await asyncio.to_thread(clob_trader.cancel_order, trade["live_order_id"])
            except Exception:
                pass

        if expired and trade.get("close_price") is None:
            frozen_close = cur_price or trade.get("last_price")
            if frozen_close:
                trade["close_price"] = frozen_close
        # PATCH: longshot — the bot's own 60-second averages at the open and the close,
        # taken 3 s after the close while the recorded prices are still in memory
        if expired and now_ts >= end_ts + 3.0 and "sampled_close_avg" not in trade:
            trade["sampled_close_avg"] = longshot.sampled_twap(end_ts)
            _ws_ms = trade.get("window_start_ms")
            trade["sampled_open_avg"] = longshot.sampled_twap(_ws_ms / 1000.0) if _ws_ms else None
        # End-of-window wallet fill reconciliation:
        # Before evaluating settlement, compare wallet's actual fills with bot records.
        # This catches any ghost fills or orders that filled without local confirmation.
        # PATCH: longshot — a long shot that never sent an order (paper only, $0 stake...) has
        # nothing to reconcile; a fill-and-kill long shot can have several orders.
        _ls_no_order = _is_longshot(trade) and not (trade.get("live_order_ids") or trade.get("live_order_id"))
        # FIX: wallet reconciliation — a live copy that stopped on its first try, before any order
        # went out (no funds, price above the cap), has nothing to reconcile either
        _never_sent = (trade.get("live_status") in ("INSUFFICIENT_FUNDS", "PRICE_ABOVE_CAP")
                       and not (trade.get("live_order_ids") or trade.get("live_order_id"))
                       and int(trade.get("live_attempts") or 0) <= 1 and trade.get("live_order_usd") is None)
        if (expired and trade.get("live_status") != "FILLED" and clob_trader.is_ready() and not trade.get("_reconciled")
                and not _ls_no_order and not _never_sent):
            trade["_reconciled"] = True
            token_id = trade.get("token_id")
            order_id = trade.get("live_order_id")
            reconciled_fills = []
            for _oid in (trade.get("live_order_ids") or ([order_id] if order_id else [])):
                reconciled_fills += await asyncio.to_thread(clob_trader.get_order_fills, _oid, token_id)
            # PATCH: longshot — the token-wide lookup below can't tell two trades on the same token
            # apart, so it is skipped when another trade in this window also ordered that token.
            shared_token = any(o is not trade and o.get("token_id") == token_id
                               and str(o.get("market_id")) == str(trade.get("market_id"))
                               and (o.get("live_order_id") or o.get("live_order_ids"))
                               for o in active_snapshot)
            if not reconciled_fills and token_id and not shared_token:
                try:
                    entry_str = trade.get("live_entry_time") or trade.get("paper_entry_time") or trade.get("entry_time")
                    start_ts = datetime.fromisoformat(entry_str).timestamp()
                except Exception:
                    start_ts = end_ts - (settings.CANDLE_WINDOW_MINUTES * 60)
                # FIX: wallet reconciliation — only this wallet's own buys of this token count, and never
                # the orders of the bot's other trades or the maker's bids that are still on the book
                _other_orders = [i for o in active_snapshot if o is not trade
                                 for i in (list(o.get("live_order_ids") or []) + [o.get("live_order_id")]) if i]
                _other_orders += [i for i in (state.get("ls_maker_open") or []) if i]
                reconciled_fills = await asyncio.to_thread(clob_trader.get_token_fills_in_window, token_id, start_ts, end_ts,
                                                           _other_orders)

            if reconciled_fills:
                total_shares = sum(float(f["size"]) for f in reconciled_fills)
                total_usd = sum(float(f["size"]) * float(f["price"]) for f in reconciled_fills)
                avg_price = total_usd / total_shares if total_shares > 0 else float(trade.get("paper_entry_price") or 0.5)

                log_message(f"WALLET RECONCILIATION: Discovered {total_shares:.2f} filled shares in wallet for {trade['market_slug']} ({trade['side']}) — reconciling trade to FILLED!")
                trade["live_status"] = "FILLED"
                trade["live_shares"] = total_shares
                trade["live_entry_price"] = avg_price
                trade["live_amount"] = total_usd
                trade["live_entry_time"] = trade.get("live_entry_time") or datetime.now().isoformat()

                # Deduct spent funds in RAM
                if total_usd > 0:
                    new_bal = max(0.0, (state.get("live_balance") or 0.0) - total_usd)
                    state["live_balance"] = new_bal
                    risk_type = (settings.RISK_TYPE or "percent").lower()
                    if risk_type == "fixed":
                        new_stake = float(settings.RISK_VALUE)
                    else:
                        new_stake = (float(settings.RISK_VALUE) / 100.0) * new_bal
                    state["precomputed_live_stake"] = round(max(1.0, min(new_stake, new_bal)) if new_bal >= 1.0 else 0.0, 2)

                save_state()
                _sync_active_trades_to_latest_data()
                await broadcast_state()
                trades_changed = True


        market = trade.get("_market")
        poll_every = 3.0 if expired else 30.0
        if expired and now_ts - end_ts > 120:
            poll_every = 10.0  # PATCH: longshot — official mode can wait minutes; poll Gamma less often
        if trade.get("last_api_check", 0) < now_ts - poll_every:
            try:
                fetched = await data.fetch_market_by_slug(trade["market_slug"])
            except Exception:
                fetched = None
            trade["last_api_check"] = now_ts
            if fetched is not None:
                market = fetched
                trade["_market"] = fetched
                trade["_market_closed"] = bool(fetched.get("closed"))
        market_closed = trade.get("_market_closed", False)

        if not expired and not market_closed:
            remaining_active.append(trade)
            continue

        outcomes = []
        outcome_prices = []
        if market:
            outcomes = market.get("outcomes", [])
            if isinstance(outcomes, str): outcomes = json.loads(outcomes)
            outcome_prices = market.get("outcomePrices", [])
            if isinstance(outcome_prices, str): outcome_prices = json.loads(outcome_prices)
        if not outcomes:
            outcomes = [settings.POLYMARKET_UP_LABEL, settings.POLYMARKET_DOWN_LABEL]

        up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), 0)
        down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), 1)

        winning_index = -1
        resolution = None
        for i, p in enumerate(outcome_prices):
            try:
                if float(p) > 0.9:
                    winning_index = i
                    resolution = "polymarket_settled"
                    break
            except Exception:
                pass

        strike = trade.get("strike_price")
        settlement_price = (trade.get("close_price") or trade.get("settlement_price_at_expiry")
                            or trade.get("last_price") or cur_price)
        if winning_index == -1 and (expired or market_closed):
            if trade.get("expired_at") is None:
                trade["expired_at"] = now_ts
            waited = now_ts - trade["expired_at"]

            _pt = _ps_settle_call(trade)             # PATCH: price stream — Polymarket's own two averages, when both are there
            if not _pt and waited < settings.AUTHORITATIVE_SETTLE_WAIT_S:
                remaining_active.append(trade)
                continue

            if _pt:
                winning_index = up_index if _pt[0] == "UP" else down_index
                resolution = "close_vs_open"
                strike, settlement_price = _pt[2], _pt[1]
                trade["settlement_price_at_expiry"] = trade["close_price"] = settlement_price
                trade["settle_wait_s"] = round(waited, 1)
                trade["settle_feed"] = "polymarket_twap"
                log_message(f"SETTLEMENT for {trade['market_slug']}: "
                            f"resolved locally on Polymarket's 60-second averages (close {settlement_price:.2f} vs open {strike:.2f} => {_pt[0]})")
            elif strike and settlement_price:
                trade["settlement_price_at_expiry"] = settlement_price
                winning_index = up_index if settlement_price > strike else down_index
                resolution = "close_vs_open"
                trade["settle_wait_s"] = round(waited, 1)
                log_message(f"SETTLEMENT FALLBACK for {trade['market_slug']}: Polymarket resolution unavailable after {waited:.0f}s — resolved locally via {resolution} ({settlement_price:.2f} vs strike {strike:.2f})")

        if winning_index == -1:
            first_seen = trade.get("unresolved_since")
            if first_seen is None:
                trade["unresolved_since"] = now_ts
                remaining_active.append(trade)
                continue
            if now_ts - first_seen < SETTLEMENT_GRACE_SECONDS:
                remaining_active.append(trade)
                continue
            trade["status"] = "VOID"
            trade["exit_reason"] = "void"
            now_iso = datetime.now().isoformat()
            trade["paper_closed_time"] = now_iso
            trade["live_closed_time"] = now_iso if trade.get("live_status") == "FILLED" else None
            trade["paper_profit_loss"] = 0.0
            trade["live_profit_loss"] = 0.0 if trade.get("live_status") == "FILLED" else None
            state["paper_balance"] += float(trade.get("paper_amount") or 0.0)
            state["trade_history"].append(_archive(trade))
            trades_changed = True
            log_message(f"VOID: Trade for {trade['market_slug']} unresolved past grace; stake refunded.")
            _le_log_result(trade)   # PATCH: longshot engine
            continue

        won = ((trade["side"] == "UP" and winning_index == up_index) or
               (trade["side"] == "DOWN" and winning_index == down_index))

        open_px = strike
        close_px = trade.get("close_price") or settlement_price
        trade["open_price"] = open_px
        trade["close_price"] = close_px
        trade["paper_close_price"] = close_px
        trade["live_close_price"] = close_px if trade.get("live_status") == "FILLED" else None
        trade["resolution"] = resolution or "unknown"
        if open_px and close_px:
            move_side = "UP" if close_px > open_px else "DOWN"
            dir_txt = f"open {open_px:.2f} -> close {close_px:.2f} ({move_side} by {abs(close_px - open_px):.2f})"
        else:
            dir_txt = f"open {open_px} -> close {close_px}"

        now_iso = datetime.now().isoformat()
        trade["paper_closed_time"] = now_iso
        trade["live_closed_time"] = now_iso if trade.get("live_status") in ("FILLED", "FILLING") else None
        if trade.get("live_status") == "FILLING":
            trade["live_status"] = "TIMEOUT"

        if won:
            # Paper payout
            paper_shares = float(trade.get("paper_shares") or 0.0)
            paper_amt = float(trade.get("paper_amount") or 0.0)
            paper_payout = paper_shares * 1.0
            state["paper_balance"] += paper_payout
            trade["paper_profit_loss"] = paper_payout - paper_amt

            # Live payout
            if trade.get("live_status") == "FILLED" and trade.get("live_shares"):
                live_shares = float(trade["live_shares"])
                live_amt = float(trade.get("live_amount") or 0.0)
                live_payout = live_shares * 1.0
                trade["live_profit_loss"] = live_payout - live_amt
                log_message(f"WIN: {trade['side']} on {trade['market_slug']}: {dir_txt} "
                            f"[{trade['resolution']}]. Live Profit: ${trade['live_profit_loss']:.2f} (Paper Profit: ${trade['paper_profit_loss']:.2f})")
                await send_telegram(f"🏆 *WIN: {trade['side']}*\n• Live Profit: `+${trade['live_profit_loss']:.2f}`\n• Details: {dir_txt}\n• Market: `{trade['market_slug']}`")
                await _redeem_win(trade, market, up_index, down_index, winning_index)
            else:
                trade["live_profit_loss"] = None
                log_message(f"WIN (Paper): {trade['side']} on {trade['market_slug']}: {dir_txt} "
                            f"[{trade['resolution']}]. Paper Profit: ${trade['paper_profit_loss']:.2f}")
                await send_telegram(f"🏆 *WIN (Paper): {trade['side']}*\n• Paper Profit: `+${trade['paper_profit_loss']:.2f}`\n• Details: {dir_txt}\n• Market: `{trade['market_slug']}`")
        else:
            paper_amt = float(trade.get("paper_amount") or 0.0)
            trade["paper_profit_loss"] = -paper_amt
            if trade.get("live_status") == "FILLED" and trade.get("live_shares"):
                live_amt = float(trade.get("live_amount") or 0.0)
                trade["live_profit_loss"] = -live_amt
                log_message(f"LOSS: {trade['side']} on {trade['market_slug']}: {dir_txt} "
                            f"[{trade['resolution']}]. Live Loss: ${trade['live_profit_loss']:.2f} (Paper Loss: -${paper_amt:.2f})")
                await send_telegram(f"❌ *LOSS: {trade['side']}*\n• Live Loss: `-${live_amt:.2f}`\n• Details: {dir_txt}\n• Market: `{trade['market_slug']}`")
            else:
                trade["live_profit_loss"] = None
                log_message(f"LOSS (Paper): {trade['side']} on {trade['market_slug']}: {dir_txt} "
                            f"[{trade['resolution']}]. Paper Loss: -${paper_amt:.2f}")
                await send_telegram(f"❌ *LOSS (Paper): {trade['side']}*\n• Paper Loss: `-${paper_amt:.2f}`\n• Details: {dir_txt}\n• Market: `{trade['market_slug']}`")

        trade["status"] = "CLOSED"
        trade["exit_reason"] = trade.get("exit_reason") or "settled"
        trade["settlement_price_at_expiry"] = trade.get("settlement_price_at_expiry") or settlement_price
        _or_note(trade, market, up_index, down_index, winning_index)   # PATCH: official results — followed until Polymarket has resolved
        trade["winning_outcome"] = outcomes[winning_index] if 0 <= winning_index < len(outcomes) else None
        _le_log_result(trade)   # PATCH: longshot engine — the result in logs/longshots.csv
        state["trade_history"].append(_archive(trade))
        trades_changed = True

    # PATCH: longshot — trades entered while this pass was waiting on the network (Gamma poll,
    # cancels, redeems) were not in the snapshot; keep them instead of overwriting them away.
    _snap_ids = {id(t) for t in active_snapshot}
    state["active_trades"] = remaining_active + [t for t in state["active_trades"] if id(t) not in _snap_ids]
    if trades_changed:
        save_state()
        _sync_active_trades_to_latest_data()
        # Controlled GC invocation: cleanup expired trade objects during quiet settlement period
        import gc
        gc.collect()

# ── Capital Extractor (Auto-Withdrawal State Machine) ─────────────────────────
async def maybe_auto_withdraw(equity: float, poly_snapshot: Dict[str, Any]):
    if not settings.AUTO_WITHDRAW_ENABLED:
        state["withdraw_state"] = "idle"
        return
    if not settings.PRIVATE_KEY:
        return
    raw_dest = (settings.WITHDRAW_ADDRESS or "").strip()
    if raw_dest.startswith("0x") and len(raw_dest) == 42:
        dest_address = raw_dest
    else:
        dest_address = clob_trader.get_eoa_address() if clob_trader else None
    if not dest_address or settings.WITHDRAW_AMOUNT <= 0:
        return

    now_ts = time.time()
    st = state["withdraw_state"]

    if st == "idle":
        if equity >= settings.WITHDRAW_TRIGGER_BALANCE:
            if not state["active_trades"]:
                log_message(f"CAPITAL EXTRACTOR: Trigger reached (${equity:.2f} >= ${settings.WITHDRAW_TRIGGER_BALANCE:.2f}). Initiating withdrawal of ${settings.WITHDRAW_AMOUNT:.2f}...")
                state["withdraw_state"] = "in_progress"
                state["withdraw_submitted_at"] = now_ts
                state["withdraw_locked_market"] = poly_snapshot.get("market", {}).get("id") if poly_snapshot.get("ok") else None
                
                res = await asyncio.to_thread(clob_trader.withdraw_pusd, dest_address, settings.WITHDRAW_AMOUNT)
                if res.get("ok"):
                    tx = res.get("tx")
                    state["last_withdrawal"] = {
                        "amount": settings.WITHDRAW_AMOUNT,
                        "recipient": dest_address,
                        "timestamp": datetime.now().isoformat(),
                        "tx": tx,
                        "status": "submitted"
                    }
                    save_state()
                    log_message(f"CAPITAL EXTRACTOR: Withdrawal tx submitted ({tx}). Waiting confirmation...")
                    await send_telegram(f"💸 *Capital Extractor*\nWithdrew `${settings.WITHDRAW_AMOUNT:.2f}` pUSD to `{dest_address}`\nTx: `{tx}`")
                else:
                    log_message(f"CAPITAL EXTRACTOR FAILED: {res.get('error')}")
                    state["withdraw_state"] = "cooldown"
                    state["withdraw_submitted_at"] = now_ts

    elif st == "in_progress":
        last_w = state.get("last_withdrawal")
        tx = (last_w or {}).get("tx")
        confirmed = False
        if tx:
            confirmed = await asyncio.to_thread(clob_trader.is_tx_confirmed, tx)
        if confirmed or (now_ts - (state["withdraw_submitted_at"] or now_ts) > 45):
            if last_w:
                last_w["status"] = "confirmed"
                save_state()
            log_message(f"CAPITAL EXTRACTOR: Withdrawal confirmed! Entering cooldown.")
            state["withdraw_state"] = "cooldown"
            state["withdraw_submitted_at"] = now_ts

    elif st == "cooldown":
        resume_after = (settings.WITHDRAW_RESUME_AFTER or "flat").lower()
        if not settings.WITHDRAW_AUTO_RESUME:
            return

        can_resume = False
        sub_at = state.get("withdraw_submitted_at") or now_ts
        if resume_after in ("flat", "confirmed"):
            if now_ts - sub_at > 30:
                can_resume = True
        elif resume_after == "submitted":
            can_resume = True
        elif resume_after == "next_window":
            cur_mkt_id = poly_snapshot.get("market", {}).get("id") if poly_snapshot.get("ok") else None
            if cur_mkt_id and cur_mkt_id != state.get("withdraw_locked_market"):
                can_resume = True
        else:
            if now_ts - sub_at > 30:
                can_resume = True

        if can_resume:
            log_message("CAPITAL EXTRACTOR: Resuming trading operations.")
            state["withdraw_state"] = "idle"
            state["withdraw_locked_market"] = None

# ── Event-driven early evaluation & watcher ───────────────────────────────────
def entry_block_reason(ctx: Dict[str, Any]) -> Optional[str]:
    if not state["running"]:
        return "stopped"
    if state["withdraw_state"] == "in_progress":
        return "withdraw_in_progress"
    if not ctx:
        return "no_context"
    _mkt = ctx.get("market") or {}
    _mid = str(_mkt.get("id") or "")
    _mslug = str(_mkt.get("slug") or "")
    _sms = ctx.get("start_ms")

    timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)
    if _sms is not None and int(_sms) != int(timing["startMs"]):
        return "window_expired"

    # PATCH: longshot engine — a window whose regular slot is taken is still checked while its
    # long-shot slot is free (the engine may take a long shot there; evaluate_entry drops a regular
    # entry). Which slots are taken goes to evaluate_entry in ctx["le_busy"].
    # Own slot off (TOG's rule): the checks below stay as they were, and an engine long shot counts
    # as the window's position.
    if _le_on():
        ctx["le_busy"] = _le_window_busy(_mid, _mslug, _sms)
        if _le_own_slot():
            if ctx["le_busy"]["regular"] and ctx["le_busy"]["longshot"]:
                return "slot_busy"
            if not ctx.get("price_is_fresh"):
                return "stale_feed"
            if ctx.get("strike_open") is None:
                return "no_strike"
            if not ctx.get("market_ok"):
                return "market_not_ready"
            return None
        if ctx["le_busy"]["engine"]:
            return "slot_busy"

    # In-flight check for this market
    if (_mid and _mid in _in_flight_markets) or (_mslug and _mslug in _in_flight_markets):
        return "slot_busy"

    # Check if THIS specific window is already busy in active_trades
    for t in state["active_trades"]:
        if _is_longshot(t):  # PATCH: longshot — a long shot doesn't take the regular slot
            continue
        if _mid and str(t.get("market_id")) == _mid:
            return "slot_busy"
        if _mslug and t.get("market_slug") == _mslug:
            return "slot_busy"
        if _sms is not None and t.get("window_start_ms") == int(_sms):
            return "slot_busy"

    if not ctx.get("price_is_fresh"):
        return "stale_feed"
    if ctx.get("strike_open") is None:
        return "no_strike"
    if not ctx.get("market_ok"):
        return "market_not_ready"
    return None

async def evaluate_entry(reason: str = "event"):
    global _last_eval_ts
    if _entry_lock.locked():
        return
    async with _entry_lock:
        now = time.time()
        if now - _last_eval_ts < MIN_EVAL_INTERVAL_S:
            return
        _last_eval_ts = now

        ctx = state.get("trade_ctx")
        # PATCH: longshot engine — the window's final seconds. The regular check stops 5 s before the end
        # (below) and has no context under 6 s; an engine long shot may still enter, checked from the
        # last context seen and the live book (as the paper bot did, down to 2 s before the end).
        _le_keep_ctx(ctx)
        _le_final = _le_final_ctx(ctx, now)
        if _le_final is not None:
            await _le_final_check(_le_final, reason, now)
            return
        if not ctx:
            return

        timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)
        cur_start_ms = timing["startMs"]

        # Strict window check: never evaluate trades using stale previous window context!
        if ctx.get("start_ms") != cur_start_ms:
            state["trade_ctx"] = None
            return

        # Strict market expiry check
        mkt = ctx.get("market") or {}
        end_date = mkt.get("endDate")
        if end_date:
            try:
                end_ts = datetime.fromisoformat(str(end_date).replace('Z', '+00:00')).timestamp()
                if now >= end_ts - 5.0:
                    state["trade_ctx"] = None
                    return
            except Exception:
                pass

        if not ctx.get("strike_open") or not ctx.get("target_open"):
            state["trade_ctx"] = None
            return

        if now - ctx.get("built_at", 0) > CTX_MAX_AGE_S:
            return

        block = entry_block_reason(ctx)
        if block:
            return

        spot_price = binance_stream.get_last().get("price")
        if not spot_price:
            return

        mc_steps = _model_steps(ctx["time_left_min"])
        fair_up = indicators.fair_prob_up(
            spot_price, ctx["target_open"], mc_steps, ctx["sigma_5m"], drift_per_step=ctx["drift_5m"]
        )

        market_up = ctx["prices"]["up"]
        market_down = ctx["prices"]["down"]

        up_ws_sum = polymarket_clob_ws.get_summary(ctx["token_ids"].get("up"), max_age_s=settings.MAX_BOOK_AGE_S)
        down_ws_sum = polymarket_clob_ws.get_summary(ctx["token_ids"].get("down"), max_age_s=settings.MAX_BOOK_AGE_S)
        if up_ws_sum and up_ws_sum.get("bestAsk"):
            market_up = up_ws_sum["bestAsk"]
        if down_ws_sum and down_ws_sum.get("bestAsk"):
            market_down = down_ws_sum["bestAsk"]

        decision = engines.decide_ev({
            "mcProbUp": fair_up,
            "priceUp": market_up,
            "priceDown": market_down,
            "minProb": settings.MIN_PROB_EV,
            "evThreshold": settings.EV_THRESHOLD,
            "rsi": ctx["rsi"],
            "haExhaustedGreen": ctx["ha_exhausted_green"],
            "haExhaustedRed": ctx["ha_exhausted_red"],
        })

        # PATCH: longshot engine — a window whose regular slot is taken may only enter a long shot
        if _le_block_regular(decision, ctx, market_up, market_down):
            return

        if decision.get("action") == "ENTER":
            market_prices = {"up": market_up, "down": market_down}
            exec_res = await execute_trade(
                decision, market_prices, ctx["market"], ctx["strike_open"],
                ctx["token_ids"], ctx.get("orderbook", {}),
                strike_source=ctx["strike_source"], window_start_ms=ctx["start_ms"],
                open_reason=f"event_entry_{reason}"
            )
            state["event_exec"] = {
                "ts": datetime.now().isoformat(),
                "side": decision["side"],
                "reason": reason,
                "result": exec_res
            }
            if exec_res == "entered":
                await broadcast_state()

# ── Long shots (PATCH: longshot) ──────────────────────────────────────────────
def _ls_books(ctx: Dict[str, Any]):
    """Best ask + dollars in the top 5 ask levels per side (WebSocket book, else the loop's snapshot)."""
    books, raw = {}, {}
    for side, key in (("UP", "up"), ("DOWN", "down")):
        tok = (ctx.get("token_ids") or {}).get(key)
        summ = polymarket_clob_ws.get_summary(tok, max_age_s=settings.MAX_BOOK_AGE_S) if tok else None
        if not summ or not summ.get("bestAsk"):
            summ = (ctx.get("orderbook") or {}).get(key) or {}
        try:
            ask = float(summ["bestAsk"]) if summ.get("bestAsk") is not None else None
        except (TypeError, ValueError):
            ask = None
        liq_sh = summ.get("askLiquidity")
        try:
            liq_usd = float(liq_sh) * ask if (liq_sh is not None and ask) else None
        except (TypeError, ValueError):
            liq_usd = None
        books[side] = {"ask": ask, "liq_usd": liq_usd}
        raw[key] = {"bestAsk": ask, "askLiquidity": liq_sh}
    return books, raw


_ls_last_error_ts = 0.0


def _ls_follow_sides(ctx: Dict[str, Any], spot: float, secs_left: float, mode: Optional[str] = None):
    """PATCH: longshot follow — (sides a long shot may take, why), from settings longshot.follow.
    "bet": the side of this window's FIRST regular GTD trade, open or already closed; none without one.
    "prediction": the side the GTD model (the dashboard's Current Prediction) gives more than 50%,
    worked out now with the same formula and inputs as the update loop.
    "either": the sides of both rules together (one side when they agree, both when they differ).
    "off": None, meaning either side."""
    mode = mode or str(settings.LONGSHOT.get("follow") or "bet")
    if mode == "bet":
        mid = str((ctx.get("market") or {}).get("id"))
        bets = [t for t in list(state["active_trades"]) + list(state["trade_history"][-50:])
                if not _is_longshot(t) and str(t.get("market_id")) == mid and t.get("side") in ("UP", "DOWN")]
        if not bets:
            return [], "no GTD bet in this window"
        first = min(bets, key=lambda t: str(t.get("paper_entry_time") or ""))
        return [first["side"]], f"GTD bet {first['side']}"
    if mode == "prediction":
        fair_up = indicators.fair_prob_up(spot, ctx.get("target_open"), _model_steps(secs_left / 60.0),
                                          ctx.get("sigma_5m"), drift_per_step=ctx.get("drift_5m") or 0.0)
        if fair_up > 0.5:
            return ["UP"], f"GTD prediction UP {fair_up:.1%}"
        if fair_up < 0.5:
            return ["DOWN"], f"GTD prediction DOWN {1.0 - fair_up:.1%}"
        return [], "GTD prediction 50/50"
    if mode == "either":
        bet_sides, bet_why = _ls_follow_sides(ctx, spot, secs_left, "bet")
        pred_sides, pred_why = _ls_follow_sides(ctx, spot, secs_left, "prediction")
        return sorted(set(bet_sides) | set(pred_sides)), f"{bet_why}, {pred_why}"
    return None, "off"


_ls_follow_logged: set = set()


def _ls_note_skip(start_ms: int, side: str, ask: Optional[float], why: str) -> None:
    """PATCH: longshot follow — one log line per window and side when follow keeps a long shot out."""
    if (start_ms, side) in _ls_follow_logged:
        return
    if len(_ls_follow_logged) > 400:
        _ls_follow_logged.clear()
    _ls_follow_logged.add((start_ms, side))
    log_message(f"LONGSHOT {side} ask {float(ask or 0) * 100:.1f}¢ is in the band — skipped (follow: {why})")


def _ls_note_weak(start_ms: int, side: str, ask: Optional[float], why: str) -> None:
    """PATCH: sniper minimum chance — one log line per window and side when the minimum keeps a buy out."""
    if (start_ms, side, "chance") in _ls_follow_logged:
        return
    _ls_follow_logged.add((start_ms, side, "chance"))
    log_message(f"LONGSHOT {side} ask {float(ask or 0) * 100:.1f}¢ is in the band — skipped ({why})")


async def evaluate_longshot(reason: str = "event"):
    """Long-shot check: runs on the same events as evaluate_entry (Binance trades, the Chainlink
    feed, and order-book updates in the long-shot look window) and once per update_loop pass."""
    global _ls_last_error_ts
    cfg = settings.LONGSHOT
    if _ls_lock.locked():
        return
    async with _ls_lock:
        now = time.time()
        try:
            ctx = state.get("ls_ctx")
            if not ctx or not state["running"] or state["withdraw_state"] == "in_progress":
                return
            timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)
            if int(ctx.get("start_ms") or 0) != int(timing["startMs"]):
                state["ls_ctx"] = None
                return
            if now - ctx.get("built_at", 0) > CTX_MAX_AGE_S:
                return
            end_ts = float(ctx["end_ts"])
            secs_left = end_ts - now
            # The check runs while either the sniper bands or maker bids look
            if secs_left < longshot.eval_cutoff_s(cfg) or secs_left > longshot.eval_look_s(cfg):
                return

            spot = (binance_stream.get_last().get("price")
                    or state.get("last_spot_price")
                    or (state.get("latest_data", {}).get("prices", {}) or {}).get("spot"))
            tgt = ctx.get("target_open")
            strike_open = ctx.get("strike_open")
            strike_src = ctx.get("strike_source") or "chainlink_ws"
            status = {"ts": now, "secs_left": round(secs_left, 2)}
            if not spot or not tgt or not strike_open:
                state["ls_last"] = {**status, "result": "no_spot_or_open"}
                return

            p_up = longshot.prob_up_exact(spot, tgt, secs_left, ctx.get("sigma_5m"), ctx.get("drift_5m") or 0.0)
            info: Dict[str, Any] = {"secs_left": round(secs_left, 2), "spot": spot, "model_open": tgt, "strike_open": strike_open}
            _pv = _ps_chance(ctx, now, end_ts)       # PATCH: price stream — the chance on Polymarket's own numbers, when they are there
            if _pv:
                p_up = _pv.pop("p_up")
                info.update(_pv)
            trade_strike, strike_src = strike_open, strike_src

            cur_start = int(ctx["start_ms"])
            one_at_a_time = longshot.one_at_a_time_on(cfg)
            if one_at_a_time:
                any_active_ls = any(_is_longshot(t) and (t.get("window_start_ms") == cur_start or str(t.get("market_id")) == str(ctx.get("market", {}).get("id"))) for t in state["active_trades"])
                if any_active_ls or (cur_start in _ls_entered_windows):
                    state["ls_last"] = {**status, "result": "one_ls_at_a_time_busy"}
                    if hasattr(ls_maker_mgr, "cancel_window"):
                        await ls_maker_mgr.cancel_window(cur_start, "one_ls_active")
                    return

            books, raw_ob = _ls_books(ctx)
            allowed, follow_why = _ls_follow_sides(ctx, spot, secs_left)   # PATCH: longshot follow
            if longshot.maker_on(cfg):   # PATCH: longshot maker — the resting bids follow every check
                _toks = ctx.get("token_ids") or {}
                await ls_maker_mgr.on_view({
                    "ts": now, "start_ms": cur_start, "end_ts": end_ts, "p_up": p_up,
                    "token_ids": {"UP": _toks.get("up"), "DOWN": _toks.get("down")},
                    "sides": allowed, "follow_why": follow_why,          # PATCH: longshot follow
                    "meta": {"market": ctx["market"], "strike": trade_strike, "strike_src": strike_src, "info": dict(info)}})
            if one_at_a_time and hasattr(ls_maker_mgr, "has_active_bid") and ls_maker_mgr.has_active_bid(cur_start):
                state["ls_last"] = {**status, "result": "maker_placed_exclusive"}
                return
            ceiling = longshot.max_band_ask(cfg)
            if secs_left <= longshot.max_look_s(cfg):
                for side in ("UP", "DOWN"):
                    longshot.note_candidate(int(ctx["start_ms"]), side, p_up if side == "UP" else 1.0 - p_up,
                                            books[side]["ask"], secs_left, ceiling)
            decision = longshot.decide(p_up, books, secs_left, cfg, sides=allowed)   # PATCH: longshot follow (sides=)
            state["ls_last"] = {**status, "p_up": round(p_up, 4), "up_ask": books["UP"]["ask"],
                                "down_ask": books["DOWN"]["ask"], "result": decision.get("reason"), **info}
            if longshot.maker_on(cfg):
                state["ls_last"]["maker"] = ls_maker_mgr.active()
            state["ls_last"]["follow"] = follow_why                          # PATCH: longshot follow
            if decision.get("action") != "ENTER":
                for side in decision.get("skipped") or []:                   # PATCH: longshot follow
                    _ls_note_skip(int(ctx["start_ms"]), side, books[side]["ask"], follow_why)
                for side, why in (decision.get("weak") or {}).items():       # PATCH: sniper minimum chance
                    _ls_note_weak(int(ctx["start_ms"]), side, books[side]["ask"], why)
                return

            decision["longshot"] = info
            res = await execute_trade(
                decision, {"up": books["UP"]["ask"], "down": books["DOWN"]["ask"]}, ctx["market"], trade_strike,
                ctx.get("token_ids") or {}, raw_ob, strike_source=strike_src, window_start_ms=ctx["start_ms"],
                open_reason=f"longshot_{decision.get('band')}", kind="longshot")
            state["ls_last"]["exec"] = res
            if res == "entered":
                if one_at_a_time:
                    _ls_entered_windows.add(int(ctx["start_ms"]))
                    if hasattr(ls_maker_mgr, "cancel_window"):
                        await ls_maker_mgr.cancel_window(cur_start, "sniper_entered")
                await broadcast_state()
        except Exception as e:
            if now - _ls_last_error_ts > 60:
                _ls_last_error_ts = now
                log_message(f"LONGSHOT check error: {type(e).__name__}: {e}")


def _ls_window_summary(start_ms: Optional[int]):
    """One log line per finished window when long shots are on and none was entered."""
    if start_ms is None:
        return
    best = longshot.pop_window_best(int(start_ms))
    entered = int(start_ms) in _ls_entered_windows
    _ls_entered_windows.discard(int(start_ms))
    if entered or not longshot.sniper_on(settings.LONGSHOT):   # PATCH: longshot strategies — no sniper, no sniper line
        return
    label = datetime.fromtimestamp(int(start_ms) / 1000).strftime('%H:%M')
    if best:
        log_message(f"LONGSHOT {label} window: no entry — closest {best['side']} ask {best['ask'] * 100:.1f}¢, "
                    f"chance {best['prob']:.3f} (×{best['multiple']:.1f}) with {best['secs_left']:.0f}s left")
    else:
        log_message(f"LONGSHOT {label} window: no entry — no ask inside the bands during the look window")


def _on_book_update():
    """PATCH: longshot — an order-book change wakes the checks in the long-shot look window."""
    cfg = settings.LONGSHOT
    ctx = state.get("ls_ctx")
    if ctx and 0.0 <= float(ctx.get("end_ts") or 0.0) - time.time() <= longshot.eval_look_s(cfg):
        _market_event.set()


async def entry_watcher():
    while True:
        try:
            await _market_event.wait()
            _market_event.clear()
            await evaluate_entry(reason="trade_tick")
            await evaluate_longshot(reason="trade_tick")  # PATCH: longshot
        except Exception:
            await asyncio.sleep(0.1)



_last_redeem_retry_ts = 0.0
async def retry_pending_redeems():
    global _last_redeem_retry_ts
    now = time.time()
    if now - _last_redeem_retry_ts < 20.0 or not settings.PRIVATE_KEY:
        return
    _last_redeem_retry_ts = now
    pending = [t for t in state.get("trade_history", [])
               if t.get("live_status") == "FILLED"
               and t.get("redeem_status") in ("PENDING_ONCHAIN_RESOLUTION", "PENDING_RELAYER_KEY")
               and t.get("condition_id") and t.get("redeem_amounts")]
    for t in pending:
        cid = t.get("condition_id")
        amts = t.get("redeem_amounts")
        neg = bool(t.get("neg_risk"))
        try:
            res = await asyncio.to_thread(clob_trader.redeem, cid, amts, neg)
            if res.get("ok"):
                t["redeem_status"] = "CONFIRMED"
                t["redeem"] = res
                idx = 0 if amts[0] > 0 else 1
                log_message(f"REDEEM ok for {t['market_slug']}: {amts[idx]:.2f} live shares (tx {res.get('tx')})")
                asyncio.create_task(asyncio.to_thread(save_state))
        except Exception:
            pass

async def live_balance_updater():
    """Background task to continuously poll the CLOB USDC balance (every 2.5s)
    so the bot always has the live balance in RAM, and precomputes the next
    live trade stake with 0ms latency at trade time."""
    while True:
        try:
            if settings.PRIVATE_KEY:
                bal = await asyncio.to_thread(clob_trader.get_usdc_balance)
                if bal is not None and bal >= 0:
                    state["live_balance"] = bal
                    state["last_balance_refresh"] = time.time()

                    # Precompute the exact trade amount for the next live trade
                    risk_type = (settings.RISK_TYPE or "percent").lower()
                    if risk_type == "fixed":
                        stake = float(settings.RISK_VALUE)
                    else:
                        stake = (float(settings.RISK_VALUE) / 100.0) * bal

                    if bal >= 1.0:
                        stake = max(1.0, min(stake, bal))
                    else:
                        stake = 0.0

                    state["precomputed_live_stake"] = round(stake, 2)
                await retry_pending_redeems()
        except Exception:
            pass
        await asyncio.sleep(2.5)

_seeding_klines = False
async def seed_kline_buffers():
    global _seeding_klines
    if globals().get("_seeding_klines"):
        return
    _seeding_klines = True
    try:
        if not settings.SYMBOL or not str(settings.SYMBOL).strip():
            settings.SYMBOL = "BTCUSDT"
        k1m, k5m = await asyncio.gather(
            data.fetch_klines(settings.SYMBOL, "1m", 240),
            data.fetch_klines(settings.SYMBOL, "5m", 200)
        )
        if k1m:
            binance_kline_1m.set_candles(k1m)
            if not state.get("last_spot_price"):
                last_c = k1m[-1]["close"]
                state["last_spot_price"] = last_c
                prices = state.setdefault("latest_data", {}).setdefault("prices", {})
                prices["spot"] = last_c
            if len(k1m) >= 15:
                closes = [c["close"] for c in k1m if c.get("close")]
                state["last_rsi"] = indicators.compute_rsi(closes, settings.RSI_PERIOD)
                state.setdefault("latest_data", {}).setdefault("indicators", {})["rsi"] = state["last_rsi"]
            if len(k1m) >= 2:
                state["last_ha_1m"] = indicators.count_consecutive(indicators.compute_heiken_ashi(k1m))
                state.setdefault("latest_data", {}).setdefault("indicators", {})["heiken"] = state["last_ha_1m"]
        if k5m:
            binance_kline_5m.set_candles(k5m)
            if len(k5m) >= 2:
                state["last_ha_5m"] = indicators.count_consecutive(indicators.compute_heiken_ashi(k5m))
                state.setdefault("latest_data", {}).setdefault("indicators", {})["heiken_5m"] = state["last_ha_5m"]
        log_message(f"Seeded Binance kline buffers (1m: {len(k1m) if k1m else 0}, 5m: {len(k5m) if k5m else 0}) for {settings.SYMBOL}")
    except Exception as e:
        log_message(f"Failed to seed kline buffers: {e}")
    finally:
        _seeding_klines = False

# ── Main update loop ──────────────────────────────────────────────────────────
async def update_loop():
    csv_header = [
        "timestamp", "entry_minute", "time_left_min", "signal",
        "model_up", "model_down", "mkt_up", "mkt_down", "edge_up", "edge_down",
        "recommendation", "reason", "exec_result"
    ]

    while True:
        try:
            timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)
            cur_window_start_ms = int(timing["startMs"])
            prev_window_start_ms = state.get("last_window_start")

            # In the last minute of a window, look up the next window's market (once; again
            # after 10 s if it wasn't listed yet) so nothing has to be fetched when it opens.
            _next_start = int(timing["endMs"])
            if (timing["remainingMs"] < NEXT_MARKET_PREFETCH_MS and not settings.POLYMARKET_SLUG
                    and _next_market.get("start_ms") != _next_start and not _next_market_busy
                    and time.time() - _prefetch_tries.get(_next_start, 0.0) > 10.0):
                _prefetch_tries.clear()
                _prefetch_tries[_next_start] = time.time()
                asyncio.create_task(_prefetch_next_market(_next_start))

            # ── WINDOW ROLLOVER DETECTED ──
            # A new 15-minute window has begun (:00, :15, :30, :45).
            # Purge all stale context, cached market, and previous window odds immediately
            # so the bot NEVER uses old window data to calculate signals or enter trades!
            if prev_window_start_ms is not None and cur_window_start_ms != prev_window_start_ms:
                state["trade_ctx"] = None
                _cached_market = None
                _cached_market_ts = 0.0
                state["last_poly_up"] = None
                state["last_poly_down"] = None
                # (books are no longer cleared here: the new window's were subscribed before it
                # opened, and books are looked up by token, so the old ones can't be mixed in)
                log_message(f"Window rollover detected: new window started at {datetime.fromtimestamp(cur_window_start_ms / 1000).strftime('%H:%M:%S')}. Cleared old window context.")
                state["ls_ctx"] = None              # PATCH: longshot
                _ls_window_summary(prev_window_start_ms)

            binance_ws = binance_stream.get_last()
            poly_ws = polymarket_ws_stream.get_last()
            poly_ws = _ps_live_as_poly_ws(poly_ws)   # PATCH: price stream — Polymarket's live price from the price socket, while "prices" is on and it is fresh
            cl_ws = chainlink_ws_stream.get_last()

            spot_cand = binance_ws.get("price") if binance_ws and binance_ws.get("price") else state.get("last_spot_price")
            need_rest_spot = spot_cand is None
            need_rest_chainlink = not (poly_ws.get("price") or cl_ws.get("price") or state.get("last_chainlink_price"))

            active_tf = settings.CANDLE_WINDOW_MINUTES
            other_tf = 5 if active_tf == 15 else 15

            results = await asyncio.gather(
                data.fetch_last_price(settings.SYMBOL) if need_rest_spot else asyncio.sleep(0, result=None),
                chainlink.chainlink_fetcher.fetch_chainlink_btc_usd() if need_rest_chainlink else asyncio.sleep(0, result={}),
                fetch_polymarket_snapshot(),
                fetch_other_tf_snapshot(other_tf),
                return_exceptions=True
            )

            last_price = results[0] if not isinstance(results[0], Exception) else None
            chainlink_data = results[1] if not isinstance(results[1], Exception) else {}
            poly_snapshot = results[2] if not isinstance(results[2], Exception) else {"ok": False}
            other_snapshot = results[3] if not isinstance(results[3], Exception) else {"ok": False}

            klines_1m = binance_kline_1m.get_candles()
            klines_5m = binance_kline_5m.get_candles()

            spot_price = binance_ws.get("price") if binance_ws and binance_ws.get("price") else last_price
            if spot_price:
                state["last_spot_price"] = spot_price
            else:
                spot_price = state.get("last_spot_price")

            # Auto-heal kline buffers if empty or low
            if len(klines_1m) < 15 or len(klines_5m) < 15:
                now_t = time.time()
                if now_t - state.get("_last_seed_kline_ts", 0) > 10.0:
                    state["_last_seed_kline_ts"] = now_t
                    asyncio.create_task(seed_kline_buffers())

            # Settle feed & freshness
            current_price = None
            price_source = None
            price_is_fresh = True
            poly_updated = poly_ws.get("updatedAt") or 0
            if poly_ws.get("price"):
                current_price = poly_ws["price"]
                price_source = "Polymarket WS"
                if poly_updated > 0:
                    age_ms = (time.time() * 1000) - poly_updated
                    if age_ms > POLY_WS_MAX_AGE_MS:
                        price_is_fresh = False
            elif cl_ws.get("price"):
                current_price = cl_ws["price"]
                price_source = "Chainlink RPC WS"
            elif state.get("last_chainlink_price"):
                current_price = state["last_chainlink_price"]
                price_source = "Chainlink RPC WS"
            elif chainlink_data.get("price"):
                current_price = chainlink_data["price"]
                price_source = "Chainlink RPC REST"

            # PATCH: longshot — when the Polymarket WS isn't the source, record the fallback price too
            if current_price and price_source != "Polymarket WS":
                longshot.record_price(time.time(), current_price)

            closes = [c["close"] for c in klines_1m if c.get("close")]
            rsi_now = indicators.compute_rsi(closes, settings.RSI_PERIOD) if len(closes) >= 15 else state.get("last_rsi")
            if rsi_now is not None:
                state["last_rsi"] = rsi_now

            consec = indicators.count_consecutive(indicators.compute_heiken_ashi(klines_1m)) if len(klines_1m) >= 2 else (state.get("last_ha_1m") or {"color": None, "count": 0})
            if consec.get("color"):
                state["last_ha_1m"] = consec

            consec_5m = {"color": None, "count": 0}
            if len(klines_5m) >= 2:
                consec_5m = indicators.count_consecutive(indicators.compute_heiken_ashi(klines_5m))
                if consec_5m.get("color"):
                    state["last_ha_5m"] = consec_5m
            elif state.get("last_ha_5m"):
                consec_5m = state["last_ha_5m"]

            EB = engines.EXHAUSTION_BARS
            def _is(color, count, want):
                return color == want and (count or 0) >= EB

            ha_exhausted_green = _is(consec["color"], consec["count"], "green") or _is(consec_5m["color"], consec_5m["count"], "green")
            ha_exhausted_red = _is(consec["color"], consec["count"], "red") or _is(consec_5m["color"], consec_5m["count"], "red")
            drift_5m, sigma_5m = indicators.realized_drift_vol(klines_5m, lookback=300)

            timing = get_candle_window_timing()
            poly_market = poly_snapshot.get("market") if poly_snapshot.get("ok") else None
            window_ms = settings.CANDLE_WINDOW_MINUTES * 60_000

            start_ms = None
            settle_ms = None
            if poly_market:
                event_start_raw = poly_market.get("eventStartTime") or poly_market.get("gameStartTime")
                if event_start_raw:
                    try:
                        start_ms = int(datetime.fromisoformat(str(event_start_raw).replace('Z', '+00:00')).timestamp() * 1000)
                    except Exception:
                        start_ms = None
                if start_ms is None and poly_market.get("endDate"):
                    try:
                        start_ms = int(datetime.fromisoformat(poly_market["endDate"].replace('Z', '+00:00')).timestamp() * 1000) - window_ms
                    except Exception:
                        start_ms = None
                if poly_market.get("endDate"):
                    try:
                        settle_ms = int(datetime.fromisoformat(str(poly_market["endDate"]).replace('Z', '+00:00')).timestamp() * 1000)
                    except Exception:
                        settle_ms = None

            if start_ms is None:
                start_ms = int(timing["startMs"])

            win = mark_window_open(start_ms, window_ms, current_price, spot_price, price_source,
                                   price_is_fresh=price_is_fresh,
                                   price_ts_ms=(poly_updated or None) if price_source == "Polymarket WS" else None)
            strike_open = win["chainlink"]
            strike_source = "chainlink_ws"
            # PATCH: price stream — the price to beat, once Polymarket has published it; and its line in the log
            strike_open, strike_source = _ps_strike(start_ms, strike_open, strike_source)
            if strike_open is not None and win.get("chainlink") is None:
                win["chainlink"] = strike_open
                win["genuine"] = True
            _ps_window_note(start_ms, window_ms, win["chainlink"])

            model_open = None
            for c in reversed(klines_5m):
                if c["openTime"] == start_ms or (c["openTime"] <= start_ms < c["openTime"] + 300_000):
                    model_open = c["open"]
                    break
            if model_open is None and klines_1m:
                for c in reversed(klines_1m):
                    if c["openTime"] == start_ms or (c["openTime"] <= start_ms < c["openTime"] + 60_000):
                        model_open = c["open"]
                        break

            target_open = strike_open
            if settle_ms is not None:
                time_left_min = max(0.0, (settle_ms - time.time() * 1000) / 60_000)
            else:
                time_left_min = max(0.0, timing["remainingMinutes"])

            market_up = poly_snapshot.get("prices", {}).get("up") if poly_snapshot.get("ok") else None
            market_down = poly_snapshot.get("prices", {}).get("down") if poly_snapshot.get("ok") else None
            if market_up is not None:
                state["last_poly_up"] = market_up
            else:
                market_up = state.get("last_poly_up")
            if market_down is not None:
                state["last_poly_down"] = market_down
            else:
                market_down = state.get("last_poly_down")

            implied_prob_up = None
            if market_up is not None and market_down is not None and (market_up + market_down) > 0:
                implied_prob_up = market_up / (market_up + market_down)

            mc_steps = _model_steps(time_left_min)
            since_start = (time.time() * 1000) - start_ms
            if strike_open is None or target_open is None:
                fair_prob_up = 0.50
                if since_start < MARK_CAPTURE_WINDOW_MS:
                    decision = {"action": "NO_TRADE", "side": None, "phase": "EV", "strength": "EV", "reason": "waiting_new_window_strike"}
                else:
                    decision = {"action": "NO_TRADE", "side": None, "phase": "EV", "strength": "EV", "reason": "no_strike_window_skipped"}
            else:
                fair_prob_up = indicators.fair_prob_up(spot_price or 0, target_open, mc_steps, sigma_5m, drift_per_step=drift_5m or 0.0)
                decision = engines.decide_ev({
                    "mcProbUp": fair_prob_up,
                    "priceUp": market_up,
                    "priceDown": market_down,
                    "minProb": settings.MIN_PROB_EV,
                    "evThreshold": settings.EV_THRESHOLD,
                    "rsi": rsi_now,
                    "haExhaustedGreen": ha_exhausted_green,
                    "haExhaustedRed": ha_exhausted_red,
                })

            edge = {
                "marketUp": implied_prob_up,
                "marketDown": (1 - implied_prob_up) if implied_prob_up is not None else None,
                "edgeUp": (fair_prob_up - implied_prob_up) if implied_prob_up is not None else None,
                "edgeDown": ((1 - fair_prob_up) - (1 - implied_prob_up)) if implied_prob_up is not None else None,
            }
            prob_view = {"adjustedUp": fair_prob_up, "adjustedDown": 1 - fair_prob_up}
            fair_data = {
                "prob_up": fair_prob_up,
                "prob_down": 1.0 - fair_prob_up,
                "bias": "BULLISH" if fair_prob_up > 0.6 else "BEARISH" if fair_prob_up < 0.4 else "NEUTRAL",
                "steps": mc_steps,
                "sigma_5m": sigma_5m,
            }

            # Publish trade_ctx ONLY IF active window strike is genuinely captured and market is valid!
            if (poly_snapshot.get("ok") and strike_open is not None and target_open is not None
                    and poly_snapshot.get("market") and time_left_min > 0.1):
                tok_up = poly_snapshot.get("token_ids", {}).get("up")
                tok_down = poly_snapshot.get("token_ids", {}).get("down")
                if tok_up:
                    clob_trader.warm_token(tok_up)
                if tok_down:
                    clob_trader.warm_token(tok_down)

                mkt_end_date = poly_snapshot["market"].get("endDate")
                mkt_end_ts = None
                if mkt_end_date:
                    try:
                        mkt_end_ts = datetime.fromisoformat(str(mkt_end_date).replace('Z', '+00:00')).timestamp()
                    except Exception:
                        mkt_end_ts = None
                if not mkt_end_ts:
                    mkt_end_ts = time.time() + (time_left_min * 60.0)

                state["trade_ctx"] = {
                    "built_at": time.time(),
                    "end_ts": mkt_end_ts,
                    "start_ms": start_ms,
                    "target_open": target_open,
                    "strike_open": strike_open,
                    "strike_source": strike_source,
                    "time_left_min": time_left_min,
                    "sigma_5m": sigma_5m,
                    "drift_5m": drift_5m or 0.0,
                    "rsi": rsi_now,
                    "ha_exhausted_green": ha_exhausted_green,
                    "ha_exhausted_red": ha_exhausted_red,
                    "prices": poly_snapshot["prices"],
                    "token_ids": poly_snapshot["token_ids"],
                    "orderbook": poly_snapshot.get("orderbook", {}),
                    "market": poly_snapshot["market"],
                    "market_ok": True,
                    "price_is_fresh": price_is_fresh,
                    "slot_busy": any(str(t.get("market_id")) == str(poly_snapshot["market"].get("id")) and not _is_longshot(t)
                                     for t in state["active_trades"]),
                }
            else:
                state["trade_ctx"] = None

            # PATCH: longshot — context for evaluate_longshot (valid until end - cutoff_s)
            ls_ctx = None
            if poly_snapshot.get("ok") and poly_snapshot.get("market"):
                ls_end = None
                try:
                    ls_end = datetime.fromisoformat(str(poly_snapshot["market"].get("endDate")).replace('Z', '+00:00')).timestamp()
                except Exception:
                    ls_end = None
                if not ls_end:
                    ls_end = time.time() + time_left_min * 60.0
                if ls_end - time.time() > longshot.eval_cutoff_s(settings.LONGSHOT):
                    ls_ctx = {
                        "built_at": time.time(), "end_ts": ls_end, "start_ms": start_ms,
                        "strike_open": strike_open, "strike_source": strike_source, "target_open": target_open,
                        "sigma_5m": sigma_5m, "drift_5m": drift_5m or 0.0,
                        "prices": poly_snapshot.get("prices", {}), "token_ids": poly_snapshot.get("token_ids", {}),
                        "orderbook": poly_snapshot.get("orderbook", {}), "market": poly_snapshot["market"],
                    }
            prev_ls = state.get("ls_ctx")
            if (ls_ctx is None and prev_ls and int(prev_ls.get("start_ms") or 0) == cur_window_start_ms
                    and float(prev_ls.get("end_ts") or 0) - time.time() > longshot.eval_cutoff_s(settings.LONGSHOT)):
                ls_ctx = dict(prev_ls, built_at=time.time(), orderbook={})

            if ls_ctx is not None and longshot.any_on(settings.LONGSHOT):
                state["ls_ctx"] = ls_ctx
                asyncio.create_task(evaluate_longshot(reason="loop"))

            # Auto-Entry Execution for Active Trading Timeframe
            exec_result = None
            if decision["action"] == "ENTER" and state["running"]:
                # PATCH: longshot engine — an entry at engine_max_price or under is skipped here
                # (execute_trade hands it to the engine, which has its own window rules and its own log)
                if not _le_block_regular(decision, state.get("trade_ctx") or {}, market_up, market_down):
                    log_message(f"AUTO-ENTRY TRIGGERED: {decision['side']} (Phase: {decision['phase']}, Strength: {decision['strength']}, EV: {decision.get('ev', 0):.2f})")
                    exec_result = await execute_trade(
                        decision,
                        {"up": market_up, "down": market_down},
                        poly_snapshot.get("market"),
                        strike_open,
                        poly_snapshot.get("token_ids", {}),
                        poly_snapshot.get("orderbook"),
                        strike_source=strike_source,
                        window_start_ms=start_ms
                    )

            live_open_value = 0.0
            if settings.PRIVATE_KEY and poly_snapshot.get("ok"):
                for t in state["active_trades"]:
                    if t.get("live_shares") and t.get("live_status") == "FILLED":
                        side = t.get("side", "").lower()
                        cur_bid = poly_snapshot.get("prices", {}).get(side)
                        if cur_bid:
                            live_open_value += float(t["live_shares"]) * float(cur_bid)

            live_balance = float(state.get("live_balance") or 0.0)
            total_equity = live_balance + live_open_value
            await maybe_auto_withdraw(total_equity, poly_snapshot)

            signal_label = f"BUY {decision['side']}" if decision["action"] == "ENTER" else "NO TRADE"
            try:
                utils.append_csv_row(SIGNALS_PATH, csv_header, [
                    datetime.now().isoformat(), timing["elapsedMinutes"], time_left_min,
                    signal_label, fair_prob_up, 1 - fair_prob_up, market_up, market_down,
                    edge["edgeUp"], edge["edgeDown"], f"{decision['side']}:{decision['phase']}:{decision['strength']}" if decision["action"] == "ENTER" else "NO_TRADE",
                    decision.get("reason", ""), exec_result or ""
                ])
            except Exception:
                pass

            prices_active = {
                "current": current_price,
                "source": price_source,
                "spot": spot_price,
                "spot_source": "Binance WS" if (binance_ws and binance_ws.get("price")) else ("Binance REST" if last_price else None),
                "chainlink": current_price,
                "chainlink_source": price_source,
                "poly_up": market_up,
                "poly_down": market_down,
                "up": market_up,
                "down": market_down,
                "up_buy": poly_snapshot.get("prices", {}).get("up_buy"),
                "down_buy": poly_snapshot.get("prices", {}).get("down_buy"),
                "spread_up": poly_snapshot.get("prices", {}).get("spread_up"),
                "spread_down": poly_snapshot.get("prices", {}).get("spread_down"),
                "window_open": strike_open,
                "window_open_source": strike_source,
                "model_open": model_open,
                "window_start_ms": start_ms,
                "price_is_fresh": price_is_fresh,
                "book_source": poly_snapshot.get("book_source") if poly_snapshot.get("ok") else None,
                "price_stream": polybolt_feed.view() if polybolt.on(settings.PRICE_STREAM) else None   # PATCH: price stream
            }

            # View-only snapshot and analysis for non-active timeframe
            other_timing = get_candle_window_timing(other_tf)
            other_market = other_snapshot.get("market") if other_snapshot.get("ok") else None
            other_w_ms = other_tf * 60_000
            other_start_ms = None
            if other_market:
                _oesr = other_market.get("eventStartTime") or other_market.get("gameStartTime")
                if _oesr:
                    try:
                        other_start_ms = int(datetime.fromisoformat(str(_oesr).replace('Z', '+00:00')).timestamp() * 1000)
                    except Exception:
                        other_start_ms = None
                if other_start_ms is None and other_market.get("endDate"):
                    try:
                        other_start_ms = int(datetime.fromisoformat(other_market["endDate"].replace('Z', '+00:00')).timestamp() * 1000) - other_w_ms
                    except Exception:
                        other_start_ms = None
            if other_start_ms is None:
                other_start_ms = int(other_timing["startMs"])

            other_m_up = other_snapshot.get("prices", {}).get("up") if other_snapshot.get("ok") else None
            other_m_down = other_snapshot.get("prices", {}).get("down") if other_snapshot.get("ok") else None
            other_implied_up = None
            if other_m_up is not None and other_m_down is not None and (other_m_up + other_m_down) > 0:
                other_implied_up = other_m_up / (other_m_up + other_m_down)

            other_model_open = None
            for c in reversed(klines_5m):
                if c["openTime"] == other_start_ms or (c["openTime"] <= other_start_ms < c["openTime"] + 300_000):
                    other_model_open = c["open"]
                    break
            if other_model_open is None and klines_1m:
                for c in reversed(klines_1m):
                    if c["openTime"] == other_start_ms or (c["openTime"] <= other_start_ms < c["openTime"] + 60_000):
                        other_model_open = c["open"]
                        break

            other_strike = _other_tf_strikes.get(other_start_ms)
            if other_strike is None and current_price and price_is_fresh:
                if 0 <= (time.time() * 1000 - other_start_ms) < MARK_CAPTURE_WINDOW_MS:
                    _other_tf_strikes[other_start_ms] = current_price
                    other_strike = current_price
            if _ps_use("prices"):
                other_ptb = polybolt_feed.twap_at(other_start_ms / 1000.0, _ps_tol())
                if other_ptb is not None:
                    other_strike = other_ptb
                    _other_tf_strikes[other_start_ms] = other_ptb

            other_left_min = max(0.0, other_timing["remainingMinutes"])
            other_mc_steps = _model_steps(other_left_min)
            if other_strike is None:
                other_fair_up = 0.50
                other_decision = {"action": "NO_TRADE", "side": None, "phase": "EV", "strength": "EV", "reason": "waiting_new_window_strike"}
            else:
                other_fair_up = indicators.fair_prob_up(spot_price or 0, other_strike, other_mc_steps, sigma_5m, drift_per_step=drift_5m or 0.0)
                other_decision = engines.decide_ev({
                    "mcProbUp": other_fair_up,
                    "priceUp": other_m_up,
                    "priceDown": other_m_down,
                    "minProb": settings.MIN_PROB_EV,
                    "evThreshold": settings.EV_THRESHOLD,
                    "rsi": rsi_now,
                    "haExhaustedGreen": ha_exhausted_green,
                    "haExhaustedRed": ha_exhausted_red,
                })

            other_view_data = {
                "timeframe": other_tf,
                "timing": other_timing,
                "market": other_market,
                "prices": {
                    "current": current_price,
                    "source": price_source,
                    "spot": spot_price,
                    "spot_source": "Binance WS" if (binance_ws and binance_ws.get("price")) else ("Binance REST" if last_price else None),
                    "up": other_m_up,
                    "down": other_m_down,
                    "up_buy": other_snapshot.get("prices", {}).get("up_buy"),
                    "down_buy": other_snapshot.get("prices", {}).get("down_buy"),
                    "spread_up": other_snapshot.get("prices", {}).get("spread_up"),
                    "spread_down": other_snapshot.get("prices", {}).get("spread_down"),
                    "window_open": other_strike,
                    "window_open_source": "view_only",
                    "model_open": other_model_open,
                    "window_start_ms": other_start_ms,
                    "price_is_fresh": price_is_fresh,
                    "book_source": other_snapshot.get("book_source") if other_snapshot.get("ok") else None,
                    "price_stream": polybolt_feed.view() if polybolt.on(settings.PRICE_STREAM) else None
                },
                "orderbook": other_snapshot.get("orderbook", {}),
                "indicators": {
                    "rsi": rsi_now,
                    "heiken": consec,
                    "heiken_5m": consec_5m,
                    "fair": {
                        "prob_up": other_fair_up,
                        "prob_down": 1.0 - other_fair_up,
                        "bias": "BULLISH" if other_fair_up > 0.6 else "BEARISH" if other_fair_up < 0.4 else "NEUTRAL",
                        "steps": other_mc_steps,
                        "sigma_5m": sigma_5m,
                    }
                },
                "analysis": {
                    "probability": {"adjustedUp": other_fair_up, "adjustedDown": 1.0 - other_fair_up},
                    "edge": {
                        "marketUp": other_implied_up,
                        "marketDown": (1 - other_implied_up) if other_implied_up is not None else None,
                        "edgeUp": (other_fair_up - other_implied_up) if other_implied_up is not None else None,
                        "edgeDown": ((1 - other_fair_up) - (1 - other_implied_up)) if other_implied_up is not None else None,
                    },
                    "decision": other_decision,
                    "longshot": None,
                },
                "is_trading": False,
            }

            active_view_data = {
                "timeframe": active_tf,
                "timing": timing,
                "market": poly_snapshot.get("market") if poly_snapshot.get("ok") else None,
                "prices": state["latest_data"]["prices"],
                "orderbook": poly_snapshot.get("orderbook", {}),
                "indicators": {
                    "rsi": rsi_now,
                    "heiken": consec,
                    "heiken_5m": consec_5m,
                    "fair": fair_data,
                },
                "analysis": {
                    "probability": prob_view,
                    "edge": edge,
                    "decision": decision,
                    "longshot": state.get("ls_last"),
                },
                "is_trading": True,
            }

            tf_data = {
                str(active_tf): active_view_data,
                str(other_tf): other_view_data
            }

            state["latest_data"] = {
                "timestamp": datetime.now().isoformat(),
                "timing": timing,
                "market": poly_snapshot.get("market") if poly_snapshot["ok"] else None,
                "trading_state": {
                    "mode": "hybrid",
                    "running": state["running"],
                    "balance": live_balance if settings.PRIVATE_KEY else None,
                    "equity": total_equity if settings.PRIVATE_KEY else None,
                    "open_value": live_open_value if settings.PRIVATE_KEY else None,
                    "has_live_creds": bool(settings.PRIVATE_KEY),
                    "active_trades": state["active_trades"],
                    "history_count": len(state["trade_history"]),
                    "risk": {"type": settings.RISK_TYPE, "value": settings.RISK_VALUE},
                    "symbol": settings.SYMBOL,
                    "candle_window_minutes": settings.CANDLE_WINDOW_MINUTES,
                    "withdraw_state": state["withdraw_state"],
                    "last_withdrawal": state.get("last_withdrawal")
                },
                "prices": {
                    "current": current_price,
                    "source": price_source,
                    "spot": spot_price,
                    "spot_source": "Binance WS" if (binance_ws and binance_ws.get("price")) else ("Binance REST" if last_price else None),
                    "chainlink": current_price,
                    "chainlink_source": price_source,
                    "poly_up": market_up,
                    "poly_down": market_down,
                    "up": market_up,
                    "down": market_down,
                    "up_buy": poly_snapshot.get("prices", {}).get("up_buy"),
                    "down_buy": poly_snapshot.get("prices", {}).get("down_buy"),
                    "spread_up": poly_snapshot.get("prices", {}).get("spread_up"),
                    "spread_down": poly_snapshot.get("prices", {}).get("spread_down"),
                    "window_open": strike_open,
                    "window_open_source": strike_source,
                    "model_open": model_open,
                    "window_start_ms": start_ms,
                    "price_is_fresh": price_is_fresh,
                    "book_source": poly_snapshot.get("book_source") if poly_snapshot.get("ok") else None,
                    "price_stream": polybolt_feed.view() if polybolt.on(settings.PRICE_STREAM) else None   # PATCH: price stream
                },
                "indicators": {
                    "rsi": rsi_now,
                    "heiken": consec,
                    "heiken_5m": consec_5m,
                    "drift_5m": drift_5m,
                    "sigma_5m": sigma_5m
                },
                "analysis": {
                    "fairProbUp": fair_prob_up,
                    "fairProbDown": 1.0 - fair_prob_up if fair_prob_up is not None else None,
                    "impliedProbUp": implied_prob_up,
                    "impliedProbDown": (1.0 - implied_prob_up) if implied_prob_up is not None else None,
                    "edgeUp": edge["edgeUp"],
                    "edgeDown": edge["edgeDown"],
                    "decision": decision
                },
                "tf_data": tf_data
            }

            state["last_update_ts"] = time.time()

            await broadcast_state()

        except Exception as e:
            print(f"Error in update loop: {e}")

        await _wait_next_pass()


async def _wait_next_pass():
    """Sleep until update_loop's next pass: POLL_INTERVAL_MS as before, but never past the
    next window opening (the pass at the open used to come up to POLL_INTERVAL_MS late),
    and right away when _loop_wake is set (the new window's first Chainlink price)."""
    poll_s = settings.POLL_INTERVAL_MS / 1000
    w_ms = settings.CANDLE_WINDOW_MINUTES * 60_000
    to_open_s = (w_ms - (time.time() * 1000) % w_ms) / 1000 + 0.002
    if not _loop_wake.is_set():
        try:
            await asyncio.wait_for(_loop_wake.wait(), timeout=max(0.0, min(poll_s, to_open_s)))
        except asyncio.TimeoutError:
            pass
    _loop_wake.clear()


async def order_conn_keeper():
    """Keeps the Rust order sender's HTTPS connection to Polymarket open: a tiny request
    every ORDER_CONN_PING_S, and one 1.5 s before each window opens. Orders go out about
    once a window, so the connection used to be closed by then and every order first waited
    for a new one (DNS lookup, TCP and TLS handshakes: several round trips)."""
    last = 0.0
    pre_open_done = None
    while True:
        try:
            now = time.time()
            w = settings.CANDLE_WINDOW_MINUTES * 60.0
            next_open = (now // w + 1) * w
            pre_open_due = now >= next_open - 1.5 and pre_open_done != next_open
            if pre_open_due or now - last >= ORDER_CONN_PING_S:
                if settings.PRIVATE_KEY and getattr(clob_trader, "ready", False):
                    await asyncio.to_thread(clob_trader.keep_warm)
                last = time.time()
                if pre_open_due:
                    pre_open_done = next_open
                continue
            next_pre_open = next_open - 1.5 if pre_open_done != next_open else next_open + w - 1.5
            # sleep at most 1 s at a time, so a clock change can't make it miss an opening
            await asyncio.sleep(min(max(min(last + ORDER_CONN_PING_S, next_pre_open) - now, 0.01), 1.0))
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(5.0)

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()

    # Pre-seed cached spot price from last run if available
    if not state.get("last_spot_price"):
        cached_p = state.get("latest_data", {}).get("prices", {}).get("spot")
        if cached_p:
            state["last_spot_price"] = cached_p

    # Pre-seed full historical kline buffers so indicators are ready on very first render
    await seed_kline_buffers()

    # Start all background tasks
    tasks = [
        asyncio.create_task(binance_stream.start()),
        asyncio.create_task(binance_kline_1m.start()),
        asyncio.create_task(binance_kline_5m.start()),
        asyncio.create_task(polymarket_ws_stream.start()),
        asyncio.create_task(polymarket_clob_ws.start()),
        asyncio.create_task(chainlink_ws_stream.start()),
        asyncio.create_task(update_loop()),
        asyncio.create_task(telegram_poller()),
        asyncio.create_task(entry_watcher()),
        asyncio.create_task(order_conn_keeper()),
        asyncio.create_task(live_balance_updater())
    ]


    tasks.append(asyncio.create_task(ls_maker_mgr.run_forever()))  # PATCH: longshot maker
    tasks.append(asyncio.create_task(_or_watch_forever()))  # PATCH: official results
    tasks.append(asyncio.create_task(polybolt_feed.run_forever()))  # PATCH: price stream — it connects only while switched on
    log_message(polybolt.status_line(settings.PRICE_STREAM))   # PATCH: price stream
    log_message(_or_status_line())   # PATCH: official results
    log_message(longshot.status_line(settings.LONGSHOT))   # PATCH: longshot strategies — on (and what runs) or off


    # Pre-warm CLOB trader client & on-chain verification in background thread if private key set
    if settings.PRIVATE_KEY:
        async def _warm_live():
            try:
                await asyncio.to_thread(clob_trader.ensure_ready)
                if settings.RELAYER_API_KEY:
                    try:
                        setup_res = await asyncio.to_thread(clob_trader.ensure_setup)
                        log_message(f"Startup on-chain wallet verification: {setup_res}")
                    except Exception as e:
                        log_message(f"Startup on-chain wallet verification note: {e}")
            except Exception as e:
                log_message(f"Live trader pre-warm note: {e}")
        asyncio.create_task(_warm_live())

    import gc
    gc.collect()

    yield

    # PATCH: longshot maker — no maker bid stays on the book after the bot stops
    try:
        await asyncio.wait_for(ls_maker_mgr.cancel_all("bot shutting down"), timeout=10.0)
    except Exception:
        pass

    for task in tasks:
        task.cancel()

    binance_stream.close()
    binance_kline_1m.close()
    binance_kline_5m.close()
    polymarket_ws_stream.close()
    polymarket_clob_ws.close()
    chainlink_ws_stream.close()

app = FastAPI(title="Polymarket BTC 15m Assistant [GTD]", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        init_msg = json.dumps({
            "type": "state_update",
            "data": state["latest_data"],
            "logs": state["logs"][-30:],
            "log_seq": state["log_seq"],
        })
        await websocket.send_text(init_msg)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _ws_clients.discard(websocket)

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

@app.get("/settings", response_class=HTMLResponse)
async def get_settings_page(request: Request):
    return templates.TemplateResponse(request=request, name="settings.html")

@app.get("/api/latest")
async def get_latest():
    return state["latest_data"]

@app.get("/api/logs")
async def get_logs():
    if os.path.exists(LOG_FILE_PATH):
        try:
            with open(LOG_FILE_PATH, "r", encoding="utf-8", errors="ignore") as f:
                lines = [line.rstrip("\r\n") for line in f if line.strip()]
                if lines:
                    return lines
        except Exception:
            pass
    return state["logs"]

DOWNLOADABLE = {
    "signals": (SIGNALS_PATH, "text/csv"),
    "trades": (STATE_PATH, "application/json"),
    "logs": (LOG_FILE_PATH, "text/plain"),
}

@app.get("/api/files")
async def list_files():
    out = []
    for key, (path, _) in DOWNLOADABLE.items():
        exists = os.path.exists(path)
        out.append({
            "key": key,
            "name": os.path.basename(path),
            "exists": exists,
            "size": os.path.getsize(path) if exists else 0,
            "rows": (max(0, sum(1 for _ in open(path, encoding="utf-8", errors="ignore")) - 1)
                     if exists and path.endswith(".csv") else None),
            "modified": (datetime.fromtimestamp(os.path.getmtime(path)).isoformat()
                         if exists else None),
        })
    return out

@app.get("/api/download/{key}")
async def download_file(key: str):
    entry = DOWNLOADABLE.get(key)
    if not entry:
        return JSONResponse({"error": "unknown_file"}, status_code=404)
    path, media = entry
    if not os.path.exists(path):
        return JSONResponse({"error": "not_generated_yet", "path": path}, status_code=404)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    base, ext = os.path.splitext(os.path.basename(path))
    return FileResponse(path, media_type=media, filename=f"{settings.CANDLE_WINDOW_MINUTES}m-{base}-{stamp}{ext}")

def _reflect_running_now():
    ts = state["latest_data"].get("trading_state")
    if isinstance(ts, dict):
        ts["running"] = state["running"]

@app.post("/api/start")
async def start_trading():
    state["running"] = True
    _reflect_running_now()
    log_message("Trading STARTED by user")
    await send_telegram("🟢 *Trading STARTED by user*")
    await broadcast_state()
    return {"ok": True, "running": True}

@app.post("/api/stop")
async def stop_trading():
    state["running"] = False
    _reflect_running_now()
    for t in state.get("active_trades", []):
        if t.get("live_status") == "GTD_OPEN" and t.get("live_order_id"):
            log_message(f"Cancelling resting GTD order {t['live_order_id']} on trading stop")
            asyncio.create_task(asyncio.to_thread(clob_trader.cancel_order, t["live_order_id"]))
            t["live_status"] = "CANCELLED"
    asyncio.create_task(ls_maker_mgr.cancel_all("trading stopped"))  # PATCH: longshot maker
    log_message("Trading STOPPED by user")
    await send_telegram("🔴 *Trading STOPPED by user*")
    await broadcast_state()
    return {"ok": True, "running": False}

@app.get("/api/available-series")
async def get_available_series(timeframe: Optional[str] = None):
    try:
        return await data.fetch_available_series(timeframe=timeframe)
    except Exception as e:
        print(f"Error fetching available series: {e}")
        return [
            {"series_id": "10192", "title": "[15M] BTC Up or Down 15m", "slug": "btc-up-or-down-15m", "timeframe": "15m", "candle_window_minutes": 15},
            {"series_id": "10212", "title": "[15M] ETH Up or Down 15m", "slug": "eth-up-or-down-15m", "timeframe": "15m", "candle_window_minutes": 15},
            {"series_id": "10684", "title": "[5M] BTC Up or Down 5m", "slug": "btc-up-or-down-5m", "timeframe": "5m", "candle_window_minutes": 5},
            {"series_id": "10683", "title": "[5M] ETH Up or Down 5m", "slug": "eth-up-or-down-5m", "timeframe": "5m", "candle_window_minutes": 5},
        ]


@app.post("/api/switch-timeframe")
async def switch_timeframe(payload: Dict[str, Any]):
    target_tf = int(payload.get("timeframe", 15))
    if target_tf not in (5, 15):
        return {"status": "error", "error": "Invalid timeframe: must be 5 or 15"}

    old_tf = settings.CANDLE_WINDOW_MINUTES
    settings.CANDLE_WINDOW_MINUTES = target_tf

    series_15m_to_5m = {
        "10192": "10684",  # BTC
        "10212": "10683",  # ETH
        "10191": "10683",  # ETH
        "10423": "10686",  # SOL
        "10422": "10685",  # XRP
        "11330": "11326",  # BNB
        "11328": "11325",  # DOGE
        "11329": "11327",  # HYPE
        "12518": "12517",  # ZEC
    }
    series_5m_to_15m = {v: k for k, v in series_15m_to_5m.items()}

    sym = (settings.SYMBOL or "BTCUSDT").upper()
    asset = sym.replace("USDT", "").replace("USD", "").lower()
    if target_tf == 5:
        if settings.POLYMARKET_SERIES_ID in series_15m_to_5m:
            settings.POLYMARKET_SERIES_ID = series_15m_to_5m[settings.POLYMARKET_SERIES_ID]
        elif not settings.POLYMARKET_SERIES_ID or settings.POLYMARKET_SERIES_ID == "10192":
            settings.POLYMARKET_SERIES_ID = "10684"
        settings.POLYMARKET_SERIES_SLUG = f"{asset}-up-or-down-5m"
    else:
        if settings.POLYMARKET_SERIES_ID in series_5m_to_15m:
            settings.POLYMARKET_SERIES_ID = series_5m_to_15m[settings.POLYMARKET_SERIES_ID]
        elif not settings.POLYMARKET_SERIES_ID or settings.POLYMARKET_SERIES_ID == "10684":
            settings.POLYMARKET_SERIES_ID = "10192"
        settings.POLYMARKET_SERIES_SLUG = f"{asset}-up-or-down-15m"

    state["trade_ctx"] = None
    state["market_opens"].clear()
    state["last_window_start"] = None
    state["ls_ctx"] = None
    global _cached_market, _cached_market_ts
    _cached_market = None
    _cached_market_ts = 0.0
    _next_market.update(start_ms=None, market=None, tokens=[], series=None)
    _loop_wake.set()

    cfg_file = CONFIG_PATH if os.path.exists(CONFIG_PATH) else "config.json"
    try:
        cfg = {}
        if os.path.exists(cfg_file):
            with open(cfg_file, "r") as f:
                cfg = json.load(f)
        cfg.setdefault("trading", {})["candle_window_minutes"] = target_tf
        cfg.setdefault("polymarket", {})["series_id"] = settings.POLYMARKET_SERIES_ID
        if settings.POLYMARKET_SERIES_SLUG:
            cfg.setdefault("polymarket", {})["series_slug"] = settings.POLYMARKET_SERIES_SLUG
        with open(cfg_file, "w") as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"Error persisting timeframe switch to config.json: {e}")

    log_message(f"Market timeframe switched to {target_tf}m (Polymarket Series ID: {settings.POLYMARKET_SERIES_ID}).")
    _reflect_running_now()
    if "trading_state" in state.get("latest_data", {}):
        state["latest_data"]["trading_state"]["candle_window_minutes"] = target_tf
    await broadcast_state()
    return {"status": "ok", "candle_window_minutes": target_tf, "series_id": settings.POLYMARKET_SERIES_ID}


@app.get("/api/telegram-subscribers")
async def get_telegram_subscribers():
    subs = state.get("telegram_subscribers", [])
    normalized = []
    for s in subs:
        norm = _normalize_subscriber(s)
        if norm:
            normalized.append(norm)
    return {"ok": True, "subscribers": normalized}

@app.post("/api/telegram-unsubscribe")
async def post_telegram_unsubscribe(req: Dict[str, Any]):
    chat_id = req.get("chat_id")
    if chat_id is not None:
        try:
            remove_telegram_subscriber(int(chat_id))
        except (ValueError, TypeError):
            pass
    return {"ok": True}

@app.post("/api/test-telegram")
async def post_test_telegram(req: Optional[Dict[str, Any]] = None):
    req = req or {}
    token = (req.get("bot_token") or "").strip()
    if not token or "..." in token or token.lower() == "set":
        token = settings.TELEGRAM_BOT_TOKEN
    
    if not token:
        return {"ok": False, "error": "No bot token provided. Enter your Telegram Bot Token from @BotFather."}
    
    url = f"https://api.telegram.org/bot{token}/getMe"
    proxy = ws_data.get_proxy_url_for(url)
    try:
        async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=8.0) as client:
            resp = await client.get(url)
            if resp.status_code == 401:
                return {
                    "ok": False,
                    "error": "Invalid Bot Token (401 Unauthorized from Telegram). Please verify the token from @BotFather."
                }
            if resp.status_code != 200:
                return {
                    "ok": False,
                    "error": f"Telegram API error (HTTP {resp.status_code}): {resp.text}"
                }
            bot_info = resp.json().get("result", {})
            username = bot_info.get("username", "bot")
            bot_name = bot_info.get("first_name", "Bot")
    except Exception as e:
        return {"ok": False, "error": f"Failed to connect to Telegram API: {e}"}

    subs = state.get("telegram_subscribers", [])
    if not subs:
        return {
            "ok": True,
            "count": 0,
            "bot_username": username,
            "message": f"Connected as @{username} ({bot_name})! No subscribers yet — open https://t.me/{username} and send /start to subscribe."
        }

    sent_count = 0
    errors = []
    send_url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=8.0) as client:
        for s in subs:
            cid = s.get("chat_id") if isinstance(s, dict) else s
            if not cid:
                continue
            try:
                s_resp = await client.post(send_url, json={
                    "chat_id": cid,
                    "text": f"🔔 *Test alert from Polymarket Assistant!*\nBot @{username} is connected and operational.",
                    "parse_mode": "Markdown"
                })
                if s_resp.status_code == 200:
                    sent_count += 1
                else:
                    errors.append(f"Chat {cid}: HTTP {s_resp.status_code}")
            except Exception as e:
                errors.append(f"Chat {cid}: {e}")

    if sent_count > 0:
        return {
            "ok": True,
            "count": sent_count,
            "bot_username": username,
            "message": f"Connected as @{username}! Sent test alert to {sent_count} subscriber(s)."
        }
    else:
        err_msg = ", ".join(errors) if errors else "failed to send message"
        return {
            "ok": False,
            "bot_username": username,
            "error": f"Connected as @{username}, but failed to deliver to subscribers: {err_msg}"
        }

@app.get("/api/settings")
async def get_settings():
    def mask(v: str) -> str:
        return v[:6] + "..." + v[-4:] if v and len(v) > 10 else v

    masked_pk = mask(settings.PRIVATE_KEY)

    return {
        "has_live_creds": bool(settings.PRIVATE_KEY),
        "private_key": masked_pk,
        "live": {
            "relayer_api_key": mask(settings.RELAYER_API_KEY),
            "alchemy_api_key": mask(settings.ALCHEMY_API_KEY),
            "funder_address": settings.FUNDER_ADDRESS,
            "order_signer": getattr(settings, "ORDER_SIGNER", "auto") or "auto",
            "max_slippage": settings.CLOB_MAX_SLIPPAGE,
            "exit_max_retries": settings.EXIT_MAX_RETRIES,
            "allow_partial_fill": settings.CLOB_ALLOW_PARTIAL_FILL
        },
        "copy_fak": copy_fak.merged(settings.COPY_FAK),   # PATCH: fill-and-kill copy
        "price_stream": polybolt.merged(settings.PRICE_STREAM),   # PATCH: price stream
        "polymarket": {
            "series_id": settings.POLYMARKET_SERIES_ID,
            "series_slug": getattr(settings, "POLYMARKET_SERIES_SLUG", ""),
            "gamma_base_url": settings.GAMMA_BASE_URL,
            "clob_base_url": settings.CLOB_BASE_URL,
            "live_ws_url": settings.POLYMARKET_LIVE_DATA_WS_URL,
            "up_label": settings.POLYMARKET_UP_LABEL,
            "down_label": settings.POLYMARKET_DOWN_LABEL
        },
        "trading": {
            "symbol": settings.SYMBOL or "BTCUSDT",
            "candle_window_minutes": settings.CANDLE_WINDOW_MINUTES,
            "risk_type": settings.RISK_TYPE,
            "risk_value": settings.RISK_VALUE
        },
        "ev": {
            "ev_threshold": settings.EV_THRESHOLD,
            "min_prob": settings.MIN_PROB_EV,
            "min_book_liquidity_usd": settings.MIN_BOOK_LIQUIDITY_USD,
            "max_book_age_s": settings.MAX_BOOK_AGE_S
        },
        "flip": {
            "enabled": settings.FLIP_ENABLED,
            "min_conviction": settings.FLIP_MIN_CONVICTION,
            "min_minutes_left": settings.FLIP_MIN_MINUTES_LEFT
        },
        "capital_extractor": {
            "enabled": settings.AUTO_WITHDRAW_ENABLED,
            "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
            "withdraw_amount": settings.WITHDRAW_AMOUNT,
            "recipient_address": settings.WITHDRAW_ADDRESS,
            "withdraw_address": settings.WITHDRAW_ADDRESS,
            "auto_resume": settings.WITHDRAW_AUTO_RESUME,
            "auto_resume_after_withdrawal": settings.WITHDRAW_AUTO_RESUME,
            "resume_after": settings.WITHDRAW_RESUME_AFTER,
            "default_destination": (clob_trader.get_eoa_address() if clob_trader else "") or ""
        },
        "telegram": {
            "enabled": settings.TELEGRAM_ENABLED,
            "bot_token": mask(settings.TELEGRAM_BOT_TOKEN)
        },
        # PATCH: longshot
        "longshot": settings.LONGSHOT,
        "settlement": {
            "authoritative_settle_wait_s": settings.AUTHORITATIVE_SETTLE_WAIT_S,
            "redeem_after_resolution": _or_redeem_on(),   # PATCH: official results
            "confirm_official": _or_confirm_on(),          # PATCH: official results
        },
    }

@app.post("/api/settings")
async def post_settings(new_settings: Dict[str, Any]):
    global binance_stream, polymarket_ws_stream, chainlink_ws_stream, binance_kline_1m, binance_kline_5m
    old_symbol = settings.SYMBOL
    _ps_new = None   # PATCH: price stream — its settings are checked before anything is saved or changed
    if "price_stream" in new_settings:
        try:
            _ps_new = polybolt.normalize(new_settings["price_stream"], settings.PRICE_STREAM)
        except ValueError as e:
            return {"status": "error", "error": f"invalid_settings: {e}"}
        new_settings["price_stream"] = _ps_new
    # PATCH: fill-and-kill copy — its settings are checked before anything is saved or changed
    _fk_new = None
    if "copy_fak" in new_settings:
        try:
            _fk_new = copy_fak.normalize(new_settings["copy_fak"], settings.COPY_FAK)
        except ValueError as e:
            return {"status": "error", "error": f"invalid_settings: {e}"}
        new_settings["copy_fak"] = _fk_new

    lv_in = new_settings.get("live") if isinstance(new_settings.get("live"), dict) else None
    if _fk_new is not None and "order_type" in _fk_new:
        settings.CLOB_ALLOW_PARTIAL_FILL = (_fk_new["order_type"] == "FAK")
        if lv_in is not None:
            lv_in["allow_partial_fill"] = settings.CLOB_ALLOW_PARTIAL_FILL
    elif lv_in is not None and "allow_partial_fill" in lv_in:
        ot = "FAK" if lv_in["allow_partial_fill"] else "FOK"
        if _fk_new is None:
            _fk_new = dict(copy_fak.merged(settings.COPY_FAK))
        _fk_new["order_type"] = ot
        new_settings["copy_fak"] = _fk_new
        settings.CLOB_ALLOW_PARTIAL_FILL = bool(lv_in["allow_partial_fill"])

    if lv_in is not None and "max_slippage" in lv_in:
        try:
            slip_val = float(lv_in["max_slippage"])
            settings.CLOB_MAX_SLIPPAGE = slip_val
            if _fk_new is None:
                _fk_new = dict(copy_fak.merged(settings.COPY_FAK))
            if "cap_cents" not in (new_settings.get("copy_fak") or {}):
                _fk_new["cap_cents"] = round(slip_val * 100.0, 2)
                _fk_new["high_cap_cents"] = round(slip_val * 100.0, 2)
                new_settings["copy_fak"] = _fk_new
        except Exception:
            pass
    if _fk_new is not None and "cap_cents" in _fk_new:
        try:
            cap_val = float(_fk_new["cap_cents"])
            if lv_in is not None and "max_slippage" not in (new_settings.get("live") or {}):
                lv_in["max_slippage"] = round(cap_val / 100.0, 4)
                settings.CLOB_MAX_SLIPPAGE = lv_in["max_slippage"]
            elif lv_in is None:
                settings.CLOB_MAX_SLIPPAGE = round(cap_val / 100.0, 4)
        except Exception:
            pass

    _new_signer = None
    if lv_in is not None and "order_signer" in lv_in:
        from bot.config import normalize_order_signer
        v = normalize_order_signer(lv_in.get("order_signer"))
        if not v:
            return {"status": "error", "error": "live.order_signer must be auto, wallet or eoa"}
        lv_in["order_signer"] = v
        _new_signer = v

    def _merge_into(base, override):
        out = json.loads(json.dumps(base))
        for k, v in (override or {}).items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k] = _merge_into(out[k], v)
            else:
                out[k] = v
        return out

    new_ls = None
    try:
        if "longshot" in new_settings:
            if not isinstance(new_settings["longshot"], dict):
                raise ValueError("longshot must be an object")
            new_ls = longshot.normalize(_merge_into(settings.LONGSHOT, new_settings["longshot"]))
            new_settings["longshot"] = new_ls
        if isinstance(new_settings.get("settlement"), dict):   # PATCH: official results — true or false, or the save is refused
            for _k in ("redeem_after_resolution", "confirm_official"):
                if _k in new_settings["settlement"]:
                    new_settings["settlement"][_k] = longshot.as_bool(new_settings["settlement"][_k], "settlement." + _k)
    except ValueError as e:
        return {"status": "error", "error": f"invalid_settings: {e}"}

    # (PATCH: longshot — the checks above run before anything is applied, so a rejected save
    # changes nothing: not the private key, not the order signer)
    new_pk = new_settings.get("private_key")
    if new_pk and "..." in new_pk:
        new_settings["private_key"] = settings.PRIVATE_KEY
    elif new_pk:
        from bot.config import normalize_private_key
        try:
            _pk = normalize_private_key(new_pk)
        except Exception as e:
            return {"status": "error", "error": f"invalid_private_key: {e}"}
        settings.PRIVATE_KEY = _pk
        new_settings["private_key"] = settings.PRIVATE_KEY
    if _new_signer:
        settings.ORDER_SIGNER = _new_signer



    cfg_file = CONFIG_PATH if os.path.exists(CONFIG_PATH) else "config.json"
    existing_cfg = {}
    if os.path.exists(cfg_file):
        try:
            with open(cfg_file, "r") as f:
                existing_cfg = json.load(f)
        except Exception:
            existing_cfg = {}

    def deep_merge(base, override):
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                deep_merge(base[k], v)
            else:
                base[k] = v
        return base

    # Pre-sync live relayer & alchemy keys into config dictionaries before dumping to disk
    if "live" in new_settings:
        lv = new_settings["live"]
        rk = lv.get("relayer_api_key")
        if rk and "..." not in rk:
            settings.RELAYER_API_KEY = rk
            new_settings.setdefault("relayer", {})["api_key"] = rk
            existing_cfg.setdefault("relayer", {})["api_key"] = rk
        elif rk:
            lv["relayer_api_key"] = settings.RELAYER_API_KEY
        fa = lv.get("funder_address") if "funder_address" in lv else new_settings.get("funder_address")
        if fa is not None:
            from bot.config import normalize_funder_address
            settings.FUNDER_ADDRESS = normalize_funder_address(fa)
            lv["funder_address"] = settings.FUNDER_ADDRESS
            existing_cfg.setdefault("live", {})["funder_address"] = settings.FUNDER_ADDRESS
            existing_cfg["funder_address"] = settings.FUNDER_ADDRESS
        ak = lv.get("alchemy_api_key")
        if ak and "..." not in ak:
            settings.ALCHEMY_API_KEY = ak
            new_settings.setdefault("chainlink", {})["alchemy_api_key"] = ak
            existing_cfg.setdefault("chainlink", {})["alchemy_api_key"] = ak
        elif ak:
            lv["alchemy_api_key"] = settings.ALCHEMY_API_KEY

    merged_cfg = deep_merge(existing_cfg, new_settings)
    if settings.RELAYER_API_KEY:
        merged_cfg.setdefault("relayer", {})["api_key"] = settings.RELAYER_API_KEY
    if settings.ALCHEMY_API_KEY:
        merged_cfg.setdefault("chainlink", {})["alchemy_api_key"] = settings.ALCHEMY_API_KEY
    if settings.FUNDER_ADDRESS:
        merged_cfg.setdefault("live", {})["funder_address"] = settings.FUNDER_ADDRESS
        merged_cfg["funder_address"] = settings.FUNDER_ADDRESS

    # Remove obsolete keys if present in file
    merged_cfg.pop("mode", None)
    merged_cfg.pop("paper_balance_usd", None)
    merged_cfg.pop("redeem_sweep", None)
    merged_cfg.pop("price_stream", None)
    if "longshot" in merged_cfg and isinstance(merged_cfg["longshot"], dict):
        for k in ("enabled", "live", "max_attempts", "resting_s", "slippage", "cutoff_s",
                  "min_ask_liquidity_usd", "max_price_age_s", "vol_usd_per_sqrt_s", "vol_fallback_usd_per_sqrt_s",
                  "sd_floor_usd", "taker_fee_rate", "model", "price_to_beat", "pick", "stake_type", "stake_value", "growth_cap_pct"):
            merged_cfg["longshot"].pop(k, None)
        if "maker" in merged_cfg["longshot"] and isinstance(merged_cfg["longshot"]["maker"], dict):
            for k in ("enabled", "live", "min_prob", "max_ask", "look_s", "cutoff_s", "max_fills_per_window", "requote_s", "poll_s",
                      "stake_type", "stake_value", "growth_cap_pct"):
                merged_cfg["longshot"]["maker"].pop(k, None)
        if "bands" in merged_cfg["longshot"] and isinstance(merged_cfg["longshot"]["bands"], dict):
            merged_cfg["longshot"]["bands"].pop("favored", None)
            for b in merged_cfg["longshot"]["bands"].values():
                if isinstance(b, dict):
                    for k in ("enabled", "look_s", "min_prob", "min_multiple", "stake_type", "stake_value", "growth_cap_pct"):
                        b.pop(k, None)

    if "trading" in merged_cfg and isinstance(merged_cfg["trading"], dict):
        raw_sym = str(merged_cfg["trading"].get("symbol") or "").strip().upper()
        merged_cfg["trading"]["symbol"] = raw_sym if raw_sym else (settings.SYMBOL or "BTCUSDT")

    with open(CONFIG_PATH, "w") as f:
        json.dump(merged_cfg, f, indent=2)
    if _ps_new is not None:   # PATCH: price stream — in use at once (the stream connects or lets go within seconds)
        if _ps_new != polybolt.merged(settings.PRICE_STREAM):
            log_message(polybolt.status_line(_ps_new))
        settings.PRICE_STREAM = _ps_new
    if _fk_new is not None:   # PATCH: fill-and-kill copy — in use from the next trade
        if _fk_new != copy_fak.merged(settings.COPY_FAK):
            log_message(copy_fak.status_line(_fk_new))
        settings.COPY_FAK = _fk_new

    # Apply the checked sections
    if new_ls is not None:
        settings.LONGSHOT = new_ls
        log_message(longshot.status_line(new_ls))   # PATCH: longshot strategies
    if "settlement" in new_settings and isinstance(new_settings["settlement"], dict):
        if "authoritative_settle_wait_s" in new_settings["settlement"]:
            settings.AUTHORITATIVE_SETTLE_WAIT_S = float(new_settings["settlement"]["authoritative_settle_wait_s"])
        for _k, _attr in (("redeem_after_resolution", "REDEEM_AFTER_RESOLUTION"), ("confirm_official", "CONFIRM_OFFICIAL")):   # PATCH: official results
            if _k in new_settings["settlement"]:
                setattr(settings, _attr, bool(new_settings["settlement"][_k]))
        log_message(_or_status_line())   # PATCH: official results

    ct = merged_cfg.get("copy_trader", {})
    if ct:
        if "retry_interval_ms" in ct: settings.COPY_RETRY_INTERVAL_MS = int(ct["retry_interval_ms"])
        if "max_retries" in ct: settings.COPY_MAX_RETRIES = int(ct["max_retries"])

    if "trading" in new_settings:
        t = new_settings["trading"]
        raw_sym = str(t.get("symbol") or "").strip().upper()
        settings.SYMBOL = raw_sym if raw_sym else (settings.SYMBOL or "BTCUSDT")
        settings.RISK_TYPE = t.get("risk_type", settings.RISK_TYPE)
        settings.RISK_VALUE = float(t.get("risk_value", settings.RISK_VALUE))
        if "candle_window_minutes" in t:
            try:
                new_cwm = int(t["candle_window_minutes"])
                if new_cwm in (5, 15) and new_cwm != settings.CANDLE_WINDOW_MINUTES:
                    old_cwm = settings.CANDLE_WINDOW_MINUTES
                    settings.CANDLE_WINDOW_MINUTES = new_cwm
                    sym = (settings.SYMBOL or "BTCUSDT").upper()
                    asset = sym.replace("USDT", "").replace("USD", "").lower()
                    settings.POLYMARKET_SERIES_ID = _series_for_tf(new_cwm)
                    settings.POLYMARKET_SERIES_SLUG = f"{asset}-up-or-down-{new_cwm}m"
                    state["trade_ctx"] = None
                    state["market_opens"].clear()
                    state["last_window_start"] = None
                    state["ls_ctx"] = None
                    _cached_markets_by_tf.clear()
                    _cached_market = None
                    _cached_market_ts = 0.0
                    _next_market.update(start_ms=None, market=None, tokens=[], series=None)
                    _loop_wake.set()
                    log_message(f"Market window timeframe switched from {old_cwm}m to {new_cwm}m.")
            except Exception:
                pass

    if "ev" in new_settings:
        e = new_settings["ev"]
        settings.EV_THRESHOLD = float(e.get("ev_threshold", settings.EV_THRESHOLD))
        settings.MIN_PROB_EV = float(e.get("min_prob", settings.MIN_PROB_EV))
        settings.MIN_BOOK_LIQUIDITY_USD = float(e.get("min_book_liquidity_usd", settings.MIN_BOOK_LIQUIDITY_USD))
        if "max_book_age_s" in e:
            settings.MAX_BOOK_AGE_S = float(e["max_book_age_s"])

    if "flip" in new_settings:
        f = new_settings["flip"]
        if "enabled" in f:
            settings.FLIP_ENABLED = bool(f["enabled"])
        settings.FLIP_MIN_CONVICTION = float(f.get("min_conviction", settings.FLIP_MIN_CONVICTION))
        settings.FLIP_MIN_MINUTES_LEFT = float(f.get("min_minutes_left", settings.FLIP_MIN_MINUTES_LEFT))

    if "polymarket" in new_settings:
        p = new_settings["polymarket"]
        old_series = settings.POLYMARKET_SERIES_ID
        settings.POLYMARKET_SERIES_ID = str(p.get("series_id", settings.POLYMARKET_SERIES_ID))
        if "series_slug" in p:
            settings.POLYMARKET_SERIES_SLUG = str(p["series_slug"])
        settings.POLYMARKET_UP_LABEL = p.get("up_label", settings.POLYMARKET_UP_LABEL)
        settings.POLYMARKET_DOWN_LABEL = p.get("down_label", settings.POLYMARKET_DOWN_LABEL)
        if settings.POLYMARKET_SERIES_ID != old_series:
            state["trade_ctx"] = None
            _cached_market = None
            _cached_market_ts = 0.0
            _next_market.update(start_ms=None, market=None, tokens=[], series=None)
            _loop_wake.set()

    if "live" in new_settings:
        lv = new_settings["live"]
        if "max_slippage" in lv:
            settings.CLOB_MAX_SLIPPAGE = float(lv["max_slippage"])
        if "exit_max_retries" in lv:
            settings.EXIT_MAX_RETRIES = int(lv["exit_max_retries"])
        if "allow_partial_fill" in lv:
            settings.CLOB_ALLOW_PARTIAL_FILL = bool(lv["allow_partial_fill"])

        rk = lv.get("relayer_api_key")
        if rk and "..." not in rk:
            settings.RELAYER_API_KEY = rk
            new_settings.setdefault("relayer", {})["api_key"] = rk
        elif rk:
            lv["relayer_api_key"] = settings.RELAYER_API_KEY
        ak = lv.get("alchemy_api_key")
        if ak and "..." not in ak:
            settings.ALCHEMY_API_KEY = ak
            new_settings.setdefault("chainlink", {})["alchemy_api_key"] = ak
        elif ak:
            lv["alchemy_api_key"] = settings.ALCHEMY_API_KEY

    if "capital_extractor" in new_settings:
        ce = new_settings["capital_extractor"]
        if "enabled" in ce: settings.AUTO_WITHDRAW_ENABLED = bool(ce["enabled"])
        if "trigger_balance" in ce: settings.WITHDRAW_TRIGGER_BALANCE = float(ce["trigger_balance"])
        if "withdraw_amount" in ce: settings.WITHDRAW_AMOUNT = float(ce["withdraw_amount"])
        recip = ce.get("recipient_address") if "recipient_address" in ce else ce.get("withdraw_address")
        if recip is not None:
            recip_str = str(recip).strip()
            # If browser autofill sent "admin" or invalid string, sanitize to blank
            if recip_str and (not recip_str.startswith("0x") or len(recip_str) != 42):
                recip_str = ""
            settings.WITHDRAW_ADDRESS = recip_str
            ce["recipient_address"] = recip_str
            ce["withdraw_address"] = recip_str
        auto_res = ce.get("auto_resume") if "auto_resume" in ce else ce.get("auto_resume_after_withdrawal")
        if auto_res is not None:
            settings.WITHDRAW_AUTO_RESUME = bool(auto_res)
            ce["auto_resume"] = settings.WITHDRAW_AUTO_RESUME
        if "resume_after" in ce: settings.WITHDRAW_RESUME_AFTER = str(ce["resume_after"]).strip()

    if "telegram" in new_settings:
        tg = new_settings["telegram"]
        if "enabled" in tg: settings.TELEGRAM_ENABLED = bool(tg["enabled"])
        tok = tg.get("bot_token")
        if tok and "..." not in tok and tok.lower() != "set":
            settings.TELEGRAM_BOT_TOKEN = str(tok).strip()

    clob_trader.reset()

    if settings.SYMBOL != old_symbol:
        binance_stream.close()
        binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL, on_update=_wake_entry)
        asyncio.create_task(binance_stream.start())

        binance_kline_1m.close()
        binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
        asyncio.create_task(binance_kline_1m.start())

        binance_kline_5m.close()
        binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)
        asyncio.create_task(binance_kline_5m.start())

        await seed_kline_buffers()

        polymarket_ws_stream.close()
        polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
            ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
            symbol_includes=get_ws_symbol_filter(settings.SYMBOL),
            on_update=_on_poly_chainlink_tick
        )
        longshot.clear_samples()  # PATCH: longshot — old symbol's prices are useless
        polybolt_feed.set_symbol(polybolt.symbol_for(settings.SYMBOL))   # PATCH: price stream — it follows the bot's symbol
        asyncio.create_task(polymarket_ws_stream.start())

        chainlink_ws_stream.close()
        chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL), on_update=_on_chainlink_tick)
        asyncio.create_task(chainlink_ws_stream.start())

    # PATCH: fill-and-kill copy — saving resets the live client (above) and the next order would
    # set it up again first (several network calls). With the one-order copy on, set it up now.
    if settings.PRIVATE_KEY and copy_fak.on(settings.COPY_FAK):
        asyncio.create_task(asyncio.to_thread(clob_trader.ensure_ready))
    await broadcast_state()
    return {"status": "ok"}

@app.post("/api/setup-wallet")
async def setup_wallet(body: Optional[Dict[str, Any]] = None):
    body = body or {}
    pk = body.get("private_key")
    if pk and "..." not in pk:
        from bot.config import normalize_private_key
        try:
            settings.PRIVATE_KEY = normalize_private_key(pk)
        except Exception:
            pass
    rk = body.get("relayer_api_key")
    if rk and "..." not in rk:
        settings.RELAYER_API_KEY = rk
    ak = body.get("alchemy_api_key")
    if ak and "..." not in ak:
        settings.ALCHEMY_API_KEY = ak
    fa = body.get("funder_address")
    if fa is not None and "..." not in str(fa):
        from bot.config import normalize_funder_address
        settings.FUNDER_ADDRESS = normalize_funder_address(fa)
    clob_trader.reset()
    try:
        result = await asyncio.to_thread(clob_trader.ensure_setup)
        if result.get("ok"):
            if result.get("skipped"):
                log_message("Wallet setup: already done this session")
            else:
                log_message(f"Wallet setup complete ({result.get('approvals', 0)} approvals)")
        else:
            log_message(f"Wallet setup failed: {result.get('error')}")
    except Exception as e:
        log_message(f"Wallet setup error: {e}")
        result = {"ok": False, "error": str(e)}

    eoa_addr = clob_trader.get_eoa_address() if clob_trader else None
    if not eoa_addr and settings.PRIVATE_KEY:
        try:
            from eth_account import Account
            eoa_addr = Account.from_key(settings.PRIVATE_KEY).address
        except Exception:
            pass
    result["eoa"] = eoa_addr
    result["funder"] = (clob_trader.get_funder_address() if clob_trader else None) or eoa_addr
    return result

@app.post("/api/test-connection")
async def test_connection(body: Optional[Dict[str, Any]] = None):
    body = body or {}
    pk = body.get("private_key")
    if pk and "..." not in pk:
        from bot.config import normalize_private_key
        try:
            settings.PRIVATE_KEY = normalize_private_key(pk)
        except Exception:
            pass
    rk = body.get("relayer_api_key")
    if rk and "..." not in rk:
        settings.RELAYER_API_KEY = rk
    ak = body.get("alchemy_api_key")
    if ak and "..." not in ak:
        settings.ALCHEMY_API_KEY = ak
    fa = body.get("funder_address")
    if fa is not None and "..." not in str(fa):
        from bot.config import normalize_funder_address
        settings.FUNDER_ADDRESS = normalize_funder_address(fa)

    # Persist tested credentials to config.json so they survive server restarts
    cfg_file = CONFIG_PATH if os.path.exists(CONFIG_PATH) else "config.json"
    try:
        cfg = {}
        if os.path.exists(cfg_file):
            with open(cfg_file, "r") as f:
                cfg = json.load(f)
        changed = False
        if pk and "..." not in str(pk):
            cfg["private_key"] = settings.PRIVATE_KEY
            changed = True
        if rk and "..." not in str(rk):
            cfg.setdefault("relayer", {})["api_key"] = settings.RELAYER_API_KEY
            changed = True
        if ak and "..." not in str(ak):
            cfg.setdefault("chainlink", {})["alchemy_api_key"] = settings.ALCHEMY_API_KEY
            changed = True
        if fa is not None and "..." not in str(fa):
            cfg.setdefault("live", {})["funder_address"] = settings.FUNDER_ADDRESS
            cfg["funder_address"] = settings.FUNDER_ADDRESS
            changed = True
        if changed:
            with open(cfg_file, "w") as f:
                json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"Warning: Failed to persist keys in test_connection: {e}")

    clob_trader.reset()
    try:
        result = await asyncio.to_thread(clob_trader.test_connection)
        if result.get("ok"):
            log_message(f"Connection OK — EOA {result.get('eoa')}, trading from "
                        f"{result.get('funder')} (sig type {result.get('chosen_signature_type')}, "
                        f"{result.get('wallet_source') or 'auto'})")
            if result.get("wallet_warning"):
                log_message(result["wallet_warning"])
        else:
            log_message(f"Connection test: {result.get('error')}")
    except Exception as e:
        result = {"ok": False, "error": str(e)}

    # Always guarantee EOA address resolution if key is present
    eoa_addr = result.get("eoa") or (clob_trader.get_eoa_address() if clob_trader else None)
    if not eoa_addr and settings.PRIVATE_KEY:
        try:
            from eth_account import Account
            eoa_addr = Account.from_key(settings.PRIVATE_KEY).address
            result["eoa"] = eoa_addr
        except Exception:
            pass

    # Resolve withdrawal destination details
    withdraw_addr = (body.get("withdraw_address") or settings.WITHDRAW_ADDRESS or "").strip()
    is_valid_eth = bool(withdraw_addr.startswith("0x") and len(withdraw_addr) == 42)
    is_blank = not bool(withdraw_addr) or not is_valid_eth
    dest = eoa_addr if is_blank else withdraw_addr

    result["withdraw_address"] = withdraw_addr
    result["withdraw_destination"] = dest
    result["withdraw_is_eoa"] = is_blank
    result["withdraw_enabled"] = bool(body.get("withdraw_enabled", settings.AUTO_WITHDRAW_ENABLED))
    result["withdraw_amount"] = float(body.get("withdraw_amount", settings.WITHDRAW_AMOUNT))
    result["withdraw_trigger"] = float(body.get("withdraw_trigger", settings.WITHDRAW_TRIGGER_BALANCE))

    return result

@app.post("/api/enable-auto-redeem")
async def enable_auto_redeem(body: Optional[Dict[str, Any]] = None):
    body = body or {}
    pk = body.get("private_key")
    if pk and "..." not in pk:
        from bot.config import normalize_private_key
        try:
            settings.PRIVATE_KEY = normalize_private_key(pk)
        except Exception:
            pass
    clob_trader.reset()
    try:
        result = await asyncio.to_thread(clob_trader.enable_auto_redeem)
        log_message("Auto-redeem enabled" if result.get("ok")
                    else f"Auto-redeem failed: {result.get('error')}")
    except Exception as e:
        result = {"ok": False, "error": str(e)}

    eoa_addr = clob_trader.get_eoa_address() if clob_trader else None
    if not eoa_addr and settings.PRIVATE_KEY:
        try:
            from eth_account import Account
            eoa_addr = Account.from_key(settings.PRIVATE_KEY).address
        except Exception:
            pass
    result["eoa"] = eoa_addr
    result["funder"] = (clob_trader.get_funder_address() if clob_trader else None) or eoa_addr
    return result

@app.get("/health")
async def health():
    return {"status": "ok", "last_update": state["last_update_ts"], "mode": state["trading_mode"],
            "running": state["running"]}

@app.get("/history")
async def get_history():
    return state["trade_history"]

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8017))
    uvicorn.run(app, host="0.0.0.0", port=port)
