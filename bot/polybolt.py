"""
Polymarket's price stream (PATCH: price stream; DO_IT_YOURSELVES.md part C).

Polymarket's own reference prices over wss://ws-live-v2.polymarket.com/ws:
  price.crypto.twap   the 60-second average of Chainlink's price, which the 15-minute markets are decided on
  price.crypto        the live reference price (kept only while its source is "chainlink")
Both need the CLOB API credentials the bot already derives from its key. They go to that socket in the
login frame and nowhere else, and are never logged.

Protocol (docs.polymarket.com, PolyBolt WebSocket):
  -> {"op": "auth", "rid": "a1", "auth": {"apiKey", "secret", "passphrase"}}          <- {"op": "authed"}
  -> {"op": "subscribe", "rid": "s1", "subscriptions": [{"channel", "filter"}, ...]}   <- {"op": "subscribed", "channel"}
  <- {"v", "channel", "seq", "ts", "snapshot"?, "dropped"?, "payload"}: a snapshot has its points in
     payload["data"] (about the last two minutes), an update is one point. A point has "timestamp" (unix ms),
     "value" and the exact "full_accuracy_value"; "source" names the vendor.
  <- {"op": "error", "code": "auth_required" | "auth_invalid" | "auth_unavailable" | "bad_filter" | ...}
  Close 4003 = server draining (reconnect after a random 0-10 s), 4001 = credentials refused,
  4008 = a limit or a bad request, 4002 = slow consumer or missed pongs.
  The server pings every 25 s; aiohttp answers those by itself.

Settings: config.json "price_stream" (DEFAULTS below). Nothing in this file places orders. main.py asks it
for numbers (twap_at, live, ticks) and decides what to do with them.
"""
import asyncio
import bisect
import json
import random
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

URL = "wss://ws-live-v2.polymarket.com/ws"
SYMBOL = "btcusd"
KEEP_TWAP_S = 3600.0       # published averages kept (a window's open is still needed when it is booked)
KEEP_TICKS_S = 900.0       # live prices kept
CONNECT_S = 15.0           # no answer to the connection for this long: give up and try again
STALL_S = 15.0             # no frame at all for this long on a connection: reconnect
DRAIN_MAX_DELAY_S = 10.0   # close 4003: reconnect after a random delay up to this
REFUSED_WAIT_S = 60.0      # credentials refused, or a limit / a refused request: wait this long before the next try,
REFUSED_MAX_WAIT_S = 900.0 # twice as long after each further refusal, up to this
NO_KEY_WAIT_S = 30.0       # the bot has no API credentials yet (no key, or its order client isn't set up): look again after this
IDLE_S = 2.0               # switched off: look at the switch again after this
FIRST_BACKOFF_S = 1.0      # a dropped connection: wait this long, then twice as long each time ...
MAX_BACKOFF_S = 30.0       # ... up to this, each with a random extra of up to the same again
FRESH_S = 5.0              # view(): both numbers arrived within this = "fresh"
SAY_AGAIN_S = 600.0        # the same trouble is logged again only after this

# ── Settings ────────────────────────────────────────────────────────────────
DEFAULTS: Dict[str, Any] = {
    "enabled": True,           # connect and read (always active by default)
    "prices": True,            # the bot's Chainlink price and the window's strike come from the stream (default)
    "settle": True,            # a window whose result isn't in yet is booked on the two published averages (default)
    "chance": True,            # the sniper's and the maker's chance is worked out on the 60-second average (default)
    "sd_floor_usd": 3.0,       # chance: the smallest uncertainty of the closing average, in dollars
    "pick_tolerance_s": 2.0,   # an average counts for a window boundary when it is stamped at most this long after it
}
USES = ("prices", "settle", "chance")
_USE_TEXT = {"prices": "the bot's price and strike", "settle": "booking a window whose result isn't in yet",
             "chance": "the sniper's and the maker's chance"}


def _as_bool(v: Any, label: str) -> bool:
    if v is None or isinstance(v, bool):
        return bool(v)
    if isinstance(v, (int, float)) and v in (0, 1):
        return bool(v)
    if isinstance(v, str):
        t = v.strip().lower()
        if t in ("true", "1", "yes", "on"):
            return True
        if t in ("false", "0", "no", "off", ""):
            return False
    raise ValueError(f"price stream: {label} must be true or false")


def merged(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """DEFAULTS overlaid with the known keys of cfg."""
    out = dict(DEFAULTS)
    for k, v in (cfg or {}).items():
        if k in DEFAULTS:
            out[k] = v
    return out


def normalize(new: Optional[Dict[str, Any]], current: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """merged(current) overlaid with new, checked. Raises ValueError naming every bad field."""
    if new is not None and not isinstance(new, dict):
        raise ValueError("price stream: the settings must be an object")
    cfg = merged(current)
    for k, v in (new or {}).items():
        if k in DEFAULTS:
            cfg[k] = v
    errs: List[str] = []
    for k in ("enabled",) + USES:
        try:
            cfg[k] = _as_bool(cfg[k], k)
        except ValueError as e:
            errs.append(str(e))
    for k, label, lo, hi in (("sd_floor_usd", "the smallest uncertainty", 0.0, 1000.0),
                             ("pick_tolerance_s", "the pick tolerance", 0.5, 30.0)):
        try:
            v = float(cfg[k])
        except (TypeError, ValueError):
            errs.append(f"price stream: {label} must be a number")
            continue
        if v != v or v < lo or v > hi:
            errs.append(f"price stream: {label} must be between {lo:g} and {hi:g}")
        else:
            cfg[k] = v
    if errs:
        raise ValueError("; ".join(errs))
    return cfg


def on(cfg: Optional[Dict[str, Any]]) -> bool:
    """The stream is switched on (default True)."""
    return bool((cfg or {}).get("enabled", True))


def use(cfg: Optional[Dict[str, Any]], what: str) -> bool:
    """The stream is on AND the bot uses it for `what` (default True)."""
    return on(cfg) and bool((cfg or {}).get(what, True))


def status_line(cfg: Optional[Dict[str, Any]]) -> str:
    """The PRICE STREAM log line: off, or on and what it is used for."""
    if not on(cfg):
        return "PRICE STREAM: off"
    return ("PRICE STREAM: on (default) — Polymarket's 60-second average and live price; used for "
            "strike (price to beat), settlement & chance")


def symbol_for(bot_symbol: Optional[str]) -> str:
    """The bot's symbol (BTCUSDT) as the stream names it (btcusd)."""
    s = str(bot_symbol or "BTCUSDT").strip().upper()
    for tail in ("USDT", "USDC", "USD"):
        if s.endswith(tail) and len(s) > len(tail):
            return s[:-len(tail)].lower() + "usd"
    return (s.lower() + "usd") if s else SYMBOL


# ── Frames ───────────────────────────────────────────────────────────────────
def _num(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _point(p: Any, fallback_ts: Any) -> Optional[Tuple[float, float]]:
    """(unix seconds, price) from one point; the exact full_accuracy_value first."""
    if not isinstance(p, dict):
        return None
    value = _num(p.get("full_accuracy_value"))
    if value is None:
        value = _num(p.get("value"))
    ts = _num(p.get("timestamp"))
    if ts is None:
        ts = _num(fallback_ts)
    if value is None or ts is None or value <= 0 or ts <= 0:
        return None
    return (ts / 1000.0 if ts > 1e11 else ts), value


def parse_frame(msg: Any) -> Tuple[Optional[str], Optional[str], Optional[str], List[Tuple[float, float]]]:
    """(channel, source, symbol, points) of a data frame; (None, None, None, []) for anything else.
    This is the one place that knows the frames' shape."""
    if not isinstance(msg, dict) or msg.get("op"):
        return None, None, None, []
    channel, payload = msg.get("channel"), msg.get("payload")
    if not channel or not isinstance(payload, dict):
        return None, None, None, []
    items = payload.get("data") if isinstance(payload.get("data"), list) else [payload]
    pts = sorted(pt for pt in (_point(p, msg.get("ts")) for p in items) if pt)
    source = payload.get("source")
    if source is None and items and isinstance(items[-1], dict):
        source = items[-1].get("source")
    symbol = payload.get("symbol")
    return (str(channel), str(source).lower() if source else None,
            str(symbol).lower() if symbol else None, pts)


def _retry_after_s(e: BaseException) -> float:
    """The Retry-After of a refused connection (HTTP 429 / 503), in seconds; 0 when there is none."""
    try:
        return max(0.0, min(float((getattr(e, "headers", None) or {}).get("Retry-After") or 0.0), 300.0))
    except (TypeError, ValueError, AttributeError):
        return 0.0


class PolyBolt:
    def __init__(self, get_creds: Callable[[], Optional[Dict[str, str]]],
                 enabled: Callable[[], bool] = lambda: True, log: Callable[[str], None] = print,
                 url: str = URL, symbol: str = SYMBOL, proxy: Optional[str] = None,
                 on_live: Optional[Callable[[float, float], None]] = None):
        self.get_creds = get_creds      # {"apiKey", "secret", "passphrase"} or None (no key yet)
        self.enabled = enabled          # looked at all the time: switching the stream on or off needs no restart
        self.log = log
        self.url, self.symbol, self.proxy = url, str(symbol or SYMBOL).lower(), proxy
        self.on_live = on_live          # called with (unix seconds, price) for every new live Chainlink price
        self.on_frame: Optional[Callable[[Dict[str, Any]], None]] = None   # a tap for try_price_stream.py
        self.twap: List[Tuple[float, float]] = []    # (unix seconds, published 60-second average), oldest first
        self.live_pts: List[Tuple[float, float]] = []   # (unix seconds, live Chainlink price), oldest first
        self.live_at = 0.0              # our clock when the last live price arrived
        self.twap_seen_at = 0.0         # our clock when the last average arrived
        self.frame_at = 0.0             # our clock when the last frame of any kind arrived
        self.data_at = 0.0              # our clock when the last price of either kind was stored
        self.source: Optional[str] = None            # the vendor of price.crypto, as last seen
        self.acks: Dict[str, Any] = {}  # channel -> the provider the server says it serves (None if it doesn't say)
        self.status = "off"
        self.connects = self.dropped = 0
        self.closed = False
        self._pin = True                # ask for Chainlink by name on price.crypto (dropped if the server refuses the filter)
        self._restart = False           # set_symbol: let go of the connection and subscribe again
        self._said: Dict[str, float] = {}            # what was said in the log, and when
        self._secrets: Tuple[str, ...] = ()          # the credentials in use, only to keep them out of any log line

    # ── what main.py asks ──
    def twap_point_at(self, ts: float, tolerance_s: float = 2.0) -> Optional[Tuple[float, float]]:
        """(its stamp, value) of Polymarket's 60-second average for a window boundary: the first one stamped
        at or after ts, if that is within tolerance_s of it; else None."""
        i = bisect.bisect_left(self.twap, (ts - 1e-6,))
        if i >= len(self.twap) or self.twap[i][0] - ts > tolerance_s:
            return None
        return self.twap[i]

    def twap_at(self, ts: float, tolerance_s: float = 2.0) -> Optional[float]:
        pt = self.twap_point_at(ts, tolerance_s)
        return pt[1] if pt else None

    def live(self, max_age_s: float) -> Optional[Tuple[float, float]]:
        """(its own time, price) of the latest live Chainlink price, if it arrived within max_age_s; else None."""
        if not self.live_pts or time.time() - self.live_at > max_age_s:
            return None
        return self.live_pts[-1]

    def ticks(self) -> List[Tuple[float, float]]:
        return list(self.live_pts)

    def view(self) -> Dict[str, Any]:
        """For the dashboard: what the stream has right now."""
        now = time.time()
        live_age = (now - self.live_at) if self.live_pts else None
        avg_age = (now - self.twap_seen_at) if self.twap else None
        return {"status": self.status, "symbol": self.symbol, "source": self.source, "connects": self.connects,
                "average": self.twap[-1][1] if self.twap else None,
                "average_age_s": round(avg_age, 1) if avg_age is not None else None,
                "live": self.live_pts[-1][1] if self.live_pts else None,
                "live_age_s": round(live_age, 1) if live_age is not None else None,
                "fresh": bool(self.status == "receiving" and live_age is not None and avg_age is not None
                              and live_age <= FRESH_S and avg_age <= FRESH_S),
                "dropped": self.dropped}

    def set_symbol(self, symbol: str) -> None:
        """The bot's symbol changed: forget the old prices and subscribe again for the new one."""
        symbol = str(symbol or SYMBOL).lower()
        if symbol == self.symbol:
            return
        self.symbol = symbol
        self.twap, self.live_pts = [], []
        self.live_at = self.twap_seen_at = 0.0
        self.source = None
        self._restart = True

    # ── the log ──
    def _say(self, key: str, text: str, again_after_s: Optional[float] = None) -> None:
        """Log text once per connection (again_after_s None), or at most once every again_after_s."""
        now = time.time()
        if key in self._said and (again_after_s is None or now - self._said[key] < again_after_s):
            return
        self._said[key] = now
        for s in self._secrets:
            if s and s in text:
                text = text.replace(s, "***")
        self.log(text)

    # ── frames ──
    def handle(self, msg: Any) -> None:
        channel, source, symbol, pts = parse_frame(msg)
        if isinstance(msg, dict) and msg.get("dropped"):
            self.dropped += int(_num(msg.get("dropped")) or 0)
        if not channel or (symbol and symbol != self.symbol):
            return
        now = time.time()
        if channel == "price.crypto.twap":
            if not pts:
                return
            last = self.twap[-1][0] if self.twap else 0.0
            for ts, v in pts:
                if ts > last:
                    self.twap.append((ts, v))
                    last = ts
                    self.twap_seen_at = self.data_at = now
            while self.twap and self.twap[0][0] < self.twap[-1][0] - KEEP_TWAP_S:
                self.twap.pop(0)
        elif channel == "price.crypto":
            if source:
                self.source = source
            if not pts:
                return
            if self.source != "chainlink":           # another vendor's price is not what the markets are decided on
                self._say("vendor", f"PRICE STREAM: the live price comes from {self.source or 'an unnamed source'}, "
                                    "not chainlink — not used")
                return
            last = self.live_pts[-1][0] if self.live_pts else 0.0
            for ts, v in pts:
                if ts > last:
                    self.live_pts.append((ts, v))
                    last = ts
                    self.live_at = self.data_at = now
                    if self.on_live:
                        try:
                            self.on_live(ts, v)
                        except Exception:
                            pass
            while self.live_pts and self.live_pts[0][0] < self.live_pts[-1][0] - KEEP_TICKS_S:
                self.live_pts.pop(0)
        else:
            return
        if self.twap and self.live_pts and self.status != "receiving":
            self.status = "receiving"
            self._say("receiving", f"PRICE STREAM: receiving — 60-second average {self.twap[-1][1]:,.2f}, "
                                   f"live price {self.live_pts[-1][1]:,.2f} ({self.source})")

    def _subscriptions(self) -> List[Dict[str, Any]]:
        live_filter: Dict[str, Any] = {"symbol": self.symbol}
        if self._pin:
            live_filter["provider"] = "chainlink"
        return [{"channel": "price.crypto.twap", "filter": {"symbol": self.symbol, "window_seconds": 60}},
                {"channel": "price.crypto", "filter": live_filter}]

    async def _session(self, creds: Dict[str, str]) -> Optional[int]:
        """One connection; returns its close code (None if it just ended; 4001 when the login was refused,
        4008 when the server refused a request of ours)."""
        self._restart = False
        self._secrets = tuple(str(v) for v in creds.values() if v)
        self.status = "connecting"
        for k in ("receiving", "vendor"):
            self._said.pop(k, None)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=8.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                ws = await asyncio.wait_for(session.ws_connect(self.url, proxy=self.proxy or None), timeout=CONNECT_S)
            except asyncio.TimeoutError:
                raise ConnectionError(f"no answer to the connection in {CONNECT_S:g}s") from None
            try:
                self.connects += 1
                self.acks = {}
                await ws.send_json({"op": "auth", "rid": "a1", "auth": creds})
                while True:
                    if self.closed or self._restart or not self.enabled():
                        await ws.close()
                        return None
                    try:
                        m = await ws.receive(timeout=STALL_S)
                    except asyncio.TimeoutError:
                        self._say("stall", f"PRICE STREAM: no frame for {STALL_S:g}s — reconnecting", SAY_AGAIN_S)
                        await ws.close()
                        return None
                    if m.type != aiohttp.WSMsgType.TEXT:
                        if m.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED,
                                      aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    self.frame_at = time.time()
                    try:
                        msg = json.loads(m.data)
                    except ValueError:
                        continue
                    if not isinstance(msg, dict):
                        continue
                    if self.on_frame:
                        try:
                            self.on_frame(msg)
                        except Exception:
                            pass
                    op = msg.get("op")
                    if op == "authed":
                        self.status = "logged in"
                        await ws.send_json({"op": "subscribe", "rid": "s1", "subscriptions": self._subscriptions()})
                    elif op == "subscribed":
                        self.acks[str(msg.get("channel"))] = msg.get("provider")
                    elif op == "error":
                        code, chan = str(msg.get("code") or "error"), msg.get("channel")
                        if code == "bad_filter" and chan == "price.crypto" and self._pin:
                            self._pin = False                # this server takes no provider here: ask without it
                            await ws.send_json({"op": "subscribe", "rid": "s2", "subscriptions": [self._subscriptions()[1]]})
                            continue
                        self._say("err:" + code, f"PRICE STREAM: the server answered {code}" + (f" ({chan})" if chan else ""),
                                  SAY_AGAIN_S)
                        if code in ("auth_invalid", "auth_attempts"):
                            await ws.close()
                            return 4001
                        if code in ("bad_op", "bad_channel", "bad_filter", "sub_limit", "rate_limited"):
                            await ws.close()
                            return 4008
                        if code in ("auth_unavailable", "auth_required", "auth_expired"):
                            await ws.close()
                            return None
                    elif not op:
                        self.handle(msg)
                return ws.close_code
            finally:
                if not ws.closed:
                    await ws.close()

    async def run_forever(self) -> None:
        backoff, refused = FIRST_BACKOFF_S, 0
        while not self.closed:
            if not self.enabled():
                self.status = "off"
                await asyncio.sleep(IDLE_S)
                continue
            code, min_wait = None, 0.0
            try:
                creds = await asyncio.to_thread(self.get_creds)
                if not creds:                        # no key in the bot, or its order client isn't set up yet
                    self.status = "no credentials yet"
                    await asyncio.sleep(NO_KEY_WAIT_S)
                    continue
                code = await self._session(creds)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                min_wait = _retry_after_s(e)
                self._say("exc:" + type(e).__name__, f"PRICE STREAM: {type(e).__name__}: {e}", SAY_AGAIN_S)
            if self.closed or self._restart or not self.enabled():
                continue
            self.status = "reconnecting"
            if code == 4003:
                await asyncio.sleep(random.uniform(0.0, DRAIN_MAX_DELAY_S))
                continue
            if time.time() - self.data_at < 60:      # prices were coming in: start again from the short waits
                backoff, refused = FIRST_BACKOFF_S, 0
            if code in (4001, 4008):
                refused += 1
                wait = min(REFUSED_WAIT_S * 2 ** (refused - 1), REFUSED_MAX_WAIT_S)
                self._say("refused:%s" % code, "PRICE STREAM: "
                          + ("the login was refused" if code == 4001 else "the server refused a request or a limit was hit (4008)")
                          + f" — next try in {wait:g}s, then less and less often", SAY_AGAIN_S)
                await asyncio.sleep(wait)
                continue
            await asyncio.sleep(max(min_wait, backoff + random.uniform(0.0, backoff)))
            backoff = min(backoff * 2, MAX_BACKOFF_S)
