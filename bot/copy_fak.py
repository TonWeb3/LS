"""
Fill-and-kill live copy (PATCH: fill-and-kill copy).

The bot's live copy uses Fill-And-Kill (FAK) orders with persistent progressive retries (FAK-main model).
What doesn't fill at once at the limit price is cancelled by Polymarket without resting on the book.
The bot then refreshes the live book ask and retries up to `max_retries` attempts with progressive slippage sweep:

  * base price: the freshest book ask (or paper entry on attempt #1) + slippage cap (default 5.0¢);
  * progressive sweep: adds +1.5¢ per retry (up to +8¢ extra) to sweep books in fast-moving markets;
  * sizing: in dollars (Risk setting, or stake_pct % of live balance, minimum $1.00 and 5 shares);
  * instant RAM balance deduction: fills immediately deduct spent USDC in memory for 0ms multi-fill safety;
  * count_fee: books Polymarket's taker fee with each fill (true net balance tracking).

Everything here is plain functions, so it runs and tests cleanly. main.py calls live_copy()
at the top of _continuous_live_copy_fill and passes in the bot's state, order client, websocket
stream, and callbacks.
"""

import asyncio
import math
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

# ── Settings ─────────────────────────────────────────────────────────────────────────────
DEFAULTS: Dict[str, Any] = {
    "enabled": True,           # live copy with retries enabled
    "order_type": "FAK",       # "FAK" (Fill-And-Kill / partial fills allowed) or "FOK" (Fill-Or-Kill / all-or-none)
    "cap_cents": 5.0,          # pay at most the entry + this many cents (+5¢ slippage default)
    "high_cap_cents": 5.0,     # +5¢ high cap
    "high_from_price": 0.55,   # paper entries above this price use high_cap_cents (55¢ threshold)
    "stake_pct": 19.0,         # % of the live balance per trade (19% compounding)
    "max_retries": 5,          # retry up to 5 times (matches FAK-main)
    "retry_interval_ms": 300,  # 300ms between attempts (matches FAK-main)
    "count_fee": True,         # book Polymarket's taker fee with each fill (true net balance tracking)
    "fee_rate": 0.07,          # Polymarket's taker fee rate for crypto markets
}

# key: (lowest, highest, what a refusal calls it)
RANGES: Dict[str, Tuple[float, float, str]] = {
    "cap_cents": (0.0, 99.0, "the cap must be 0 to 99 cents"),
    "high_cap_cents": (0.0, 99.0, "the cap for higher-priced entries must be 0 to 99 cents"),
    "high_from_price": (0.01, 1.0, "the price the higher cap starts above must be 0.01 to 1"),
    "stake_pct": (0.0, 100.0, "the bet size must be 0 to 100 percent of the balance"),
    "max_retries": (1.0, 20.0, "max retries must be between 1 and 20"),
    "retry_interval_ms": (10.0, 5000.0, "retry interval must be between 10 and 5000 ms"),
    "fee_rate": (0.0, 1.0, "the fee rate must be 0 to 1"),
}
BOOLS = ("enabled", "count_fee")

# what Polymarket says when a fill-and-kill order found nothing at its price (the order is gone)
NO_MATCH = ("no orders found to match", "no matching orders")
LOOKUP_WAITS_S = (0.5, 1.0, 1.5)   # an accepted order whose answer has no amounts: when to look its fills up


def as_bool(v: Any, label: str = "value") -> bool:
    """True / False from a JSON value; anything that isn't clearly one of them is refused."""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and v in (0, 1):
        return bool(v)
    if isinstance(v, str) and v.strip().lower() in ("true", "false", "1", "0", "on", "off", "yes", "no"):
        return v.strip().lower() in ("true", "1", "on", "yes")
    raise ValueError(f"{label} must be true or false")


def _number(v: Any) -> Optional[float]:
    """A finite number, or None for anything else (true / false are not numbers here)."""
    if isinstance(v, bool) or v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def merged(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """DEFAULTS with cfg's usable values on top. Never raises: a missing, unknown or unusable
    value is left at its default (so a hand-edited config.json can't stop the bot)."""
    out = dict(DEFAULTS)
    if not isinstance(cfg, dict):
        return out
    ot = str(cfg.get("order_type") or "FAK").strip().upper()
    if ot in ("FAK", "FOK"):
        out["order_type"] = ot
    for k in BOOLS:
        if k in cfg:
            try:
                out[k] = as_bool(cfg[k])
            except ValueError:
                pass
    for k, (lo, hi, _) in RANGES.items():
        x = _number(cfg.get(k))
        if x is not None and lo <= x <= hi:
            out[k] = int(round(x)) if k in ("max_retries", "retry_interval_ms") else float(x)
    return out


def normalize(cfg: Any, base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """A clean dict from user / API input, or raises ValueError with what is wrong."""
    if not isinstance(cfg, dict):
        raise ValueError("copy_fak settings must be an object")
    target = dict(merged(base) if base else DEFAULTS)
    out: Dict[str, Any] = {}
    if "order_type" in cfg:
        ot = str(cfg.get("order_type") or "").strip().upper()
        if ot in ("FAK", "FOK"):
            out["order_type"] = ot
        else:
            raise ValueError("order_type must be FAK or FOK")
    for k in BOOLS:
        if k in cfg:
            out[k] = as_bool(cfg[k], k)
    for k, (lo, hi, msg) in RANGES.items():
        if k in cfg and cfg[k] is not None:
            x = _number(cfg[k])
            if x is None or not (lo <= x <= hi):
                raise ValueError(msg)
            out[k] = int(round(x)) if k in ("max_retries", "retry_interval_ms") else round(float(x), 4)
    target.update(out)
    return merged(target)


def on(cfg: Optional[Dict[str, Any]]) -> bool:
    """True when fill-and-kill live copy is turned on in its settings."""
    return bool(merged(cfg)["enabled"])


def status_line(cfg: Optional[Dict[str, Any]] = None) -> str:
    """A one-line log summary of the live copy settings."""
    c = merged(cfg)
    otype = str(c.get("order_type") or "FAK").upper()
    if not c["enabled"]:
        return f"{otype} COPY: off — live trades use GTD limit orders"
    fee_str = f", count fee ({c['fee_rate']*100:.0f}%)" if c["count_fee"] else ""
    return (f"{otype} COPY: on — type {otype}, base cap {c['cap_cents']:.1f}¢ (high {c['high_cap_cents']:.1f}¢ above {c['high_from_price']:.2f}), "
            f"stake {c['stake_pct']:.0f}% balance, max {c['max_retries']} retries @ {c['retry_interval_ms']}ms{fee_str}")


# ── Prices and stakes ────────────────────────────────────────────────────────────────────
def _whole_cent(price: float) -> bool:
    """True when the price is whole cents (0.42, not 0.425). Polymarket's order tick is 0.01
    for whole cents and 0.001 for sub-cent prices."""
    cents = float(price) * 100.0
    return abs(cents - round(cents)) < 1e-6


def _px(p: Optional[float]) -> str:
    return f"{float(p):.4f}" if p is not None else "none"


def _cents(c: float) -> str:
    c = float(c)
    return f"{int(c)}¢" if abs(c - round(c)) < 1e-6 else f"{c:.1f}¢"


def cap_cents_for(price: float, cfg: Optional[Dict[str, Any]] = None, settings: Any = None) -> float:
    """The cent cap that applies to this price: high_cap_cents above high_from_price, cap_cents
    otherwise (e.g. 5.0¢). Falls back to settings.CLOB_MAX_SLIPPAGE * 100 if cap_cents <= 0."""
    c = merged(cfg)
    cap = float(c["high_cap_cents"] if float(price) > float(c["high_from_price"]) else c["cap_cents"])
    if cap <= 0 and settings is not None:
        slip = getattr(settings, "CLOB_MAX_SLIPPAGE", None)
        if slip is not None and float(slip) > 0:
            cap = round(float(slip) * 100.0, 2)
    return cap


def order_price(paper: float, cfg: Optional[Dict[str, Any]] = None, extra_slip: float = 0.0, settings: Any = None) -> Optional[float]:
    """The limit price for the order: paper entry (or current ask) + cap + extra_slip (capped between 0.01 and 0.99)."""
    p = _number(paper)
    if p is None or p <= 0:
        return None
    cap = cap_cents_for(p, cfg, settings=settings) + (extra_slip * 100.0)
    return round(min(0.99, max(0.01, p + cap / 100.0)), 4)


def taker_fee(shares: float, price: float, rate: float = 0.07) -> float:
    """Polymarket's taker fee for crypto markets: shares x rate x price x (1 - price).
    Charged in USDC on top of the fill."""
    p = max(0.0, min(1.0, float(price)))
    return round(max(0.0, float(shares)) * max(0.0, float(rate)) * p * (1.0 - p), 5)


def min_order_usd(price: float) -> float:
    """Polymarket's minimum order in dollars at this limit price: $1, and 5 shares."""
    return round(max(1.00, math.ceil(5.0 * float(price) * 100.0 - 1e-6) / 100.0), 2)


def bot_stake(balance: float, settings: Any) -> float:
    """The bot's own live stake from its Risk setting, as the bot computes it: RISK_VALUE dollars
    ("fixed") or RISK_VALUE % of the balance, at least $1, at most the balance."""
    balance = float(balance or 0.0)
    if balance < 1.0:
        return 0.0
    if str(getattr(settings, "RISK_TYPE", "percent") or "percent").lower() == "fixed":
        stake = float(settings.RISK_VALUE)
    else:
        stake = float(settings.RISK_VALUE) / 100.0 * balance
    return round(max(1.0, min(stake, balance)), 2)


def stake_usd(balance: float, precomputed: Optional[float], settings: Any, cfg: Optional[Dict[str, Any]]) -> float:
    """The dollars to bet: stake_pct % of the live balance, or (stake_pct 0) the bot's own stake."""
    c = merged(cfg)
    balance = float(balance or 0.0)
    if c["stake_pct"] > 0:
        return round(c["stake_pct"] / 100.0 * balance, 2)
    stake = float(precomputed or 0.0)
    return stake if stake > 0 else bot_stake(balance, settings)


def order_usd(stake: float, price: float, balance: float, fee_rate: float) -> Tuple[float, str]:
    """The dollars to send: the stake, at least Polymarket's minimums, at most what the balance
    pays for with the taker fee on top (the fee is charged in cash beside the fill).
    (0.0, reason) when the balance can't pay for the minimum order."""
    low = min_order_usd(price)
    per_dollar = 1.0 + max(0.0, float(fee_rate)) * max(0.0, 1.0 - float(price))   # fill + fee per fill dollar
    room = math.floor(float(balance) / per_dollar * 100.0 + 1e-6) / 100.0
    if room < low:
        return 0.0, (f"deposit balance too low (${float(balance):.2f} can't pay for the ${low:.2f} minimum order "
                     f"at {_px(price)} with its fee)")
    return round(min(max(float(stake), low), room), 2), ""


# ── The order ────────────────────────────────────────────────────────────────────────────
def _to_float(x: Any) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def order_amounts(trader: Any, token_id: str, usd: float, price: float) -> Tuple[int, int, int]:
    """(side, maker amount, taker amount) of a fill-and-kill BUY of `usd` dollars at `price` or
    better, in the 6-decimal units the order is signed with. Polymarket's rule for these orders:
    dollars with at most 2 decimals, shares with at most 4 (the bot's order builder keeps to it)."""
    from polymarket_apis.utilities.order_builder.builder import ROUNDING_CONFIG
    tick = "0.01" if _whole_cent(float(price)) else "0.001"
    if hasattr(trader, "warm_token"):
        try:
            trader.warm_token(token_id, tick)
        except Exception:
            pass
    if hasattr(trader, "clob") and hasattr(trader.clob, "builder"):
        side_int, maker_amt, taker_amt = trader.clob.builder.get_market_order_amounts(
            "BUY", round(float(usd), 2), round(float(price), 3), ROUNDING_CONFIG[tick])
    else:
        shares = usd / price if price > 0 else 0.0
        side_int, maker_amt, taker_amt = 0, int(round(usd * 1e6)), int(round(shares * 1e6))
    return int(side_int), int(maker_amt), int(taker_amt)


def read_answer(data: Dict[str, Any], maker_amt: int, taker_amt: int) -> Dict[str, Any]:
    """Polymarket's answer to the order -> a result dict: ok, order_id, status ("filled" when it
    matched and says how much), fill_size (shares), fill_usd, fill_price, order_usd, order_shares."""
    data = data if isinstance(data, dict) else {}
    order_id = data.get("order_id") or data.get("orderID") or data.get("orderId") or data.get("id")
    status = str(data.get("status", "") or "").lower()
    making = _to_float(data.get("making_amount") or data.get("makingAmount"))
    taking = _to_float(data.get("taking_amount") or data.get("takingAmount"))
    sent_usd, sent_shares = maker_amt / 1e6, taker_amt / 1e6
    if making > sent_usd * 1000.0 or taking > sent_shares * 1000.0:   # the amounts came in 6-decimal units
        making, taking = making / 1e6, taking / 1e6
    accepted = bool(order_id) and data.get("success") is not False
    filled = accepted and status in ("matched", "filled", "closed") and making > 0 and taking > 0
    return {
        "ok": accepted,
        "order_id": order_id,
        "status": "filled" if filled else (status or "unknown"),
        "fill_size": taking if filled else None,
        "fill_usd": making if filled else None,
        "fill_price": (making / taking) if filled else None,
        "order_usd": sent_usd,
        "order_shares": sent_shares,
        "error": None if accepted else (data.get("errorMsg") or data.get("error") or "order_not_accepted"),
        "response": data,
    }


def place(trader: Any, settings: Any, token_id: str, usd: float, price: float, order_type: str = "FAK") -> Dict[str, Any]:
    """Send the FAK or FOK marketable BUY and wait for Polymarket's answer (blocking: run it on a thread)."""
    if not token_id:
        return {"ok": False, "error": "missing_token_id"}
    if not trader.ensure_ready():
        return {"ok": False, "error": getattr(trader, "last_error", None) or "client_not_ready"}
    if not getattr(trader, "_approvals_done", True) and getattr(settings, "RELAYER_API_KEY", ""):
        try:
            trader.ensure_setup()
        except Exception:
            pass
    try:
        ot = "FOK" if str(order_type).upper() == "FOK" else "FAK"
        side_int, maker_amt, taker_amt = order_amounts(trader, token_id, usd, price)
        data = trader._post_order_fast(token_id=str(token_id), maker_amount=maker_amt, taker_amount=taker_amt,
                                       side_int=side_int, order_type_str=ot, expiration_str="0", neg_risk=False)
        return read_answer(data, maker_amt, taker_amt)
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _thread_runner(trader: Any) -> Callable:
    """The bot's own order threads when it has them (run_io), a plain worker thread otherwise."""
    return getattr(trader, "run_io", None) or (lambda fn, *a: asyncio.to_thread(fn, *a))


async def place_async(trader: Any, settings: Any, token_id: str, usd: float, price: float, order_type: str = "FAK") -> Dict[str, Any]:
    """place() from the event loop with FAK or FOK order type. When the bot's order library can send from its own threads
    (rust_clob's post_order_async) the order is signed right here and leaves at once; otherwise
    place() runs on a thread. Same result dict."""
    ot = "FOK" if str(order_type).upper() == "FOK" else "FAK"
    run = _thread_runner(trader)
    mod = sys.modules.get("rust_clob")
    ready = bool(getattr(trader, "ready", False) and getattr(trader, "_rust_ready", False)
                 and getattr(trader, "clob", None) is not None)
    two_step = hasattr(trader, "_post_args") and hasattr(trader, "_finish_post")   # the Longshot bot's order code
    one_step = hasattr(trader, "_order_json")                                      # the 15m bot's latency patch
    fast = (bool(token_id) and ready and mod is not None and hasattr(mod, "post_order_async")
            and hasattr(mod, "post_order_wait") and (two_step or one_step))
    if fast and not getattr(trader, "_approvals_done", True) and getattr(settings, "RELAYER_API_KEY", ""):
        if hasattr(trader, "_setup_in_background"):
            trader._setup_in_background()      # the one-time wallet setup, off the order's path
        else:
            fast = False                       # place() runs that setup first, as the bot's own orders do
    if not fast:
        return await run(place, trader, settings, token_id, usd, price, ot)
    try:
        side_int, maker_amt, taker_amt = order_amounts(trader, token_id, usd, price)
        if two_step:
            post_args = trader._post_args(token_id, maker_amt, taker_amt, side_int, ot, "0", False, False)
            try:
                ticket = mod.post_order_async(*post_args)
            except TypeError:
                try:
                    ticket = mod.post_order_async(*post_args[:-1])
                except TypeError:
                    ticket = mod.post_order_async(*post_args[:-2])
        else:
            ticket = mod.post_order_async(str(token_id), int(maker_amt), int(taker_amt), int(side_int),
                                          ot, "0", False, False, False)
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    try:
        res = await run(mod.post_order_wait, ticket, 30.0)
        if two_step:
            data = await run(trader._finish_post, res, post_args, mod)
        else:   # a refusal is explained off the event loop (the error text can need the key's address)
            data = trader._order_json(res) if res.get("ok_http") else await run(trader._order_json, res)
        return read_answer(data, maker_amt, taker_amt)
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


async def order_fills(trader: Any, order_id: Optional[str], token_id: str, status: Optional[str]) -> List[Dict[str, Any]]:
    """An accepted order whose answer has no fill amounts: look its fills up (Polymarket's trade
    list can lag a second or two). A fill-and-kill order never rests; if its status says it does,
    cancel it first."""
    if not order_id:
        return []
    run = _thread_runner(trader)
    if str(status or "").lower() in ("live", "unmatched", "open"):
        try:
            await run(trader.cancel_order, order_id)
        except Exception:
            pass
    for wait in LOOKUP_WAITS_S:
        await asyncio.sleep(wait)
        try:
            fills = await run(trader.get_order_fills, order_id, token_id)
        except Exception:
            fills = []
        if fills:
            return fills
    return []


# ── The live copy ────────────────────────────────────────────────────────────────────────
def book_fill(trade: Dict[str, Any], shares: float, usd: float, *, settings: Any, state: Dict[str, Any],
              paper: float, target_usd: Optional[float] = None) -> float:
    """Book a fill on the trade (accumulates multiple partial fills across retries)
    and deduct what it cost from the live balance in RAM (and recalculate next stake)."""
    cfg = merged(getattr(settings, "COPY_FAK", None))
    otype = str(cfg.get("order_type") or "FAK").upper()
    shares = float(shares or 0.0)
    usd = float(usd or 0.0)
    if shares <= 0 or usd <= 0:
        return 0.0
    fill_price = usd / shares
    fee = taker_fee(shares, fill_price, cfg["fee_rate"]) if cfg["count_fee"] else 0.0
    cost = usd + fee

    prev_shares = float(trade.get("live_shares") or 0.0)
    prev_usd = float(trade.get("live_raw_usd") or (prev_shares * float(trade.get("live_entry_price") or 0.0)))
    prev_fee = float(trade.get("live_fee") or 0.0)

    total_shares = prev_shares + shares
    total_raw_usd = prev_usd + usd
    total_fee = prev_fee + fee
    total_cost = total_raw_usd + total_fee
    avg_price = total_raw_usd / total_shares if total_shares > 0 else fill_price

    trade["live_status"] = "FILLED"
    trade["live_entry_price"] = round(avg_price, 4)
    trade["live_shares"] = round(total_shares, 4)
    trade["live_raw_usd"] = round(total_raw_usd, 4)
    trade["live_amount"] = round(total_cost, 2)
    trade["live_entry_time"] = trade.get("live_entry_time") or datetime.now().isoformat()
    trade["live_quoted_price"] = paper
    trade["live_slippage"] = round(avg_price - paper, 4)
    if cfg["count_fee"]:
        trade["live_fee"] = round(total_fee, 4)

    is_partial = target_usd is not None and total_raw_usd < float(target_usd) * 0.99
    trade["copy_status"] = f"{otype}_PARTIAL" if is_partial else f"{otype}_FILLED"
    trade["ls_partial"] = is_partial

    if cost > 0:
        new_bal = max(0.0, float(state.get("live_balance") or 0.0) - cost)
        state["live_balance"] = new_bal
        state["precomputed_live_stake"] = bot_stake(new_bal, settings)

    return fee


async def live_copy(trade: Dict[str, Any], evaluated_price: Optional[float] = None, *, settings: Any,
                    state: Dict[str, Any], trader: Any, log: Callable[[str], Any], save: Callable[[], Any],
                    sync: Callable[[], Any], broadcast: Callable[[], Any], telegram: Callable[[str], Any],
                    order_sent: Optional[Any] = None, ws_client: Optional[Any] = None) -> None:
    """The live copy of a trade executed using continuous FAK/FOK retries:
    Refreshes live ask from book on retries, applies progressive slippage sweep (+1.5¢ per retry),
    and attempts up to `max_retries` times every `retry_interval_ms` ms without dropping trades.
    In FAK mode, partial fills are accumulated and subsequent retries cover the remaining amount."""
    if trade.get("status") != "OPEN" or trade.get("live_status") != "FILLING":
        return

    cfg = merged(getattr(settings, "COPY_FAK", None))
    otype = str(cfg.get("order_type") or "FAK").strip().upper()
    if otype not in ("FAK", "FOK"):
        otype = "FAK"
    side, token_id, slug = trade.get("side"), trade.get("token_id"), trade.get("market_slug")
    paper = float(trade.get("paper_entry_price") or evaluated_price or 0.0)

    if not token_id or paper <= 0:
        if order_sent is not None:
            order_sent.set()
        trade["live_status"], trade["copy_status"] = "FAILED", f"{otype}_FAILED"
        trade["live_error"] = "missing token_id or paper entry price"
        log(f"LIVE copy aborted for {slug}: missing token_id or paper price")
        return

    max_attempts = int(cfg.get("max_retries") or getattr(settings, "COPY_MAX_RETRIES", 5) or 5)
    retry_interval = max(0.01, float(cfg.get("retry_interval_ms") or getattr(settings, "COPY_RETRY_INTERVAL_MS", 300) or 300) / 1000.0)

    if ws_client is None:
        try:
            import main as _m
            ws_client = getattr(_m, "polymarket_clob_ws", None)
        except Exception:
            pass

    base_cap_c = cap_cents_for(paper, cfg, settings=settings)
    log(f"LIVE copy started for {slug} ({side}) — continuous {otype} fill loop initiated (base slippage {base_cap_c:.1f}¢, max {max_attempts} attempts)")

    # Establish initial target stake in dollars
    initial_balance = float(state.get("live_balance") or 0.0)
    if initial_balance <= 0:
        initial_balance = float(await asyncio.to_thread(trader.get_usdc_balance) or 0.0)
        state["live_balance"] = initial_balance

    target_usd = stake_usd(initial_balance, state.get("precomputed_live_stake"), settings, cfg)

    current_ask = paper
    limit = None
    t_start = time.perf_counter()
    total_filled_shares = 0.0
    total_filled_usd = 0.0

    try:
        while trade.get("status") == "OPEN" and trade.get("live_status") in ("FILLING", "FILLED"):
            now_ts = time.time()
            # 1. Expiry check: window expired
            if trade.get("end_ts") and now_ts >= float(trade["end_ts"]):
                if total_filled_shares > 0:
                    trade["live_status"] = "FILLED"
                    is_p = total_filled_usd < target_usd * 0.99
                    trade["copy_status"] = f"{otype}_PARTIAL" if is_p else f"{otype}_FILLED"
                    log(f"LIVE copy window closed: covered ${total_filled_usd:.2f} of ${target_usd:.2f} ({total_filled_shares:.2f} shares)")
                else:
                    trade["live_status"], trade["copy_status"] = "TIMEOUT", f"{otype}_TOO_LATE"
                    log(f"LIVE copy stopped for {slug}: window expired")
                break

            attempt_num = int(trade.get("live_attempts") or 0) + 1
            trade["live_attempts"] = attempt_num

            if attempt_num > max_attempts:
                if total_filled_shares > 0:
                    trade["live_status"] = "FILLED"
                    is_p = total_filled_usd < target_usd * 0.99
                    trade["copy_status"] = f"{otype}_PARTIAL" if is_p else f"{otype}_FILLED"
                    log(f"LIVE copy max attempts reached ({max_attempts}): covered ${total_filled_usd:.2f} of ${target_usd:.2f} ({total_filled_shares:.2f} shares)")
                else:
                    trade["live_status"], trade["copy_status"] = "TIMEOUT", f"{otype}_MAX_RETRIES"
                    log(f"LIVE copy max attempts reached ({max_attempts}) for {slug} — halting to prevent duplicate orders")
                break

            # 2. Get freshest book ask for this token:
            # Attempt #1 uses paper entry / evaluated_price instantly with 0ms RAM lookup.
            # Subsequent attempts (re-quotes) MUST refresh the live ask from WebSocket and CLOB REST API!
            if attempt_num == 1 and paper > 0:
                current_ask = paper
            else:
                current_ask = None
                # Priority 1: WebSocket cache with relaxed age window
                if ws_client:
                    try:
                        summary = ws_client.get_summary(token_id, max_age_s=15.0)
                        if summary and summary.get("bestAsk"):
                            current_ask = float(summary["bestAsk"])
                    except Exception:
                        pass

                # Priority 2: Direct query to CLOB API to guarantee fresh on-exchange live price
                if not current_ask or current_ask <= 0:
                    try:
                        from bot.data import fetch_clob_price
                        clob_p = await fetch_clob_price(token_id, "buy")
                        if clob_p and clob_p > 0:
                            current_ask = float(clob_p)
                    except Exception:
                        pass

                # Priority 3: Raw WebSocket book without age filtering
                if (not current_ask or current_ask <= 0) and ws_client:
                    try:
                        raw_book = ws_client.get_token_market(token_id)
                        asks = raw_book.get("asks", [])
                        if asks:
                            current_ask = float(asks[0].get("px", asks[0]["price"]))
                    except Exception:
                        pass

                # Priority 4: Fallback to initial price
                if not current_ask or current_ask <= 0:
                    current_ask = paper or 0.50

            # 3. Progressive slippage sweep (from FAK-main: +1.5¢ per retry up to +8¢ extra)
            extra_slip = min(0.08, (attempt_num - 1) * 0.015)
            limit = order_price(current_ask, cfg, extra_slip=extra_slip, settings=settings)
            cap_c = cap_cents_for(current_ask, cfg, settings=settings)
            effective_slip_c = cap_c + (extra_slip * 100.0)

            # 4. Sizing: Instant RAM balance lookup and calculate remaining unfilled stake
            balance = float(state.get("live_balance") or 0.0)
            if balance <= 0:
                balance = float(await asyncio.to_thread(trader.get_usdc_balance) or 0.0)
                state["live_balance"] = balance

            remaining_target = max(0.0, target_usd - total_filled_usd) if total_filled_usd > 0 else target_usd
            usd, why = order_usd(remaining_target, limit, balance, cfg["fee_rate"])
            if usd <= 0:
                if total_filled_shares > 0:
                    trade["live_status"] = "FILLED"
                    is_p = total_filled_usd < target_usd * 0.99
                    trade["copy_status"] = f"{otype}_PARTIAL" if is_p else f"{otype}_FILLED"
                    log(f"LIVE copy completed for {slug}: covered ${total_filled_usd:.2f} of ${target_usd:.2f} ({total_filled_shares:.2f} shares) — {why}")
                else:
                    trade["live_status"], trade["copy_status"] = "INSUFFICIENT_FUNDS", f"{otype}_NO_FUNDS"
                    log(f"LIVE copy halted for {slug}: {why}")
                break

            trade["live_limit_price"] = limit
            trade["live_quoted_price"] = current_ask
            trade["live_order_usd"] = usd

            t_send = time.perf_counter()
            res = await place_async(trader, settings, token_id, usd, limit, order_type=otype)
            t_answer = time.perf_counter()
            send_ms = (t_answer - t_send) * 1000.0
            trade["live_answer_ms"] = round(send_ms, 1)

            if order_sent is not None:
                order_sent.set()

            oid = res.get("order_id")
            if oid:
                trade["live_order_id"] = oid

            # 5. Check for fill:
            fee = 0.0
            cur_filled = False
            cur_shares = 0.0
            cur_usd = 0.0
            if res.get("ok") and res.get("fill_size"):
                cur_shares = float(res["fill_size"])
                cur_usd = float(res.get("fill_usd") or (cur_shares * float(res.get("fill_price") or limit)))
                cur_filled = True
            elif res.get("ok"):
                fills = await order_fills(trader, oid, token_id, res.get("status"))
                if fills:
                    cur_shares = sum(float(f["size"]) for f in fills)
                    cur_usd = sum(float(f["size"]) * float(f["price"]) for f in fills)
                    cur_filled = True

            if cur_filled and cur_shares > 0 and cur_usd > 0:
                fee = book_fill(trade, cur_shares, cur_usd, settings=settings,
                                state=state, paper=paper, target_usd=target_usd)
                total_filled_shares += cur_shares
                total_filled_usd += cur_usd

                avg_price = float(trade.get("live_entry_price") or (cur_usd / cur_shares))
                cost = float(trade.get("live_amount") or total_filled_usd)
                rem_after = max(0.0, target_usd - total_filled_usd)
                min_req = max(1.0, limit * 5.0 if limit and limit > 0 else 1.0)
                is_done = (otype == "FOK") or (rem_after < min_req) or (total_filled_usd >= target_usd * 0.99)

                part = (f"; partial fill (${total_filled_usd:.2f} of ${target_usd:.2f})" if rem_after >= min_req else "")
                msg = (f"LIVE trade FILLED via {otype} (attempt #{attempt_num}): {side} ${cur_usd:.2f} on {slug} "
                       f"— got {cur_shares:.2f} shares @ {cur_usd/cur_shares:.4f} (total {total_filled_shares:.2f} shs @ avg {avg_price:.4f}; "
                       f"paper entry {paper:.4f}, order {trade.get('live_order_id')}); limit {limit:.4f} (ask {current_ask:.4f} + {effective_slip_c:.1f}¢); "
                       f"Polymarket answered in {send_ms:.0f} ms{part}"
                       + (f"; taker fee ${fee:.2f} counted, total cost ${cost:.2f}" if cfg["count_fee"] else ""))
                log(msg)

                try:
                    await asyncio.to_thread(save)
                except Exception:
                    try: save()
                    except Exception: pass
                try: sync()
                except Exception: pass
                if broadcast is not None:
                    try:
                        r = broadcast()
                        if asyncio.iscoroutine(r): await r
                    except Exception: pass

                if is_done:
                    if telegram is not None:
                        try:
                            r = telegram(f"🚀 *LIVE Copy FILLED (Attempt #{attempt_num})*\n• Type: `{otype}`\n• Side: `{side}`\n• Avg Price: `{avg_price:.4f}`\n"
                                         f"• Shares: `{total_filled_shares:.2f}`\n• Amount: `${cost:.2f}`\n• Market: `{slug}`\n• Paper Entry: `{paper:.4f}`")
                            if asyncio.iscoroutine(r): await r
                        except Exception: pass
                    return

                # If partial fill remains to be covered in FAK mode, log and continue immediately to cover the fill
                log(f"LIVE trade FAK partial fill: ${total_filled_usd:.2f} covered, remaining ${rem_after:.2f} — continuing retries to cover fill...")
                await asyncio.sleep(retry_interval)
                continue

            # 6. Not filled on this attempt — examine error and retry:
            err = str(res.get("error") or "unfilled")
            try:
                err = trader.enrich_order_error(err) or err
            except Exception:
                pass

            # Fatal account/configuration errors abort immediately
            if any(k in err.lower() for k in ("setup_failed", "client_not_ready", "invalid_private_key", "not enough balance", "balance is not enough")):
                if total_filled_shares > 0:
                    trade["live_status"] = "FILLED"
                    is_p = total_filled_usd < target_usd * 0.99
                    trade["copy_status"] = f"{otype}_PARTIAL" if is_p else f"{otype}_FILLED"
                    log(f"LIVE copy stopped for {slug}: {err} — partial fill preserved (${total_filled_usd:.2f} covered)")
                else:
                    trade["live_status"], trade["copy_status"] = "FAILED", f"{otype}_FAILED"
                    trade["live_error"] = err
                    log(f"LIVE copy aborted: {err}")
                break

            if total_filled_shares > 0:
                log(f"LIVE copy attempt #{attempt_num} for {side} @ {current_ask:.4f} [{otype}]: {err} (covered ${total_filled_usd:.2f}/${target_usd:.2f}, limit {limit:.4f}) — retrying to cover fill in {int(retry_interval * 1000)}ms...")
            else:
                log(f"LIVE copy attempt #{attempt_num} for {side} @ {current_ask:.4f} [{otype}]: {err} (limit {limit:.4f} = ask + {effective_slip_c:.1f}¢) — retrying {otype} in {int(retry_interval * 1000)}ms...")
            await asyncio.sleep(retry_interval)

    except Exception as e:
        if total_filled_shares > 0:
            trade["live_status"] = "FILLED"
            is_p = total_filled_usd < target_usd * 0.99
            trade["copy_status"] = f"{otype}_PARTIAL" if is_p else f"{otype}_FILLED"
            log(f"LIVE copy exception for {slug} ({side}): {type(e).__name__}: {e} — partial fill preserved (${total_filled_usd:.2f} covered)")
        else:
            trade["live_status"], trade["copy_status"] = "FAILED", f"{otype}_FAILED"
            trade["live_error"] = f"{type(e).__name__}: {e}"
            log(f"LIVE copy exception for {slug} ({side}): {type(e).__name__}: {e}")
    finally:
        if order_sent is not None:
            order_sent.set()

    # If loop exited without filling
    if trade.get("live_status") == "FILLING":
        if total_filled_shares > 0:
            trade["live_status"] = "FILLED"
            is_p = total_filled_usd < target_usd * 0.99
            trade["copy_status"] = f"{otype}_PARTIAL" if is_p else f"{otype}_FILLED"
            log(f"LIVE copy completed for {slug} ({side}): covered ${total_filled_usd:.2f} of ${target_usd:.2f} ({total_filled_shares:.2f} shares)")
        else:
            trade["live_status"], trade["copy_status"] = "MISSED", f"{otype}_MISSED"
            log(f"LIVE copy MISSED for {slug} ({side}): max attempts ({max_attempts}) reached without fill (last ask {current_ask:.4f}, limit {limit or 0:.4f})")

    try:
        await asyncio.to_thread(save)
    except Exception:
        try: save()
        except Exception: pass
    try: sync()
    except Exception: pass
    if broadcast is not None:
        try:
            r = broadcast()
            if asyncio.iscoroutine(r): await r
        except Exception: pass

    try:
        await asyncio.to_thread(save)
    except Exception:
        try: save()
        except Exception: pass
    try: sync()
    except Exception: pass
    if broadcast is not None:
        try:
            r = broadcast()
            if asyncio.iscoroutine(r): await r
        except Exception: pass
