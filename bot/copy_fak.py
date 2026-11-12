"""
Fill-and-kill live copy (PATCH: fill-and-kill copy).

The bot's own live copy is a GTD limit order priced above the paper entry (Max Slippage). What
doesn't fill at once rests on the book, and the order is re-quoted higher. With "enabled" on, a
regular trade's live copy is ONE fill-and-kill BUY instead:

  * its price is the paper entry + a cap: cap_cents, or high_cap_cents when the paper entry is
    above high_from_price;
  * its size is in dollars: the bot's own stake (its Risk setting), or stake_pct % of the live
    balance;
  * it takes what is offered at that price or better at that moment and Polymarket cancels the
    rest. Nothing is left on the book, nothing is re-quoted, and what is booked is what that one
    order filled;
  * count_fee books Polymarket's taker fee with the fill (shares x fee_rate x price x (1 - price),
    paid in cash on top of the fill): the trade's live amount is then what left the wallet, and
    its live profit/loss follows from it.

Everything here is plain functions, so it runs and tests without the bot. main.py calls
live_copy() at the top of _continuous_live_copy_fill and passes in the bot's own pieces (its
state, its order client, its log / save / broadcast / Telegram functions).

Settings live in config.json under "copy_fak" (settings.COPY_FAK). Missing keys fall back to
DEFAULTS; normalize() checks types and ranges before anything is saved.
"""

import asyncio
import math
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

# ── Settings ─────────────────────────────────────────────────────────────────────────────
DEFAULTS: Dict[str, Any] = {
    "enabled": True,           # the live copy is one fill-and-kill order (enabled by default)
    "cap_cents": 0.0,          # pay at most the paper entry + this many cents (+0¢ base cap)
    "high_cap_cents": 5.0,     # ... or + this many when the paper entry is above high_from_price (+5¢ high cap)
    "high_from_price": 0.55,   # paper entries above this price use high_cap_cents (55¢ threshold)
    "stake_pct": 19.0,         # % of the live balance per trade (19% optimal compounding)
    "count_fee": True,         # book Polymarket's taker fee with each fill (true net balance tracking)
    "fee_rate": 0.07,          # Polymarket's taker fee rate for crypto markets
}

# key: (lowest, highest, what a refusal calls it)
RANGES: Dict[str, Tuple[float, float, str]] = {
    "cap_cents": (0.0, 99.0, "the cap must be 0 to 99 cents"),
    "high_cap_cents": (0.0, 99.0, "the cap for higher-priced entries must be 0 to 99 cents"),
    "high_from_price": (0.01, 1.0, "the price the higher cap starts above must be 0.01 to 1"),
    "stake_pct": (0.0, 100.0, "the bet size must be 0 to 100 percent of the balance"),
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
    for k in BOOLS:
        if k in cfg:
            try:
                out[k] = as_bool(cfg[k])
            except ValueError:
                pass
    for k, (lo, hi, _) in RANGES.items():
        x = _number(cfg.get(k))
        if x is not None and lo <= x <= hi:
            out[k] = x
    return out


def normalize(new: Any, current: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """The settings to save: `current` (what is in use) with `new` (what the Settings page or
    config.json gives) on top, every value checked. Raises ValueError with a plain reason.
    A number left blank keeps the value in use."""
    if new is None:
        new = {}
    if not isinstance(new, dict):
        raise ValueError("copy_fak must be an object")
    out = merged(current)
    for k in BOOLS:
        if k in new and new[k] is not None and new[k] != "":
            out[k] = as_bool(new[k], f"copy_fak.{k}")
    for k, (lo, hi, reason) in RANGES.items():
        if k not in new or new[k] is None or (isinstance(new[k], str) and not new[k].strip()):
            continue
        x = _number(new[k])
        if x is None or not (lo <= x <= hi):
            raise ValueError(reason)
        out[k] = x
    return out


def on(cfg: Optional[Dict[str, Any]]) -> bool:
    """True when the live copy is the fill-and-kill order."""
    return bool(merged(cfg)["enabled"])


def _cents(x: float) -> str:
    return f"{round(float(x), 3):g}¢"


def _px(x: float) -> str:
    """A price as the log shows it: 0.52, or 0.965 when it isn't a whole cent."""
    x = float(x)
    return f"{x:.2f}" if abs(x * 100.0 - round(x * 100.0)) < 1e-6 else f"{x:.3f}"


def status_line(cfg: Optional[Dict[str, Any]]) -> str:
    """One log line that says what the settings do."""
    c = merged(cfg)
    if not c["enabled"]:
        return "Fill-and-kill copy: OFF (the live copy is the GTD loop)"
    caps = f"paper entry + {_cents(c['cap_cents'])}"
    if c["high_from_price"] < 1.0 and c["high_cap_cents"] != c["cap_cents"]:
        caps += f", + {_cents(c['high_cap_cents'])} for entries above {_cents(c['high_from_price'] * 100.0)}"
    stake = f"{c['stake_pct']:g}% of the live balance" if c["stake_pct"] > 0 else "the bot's own stake (Risk setting)"
    fee = f"taker fee counted (rate {c['fee_rate']:g})" if c["count_fee"] else "taker fee not counted"
    return f"Fill-and-kill copy: ON — one order at {caps}; bet size: {stake}; {fee}"


# ── Price, size, fee ─────────────────────────────────────────────────────────────────────
def _whole_cent(x: float) -> bool:
    return abs(x * 100.0 - round(x * 100.0)) < 1e-6


def cap_cents_for(paper: float, cfg: Optional[Dict[str, Any]]) -> float:
    """The cap, in cents, for a paper entry at this price."""
    c = merged(cfg)
    return c["high_cap_cents"] if float(paper) > c["high_from_price"] + 1e-9 else c["cap_cents"]


def order_price(paper: float, cfg: Optional[Dict[str, Any]]) -> Optional[float]:
    """The order's limit price: the paper entry + its cap, rounded DOWN to the book's price grid
    and kept inside it. The grid is whole cents (0.01 .. 0.99); when the paper entry itself is
    not a whole cent, the book is on tenths of a cent (0.001 .. 0.999). None without a price."""
    try:
        paper = float(paper)
    except (TypeError, ValueError):
        return None
    if not (0.0 < paper < 1.0):
        return None
    tick = 0.01 if _whole_cent(paper) else 0.001
    raw = paper + cap_cents_for(paper, cfg) / 100.0
    px = math.floor(raw / tick + 1e-6) * tick
    return round(min(max(px, tick), 1.0 - tick), 3)


def taker_fee(shares: float, price: float, rate: float) -> float:
    """Polymarket's taker fee in dollars: shares x rate x price x (1 - price), to 5 decimals."""
    p = min(max(float(price), 0.0), 1.0)
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
    trader.warm_token(token_id, tick)
    side_int, maker_amt, taker_amt = trader.clob.builder.get_market_order_amounts(
        "BUY", round(float(usd), 2), round(float(price), 3), ROUNDING_CONFIG[tick])
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


def place(trader: Any, settings: Any, token_id: str, usd: float, price: float) -> Dict[str, Any]:
    """Send the fill-and-kill BUY and wait for Polymarket's answer (blocking: run it on a thread)."""
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
        side_int, maker_amt, taker_amt = order_amounts(trader, token_id, usd, price)
        data = trader._post_order_fast(token_id=str(token_id), maker_amount=maker_amt, taker_amount=taker_amt,
                                       side_int=side_int, order_type_str="FAK", expiration_str="0", neg_risk=False)
        return read_answer(data, maker_amt, taker_amt)
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _thread_runner(trader: Any) -> Callable:
    """The bot's own order threads when it has them (run_io), a plain worker thread otherwise."""
    return getattr(trader, "run_io", None) or (lambda fn, *a: asyncio.to_thread(fn, *a))


async def place_async(trader: Any, settings: Any, token_id: str, usd: float, price: float) -> Dict[str, Any]:
    """place() from the event loop. When the bot's order library can send from its own threads
    (rust_clob's post_order_async) the order is signed right here and leaves at once; otherwise
    place() runs on a thread. Same result dict."""
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
        return await run(place, trader, settings, token_id, usd, price)
    try:
        side_int, maker_amt, taker_amt = order_amounts(trader, token_id, usd, price)
        if two_step:
            post_args = trader._post_args(token_id, maker_amt, taker_amt, side_int, "FAK", "0", False, False)
            try:
                ticket = mod.post_order_async(*post_args)
            except TypeError:
                try:
                    ticket = mod.post_order_async(*post_args[:-1])
                except TypeError:
                    ticket = mod.post_order_async(*post_args[:-2])
        else:
            ticket = mod.post_order_async(str(token_id), int(maker_amt), int(taker_amt), int(side_int),
                                          "FAK", "0", False, False, False)
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
              paper: float, asked_usd: Optional[float] = None) -> float:
    """Book a fill on the trade and take what it cost off the live balance in RAM (and work out
    the bot's next stake from what is left, as the bot's own copy does). With count_fee on, the
    cost includes Polymarket's taker fee. Returns the fee booked (0.0 when it isn't counted)."""
    cfg = merged(getattr(settings, "COPY_FAK", None))
    price = (usd / shares) if shares > 0 else float(trade.get("live_limit_price") or paper or 0.0)
    fee = taker_fee(shares, price, cfg["fee_rate"]) if cfg["count_fee"] else 0.0
    cost = usd + fee
    trade["live_status"] = "FILLED"
    trade["live_entry_price"] = price
    trade["live_shares"] = shares
    trade["live_amount"] = cost
    trade["live_entry_time"] = datetime.now().isoformat()
    trade["live_quoted_price"] = paper
    trade["live_slippage"] = price - paper
    if cfg["count_fee"]:
        trade["live_fee"] = fee
    partial = asked_usd is not None and usd < float(asked_usd) * 0.995
    trade["copy_status"] = "FAK_PARTIAL" if partial else "FAK_FILLED"
    if cost > 0:
        new_bal = max(0.0, float(state.get("live_balance") or 0.0) - cost)
        state["live_balance"] = new_bal
        state["precomputed_live_stake"] = bot_stake(new_bal, settings)
    return fee


async def live_copy(trade: Dict[str, Any], evaluated_price: Optional[float] = None, *, settings: Any,
                    state: Dict[str, Any], trader: Any, log: Callable[[str], Any], save: Callable[[], Any],
                    sync: Callable[[], Any], broadcast: Callable[[], Any], telegram: Callable[[str], Any],
                    order_sent: Optional[Any] = None) -> None:
    """The live copy of a trade as ONE fill-and-kill BUY: up to the stake in dollars at the paper
    entry + the cap, or better. live_status afterwards: FILLED (all of it or part: copy_status
    FAK_FILLED / FAK_PARTIAL), MISSED (nothing was offered at that price), INSUFFICIENT_FUNDS,
    TIMEOUT (the window was over) or FAILED (live_error). It is never sent twice."""
    if trade.get("status") != "OPEN" or trade.get("live_status") != "FILLING":
        return                                   # closed before the copy started: nothing to send
    cfg = merged(getattr(settings, "COPY_FAK", None))
    side, token_id, slug = trade.get("side"), trade.get("token_id"), trade.get("market_slug")
    paper = float(trade.get("paper_entry_price") or evaluated_price or 0.0)
    cap_c = cap_cents_for(paper, cfg) if paper > 0 else cfg["cap_cents"]
    limit = usd = send_ms = fee = None
    note = ""
    t_start = time.perf_counter()
    try:
        limit = order_price(paper, cfg)
        if trade.get("end_ts") and time.time() >= float(trade["end_ts"]):
            trade["live_status"], trade["copy_status"], note = "TIMEOUT", "FAK_TOO_LATE", "window expired"
        elif limit is None:
            trade["live_status"], trade["copy_status"] = "FAILED", "FAK_FAILED"
            trade["live_error"] = note = "no paper entry price"
        else:
            balance = float(state.get("live_balance") or 0.0)
            if balance <= 0:
                balance = float(await asyncio.to_thread(trader.get_usdc_balance) or 0.0)
                state["live_balance"] = balance
            usd, why = order_usd(stake_usd(balance, state.get("precomputed_live_stake"), settings, cfg),
                                 limit, balance, cfg["fee_rate"])
            if usd <= 0:
                trade["live_status"], trade["copy_status"], note = "INSUFFICIENT_FUNDS", "FAK_NO_FUNDS", why
            else:
                trade["live_attempts"] = int(trade.get("live_attempts") or 0) + 1
                trade["live_limit_price"] = limit
                trade["live_quoted_price"] = paper
                trade["live_order_usd"] = usd
                t_send = time.perf_counter()
                res = await place_async(trader, settings, token_id, usd, limit)
                t_answer = time.perf_counter()
                send_ms = (t_answer - t_send) * 1000.0
                trade["live_answer_ms"] = round((t_answer - t_start) * 1000.0, 1)
                if order_sent is not None:
                    order_sent.set()
                if res.get("order_id"):
                    trade["live_order_id"] = res["order_id"]
                if res.get("ok") and res.get("fill_size"):
                    fee = book_fill(trade, float(res["fill_size"]), float(res["fill_usd"]), settings=settings,
                                    state=state, paper=paper, asked_usd=usd)
                elif res.get("ok"):
                    fills = await order_fills(trader, res.get("order_id"), token_id, res.get("status"))
                    if fills:
                        fee = book_fill(trade, sum(float(f["size"]) for f in fills),
                                        sum(float(f["size"]) * float(f["price"]) for f in fills),
                                        settings=settings, state=state, paper=paper, asked_usd=usd)
                    else:
                        trade["live_status"], trade["copy_status"] = "MISSED", "FAK_MISSED"
                        note = f"accepted ({res.get('status')}), no fill found"
                else:
                    err = str(res.get("error") or "order_failed")
                    try:
                        err = trader.enrich_order_error(err) or err
                    except Exception:
                        pass
                    if any(k in err.lower() for k in NO_MATCH):
                        trade["live_status"], trade["copy_status"] = "MISSED", "FAK_MISSED"
                        note = f"nothing offered at {_px(limit)} or better"
                    else:
                        trade["live_status"], trade["copy_status"] = "FAILED", "FAK_FAILED"
                        trade["live_error"] = note = err
    except Exception as e:
        trade["live_status"], trade["copy_status"] = "FAILED", "FAK_FAILED"
        trade["live_error"] = note = f"{type(e).__name__}: {e}"
    finally:
        if order_sent is not None:
            order_sent.set()

    st = trade.get("live_status")
    how = (f"limit {_px(limit)} = paper entry {paper:.4f} + {_cents(cap_c)}" if limit is not None
           else f"paper entry {paper:.4f}")
    how += f"; asked ${usd:.2f}" if usd else ""
    how += f"; Polymarket answered in {send_ms:.0f} ms" if send_ms is not None else ""
    try:
        if st == "FILLED":
            got = float(trade["live_shares"]) * float(trade["live_entry_price"])
            part = (f"; part of the order: ${got:.2f} of ${usd:.2f} was offered at {_px(limit)} or better"
                    if trade.get("copy_status") == "FAK_PARTIAL" and usd else "")
            log(f"LIVE trade FILLED via fill-and-kill (one order): {side} ${got:.2f} on {slug} "
                f"— {float(trade['live_shares']):.2f} shares @ {float(trade['live_entry_price']):.4f} "
                f"(paper entry {paper:.4f}, order {trade.get('live_order_id')}); {how}{part}"
                + (f"; taker fee ${fee:.2f} counted, cost ${float(trade['live_amount']):.2f}" if cfg["count_fee"] else ""))
        elif st == "MISSED":
            log(f"LIVE copy MISSED for {slug} ({side}): {note} — fill-and-kill, not sent again ({how})")
        elif st == "INSUFFICIENT_FUNDS":
            log(f"LIVE copy halted for {slug}: {note}")
        elif st == "TIMEOUT":
            log(f"LIVE copy stopped for {slug}: {note}")
        else:
            log(f"LIVE copy FAILED for {slug} ({side}): {note} (fill-and-kill; {how})")
        if trade.get("status") != "OPEN" and st == "FILLED":
            log(f"LIVE copy note for {slug}: the trade was closed while its order was on the way; "
                f"the {float(trade['live_shares']):.2f} shares it bought are in the wallet")
    except Exception:
        pass
    try:
        await asyncio.to_thread(save)
        sync()
        await broadcast()
        if st == "FILLED":
            await telegram(f"🚀 *LIVE Copy FILLED (Fill-and-Kill)*\n• Side: `{side}`\n• Fill Price: `{float(trade['live_entry_price']):.4f}`"
                           f"\n• Shares: `{float(trade['live_shares']):.2f}`\n• Amount: `${float(trade['live_amount']):.2f}`"
                           f"\n• Market: `{slug}`\n• Paper Entry: `{paper:.4f}`")
    except Exception:
        pass
