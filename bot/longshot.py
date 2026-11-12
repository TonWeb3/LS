"""
Long-shot mode (PATCH: longshot).

A second entry path, separate from the regular EV strategy. It only looks at the end of
each window and buys a cheap side when the model's chance for that side is high enough
against its ask. It has its own slot (it can trade in a window where the regular strategy
already holds a position), its own stake, its own price cap and its own model.

Everything in this file is plain functions plus a small price recorder, so it runs and
tests without the network. main.py wires it in (evaluate_longshot, execute_trade,
_continuous_live_copy_fill).

Settings live in config.json under "longshot". Missing keys fall back to DEFAULTS;
normalize() checks types and ranges before anything is saved.

PATCH: longshot maker — the bands take the ask, so they pay Polymarket's taker fee:
their chance ÷ price and chance − price use the price plus that fee (taker_fee_rate).
The "maker" block is a third way in: a resting post-only bid (no fee) that bot/ls_maker.py
keeps on the book while the model gives that side at least maker.min_prob.
"""

import copy
import math
import threading
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

# ── Settings ──────────────────────────────────────────────────────────────────
DEFAULTS: Dict[str, Any] = {
    "max_per_window": 1,       # long-shot trades per 15m window
    "look_s": 60,              # final minute look window in seconds
    "cutoff_s": 3.0,           # no new long shots in the last N seconds
    "follow": "bet",           # PATCH: longshot follow — "bet", "prediction", "either" or "off" (see FOLLOW_MODES)
    "sniper_stake_pct": 1.0,   # PATCH: longshot stake — % of the balance per sniper buy; 0 = the GTD stake
    "maker_stake_pct": 1.0,    # PATCH: longshot stake — % of the balance per maker bid; 0 = the GTD stake
    "sniper_enabled": True,    # PATCH: longshot strategies — the final-minute sniper buys on/off
    "maker_enabled": True,     # PATCH: longshot strategies — the resting maker bids on/off
    "engine_enabled": False,   # PATCH: longshot engine — the GTD engine's own cheap entries as long shots (the paper bot's kind)
    "engine_max_price": 0.10,  # PATCH: longshot engine — a GTD entry at this ask or less is a long shot
    "engine_min_seconds_left": 2.0,   # PATCH: longshot engine — none with less time left (the paper bot's latest: 2.1 s)
    "engine_cap_cents": 2.0,   # PATCH: longshot engine — the live order: one fill-and-kill at the paper entry + this
    "engine_stake_pct": 0.0,   # PATCH: longshot engine — % of the balance per engine long shot; 0 = the GTD stake
    "engine_own_slot": True,   # PATCH: longshot engine — next to the window's regular trade; False = only with no open position (as TOG)
    "maker": {
        "price": 0.01,         # resting maker bid price (1¢)
    },
    "bands": {
        "final": {
            "ask_min": 0.01,
            "ask_max": 0.03,
            "max_price": 0.04,
        },
    },
}

BAND_TEMPLATE = {"ask_min": 0.01, "ask_max": 0.03, "max_price": 0.04}

MAKER_DEFAULTS = DEFAULTS["maker"]

ORDER_TYPES = ("GTD",)

# PATCH: longshot follow — which side a long shot (sniper buy or maker bid) may take:
#   "bet"        the side of the window's first regular GTD trade; no long shot without one
#   "prediction" the side the GTD model gives more than 50% at that moment
#   "either"     the GTD bet's side or the predicted side (both when they differ)
#   "off"        either side, as before
FOLLOW_MODES = ("bet", "prediction", "either", "off")


def as_bool(v: Any, label: str = "value") -> bool:
    """true/false from JSON or a hand edit: "false", "0", "no", "off" are False (bool("false") is True)."""
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
    raise ValueError(f"{label} must be true or false")


def merged(user_cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """DEFAULTS overlaid with the user's settings (bands merged one level deep)."""
    out = copy.deepcopy(DEFAULTS)
    for k, v in (user_cfg or {}).items():
        if k == "bands" and isinstance(v, dict):
            for name, band in v.items():
                base = out["bands"].get(name, dict(BAND_TEMPLATE))
                if isinstance(band, dict):
                    base = {**base, **band}
                out["bands"][name] = base
        elif k == "maker" and isinstance(v, dict):   # PATCH: longshot maker — missing keys keep defaults
            out["maker"] = {**out["maker"], **v}
        else:
            out[k] = v
    return out


def normalize(user_cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """merged() plus type/range checks. Raises ValueError listing every bad field."""
    cfg = merged(user_cfg)
    errs: List[str] = []

    def num(obj, key, label, lo=None, hi=None, as_int=False):
        try:
            v = int(obj[key]) if as_int else float(obj[key])
        except (TypeError, ValueError):
            errs.append(f"{label} must be a number")
            return
        if (lo is not None and v < lo) or (hi is not None and v > hi):
            errs.append(f"{label} must be between {lo} and {hi}")
            return
        obj[key] = v

    num(cfg, "max_per_window", "max per window", 0, 100, as_int=True)
    num(cfg, "look_s", "look window", 1, 900)
    cfg["order_type"] = "GTD"
    follow = str(cfg.get("follow") or "bet").strip().lower()      # PATCH: longshot follow
    if follow in FOLLOW_MODES:
        cfg["follow"] = follow
    else:
        errs.append('follow must be "bet", "prediction", "either" or "off"')
    num(cfg, "sniper_stake_pct", "sniper stake %", 0, 100)      # PATCH: longshot stake
    num(cfg, "maker_stake_pct", "maker stake %", 0, 100)
    num(cfg, "engine_max_price", "engine long shots max price", 0.001, 0.99)   # PATCH: longshot engine
    num(cfg, "engine_min_seconds_left", "engine long shots min seconds left", 0, 900)
    num(cfg, "engine_cap_cents", "engine long shots live cap (cents)", 0, 99)
    num(cfg, "engine_stake_pct", "engine long shots stake %", 0, 100)
    for key, label in (("sniper_enabled", "sniper"), ("maker_enabled", "maker bids"),
                       ("engine_enabled", "engine long shots"), ("engine_own_slot", "engine long shots own slot")):   # PATCH: longshot strategies (+ PATCH: longshot engine)
        v = cfg.get(key, True)
        if isinstance(v, str):
            v = {"true": True, "on": True, "yes": True, "1": True,
                 "false": False, "off": False, "no": False, "0": False}.get(v.strip().lower(), v)
        if v in (True, False):
            cfg[key] = bool(v)
        else:
            errs.append(f"{label} on/off must be true or false")

    mk = cfg.get("maker")
    if isinstance(mk, dict):
        if "price" in mk:
            num(mk, "price", "maker.price", 0.001, 0.5)

    bands = cfg.get("bands")
    if isinstance(bands, dict):
        for name, b in bands.items():
            if isinstance(b, dict):
                if "ask_min" in b: num(b, "ask_min", f"{name}.ask_min", 0, 1)
                if "ask_max" in b: num(b, "ask_max", f"{name}.ask_max", 0, 1)
                if "max_price" in b: num(b, "max_price", f"{name}.max_price", 0, 1)
                if "ask_min" in b and "ask_max" in b and float(b["ask_min"]) > float(b["ask_max"]):
                    errs.append(f"{name}: ask_min is above ask_max")

    if errs:
        raise ValueError("; ".join(errs))
    return cfg


def describe(cfg: Dict[str, Any]) -> str:
    """One log line with the active settings."""
    parts = []
    for name, b in (cfg.get("bands") or {}).items():
        if b.get("enabled", True):
            parts.append(f"{name} {float(b.get('ask_min', 0.01))*100:g}–{float(b.get('ask_max', 0.03))*100:g}¢ (cap {float(b.get('max_price', 0.04))*100:g}¢)")
    mk = cfg.get("maker") or {}
    maker_str = f" | maker {float(mk.get('price', 0.01))*100:g}¢ bid" if maker_on(cfg) else " | maker bids off"
    if not sniper_on(cfg):                            # PATCH: longshot strategies
        parts = ["sniper off"]
    follow = {"bet": ", follows the GTD bet", "prediction": ", follows the GTD prediction",
              "either": ", follows the GTD bet or prediction"}.get(cfg.get("follow"), ", any side")   # PATCH: longshot follow
    return (f"stake {stake_text(cfg)}, GTD, max {cfg.get('max_per_window', 1)}/window, last {cfg.get('look_s', 60)}s{follow} | "
            + ("; ".join(parts) or "no bands") + maker_str + engine_text(cfg))   # PATCH: longshot engine


def max_look_s(cfg: Dict[str, Any]) -> float:
    look = float(cfg.get("look_s", 60.0))
    for b in (cfg.get("bands") or {}).values():
        if b.get("enabled", True):
            look = max(look, float(b.get("look_s", look)))
    return look


# PATCH: longshot maker — the checks run while either the bands or the maker bids look
def sniper_on(cfg: Dict[str, Any]) -> bool:
    """PATCH: longshot strategies — the final-minute sniper buys are on (longshot.sniper_enabled)."""
    return bool(cfg.get("sniper_enabled", True))


def status_line(cfg: Dict[str, Any]) -> str:
    """PATCH: longshot strategies — the LONGSHOT log line: what runs, or off."""
    if not (sniper_on(cfg) or maker_on(cfg)):
        if engine_on(cfg):   # PATCH: longshot engine — the engine long shots run on their own
            return "LONGSHOT: on — " + describe(cfg)
        return "LONGSHOT: off (sniper and maker bids both switched off)"
    return "LONGSHOT: on — " + describe(cfg)


# ── Engine long shots (PATCH: longshot engine) ────────────────────────────────
def engine_on(cfg: Dict[str, Any]) -> bool:
    """The GTD engine's own cheap entries are taken as long shots (longshot.engine_enabled)."""
    return bool((cfg or {}).get("engine_enabled", False))


def engine_text(cfg: Dict[str, Any]) -> str:
    """The status line's engine part: " | engine long shots ≤10¢ (live +2¢, to 2s, GTD stake, own slot)" or off."""
    if not engine_on(cfg):
        return " | engine long shots off"
    pct = stake_pct(cfg, "engine_stake_pct")
    stake = f"{pct:g}% balance" if pct > 0 else f"GTD stake ({gtd_stake_text()})"
    slot = "own slot" if cfg.get("engine_own_slot", True) else "one position per window"
    return (f" | engine long shots ≤{float(cfg.get('engine_max_price', 0.10)) * 100:g}¢ "
            f"(live +{float(cfg.get('engine_cap_cents', 2.0)):g}¢, to {float(cfg.get('engine_min_seconds_left', 2.0)):g}s, {stake}, {slot})")


def engine_stake_for(balance: float, cfg: Optional[Dict[str, Any]] = None) -> float:
    """Dollars for one engine long shot: longshot.engine_stake_pct % of the balance, or the bot's
    risk value (the GTD stake, as the paper bot) when that is 0. Never under $1."""
    pct = stake_pct(cfg, "engine_stake_pct")
    if pct > 0:
        bal = max(0.0, float(balance or 0.0))
        return max(1.0, min(pct / 100.0 * bal, bal)) if bal >= 1.0 else 0.0
    return stake_for(balance, {"sniper_stake_pct": 0})


def engine_order_price(ask: float, cfg: Optional[Dict[str, Any]] = None) -> float:
    """The fill-and-kill price of an engine long shot: the paper entry (the ask) + engine_cap_cents,
    rounded down to the book's tick (0.1¢ under 4¢, 1¢ from 4¢); 0.1¢ to 99¢."""
    cents = max(0.0, float((cfg or {}).get("engine_cap_cents", 0.0) or 0.0))
    raw = float(ask) + cents / 100.0
    t = 0.001 if raw < 0.04 else 0.01
    px = math.floor(raw / t + 1e-9) * t
    return round(min(0.99, max(0.001, px)), 3)


def maker_on(cfg: Dict[str, Any]) -> bool:
    if not cfg.get("maker_enabled", True):          # PATCH: longshot strategies — maker bids switched off
        return False
    mk = cfg.get("maker")
    if not isinstance(mk, dict):
        return False
    return float(mk.get("price", 0.0) or 0.0) > 0


def maker_live(cfg: Dict[str, Any]) -> bool:
    return True


def eval_look_s(cfg: Dict[str, Any]) -> float:
    look = max_look_s(cfg)
    if maker_on(cfg):
        look = max(look, float((cfg.get("maker") or {}).get("look_s", look)))
    return look


def eval_cutoff_s(cfg: Dict[str, Any]) -> float:
    return float(cfg.get("cutoff_s", 3.0))


def max_band_ask(cfg: Dict[str, Any]) -> float:
    vals = [float(b.get("ask_max", 0)) for b in (cfg.get("bands") or {}).values() if b.get("enabled", True)]
    return max(vals) if vals else 0.04


# ── Settlement-feed price recorder (for the 60-second average) ────────────────
_lock = threading.Lock()
_samples: Deque[Tuple[float, float]] = deque(maxlen=5000)   # (unix seconds, price)


def record_price(ts: float, price: Optional[float]) -> None:
    """Store a settlement-feed price. Out-of-order or duplicate timestamps are ignored."""
    try:
        ts, price = float(ts), float(price or 0)
    except (TypeError, ValueError):
        return
    if price <= 0:
        return
    with _lock:
        if _samples and ts <= _samples[-1][0]:
            return
        _samples.append((ts, price))
        cutoff = ts - 1200.0
        while _samples and _samples[0][0] < cutoff:
            _samples.popleft()


def samples() -> List[Tuple[float, float]]:
    with _lock:
        return list(_samples)


def clear_samples() -> None:
    with _lock:
        _samples.clear()
    _ptb_cache.clear()


def time_weighted_mean(pts: List[Tuple[float, float]], a: float, b: float) -> Tuple[Optional[float], float]:
    """Mean over [a, b] of the step line through pts (each price holds until the next one),
    and how many seconds of [a, b] that line covers (nothing before the first sample)."""
    if b <= a or not pts:
        return None, 0.0
    total = covered = 0.0
    cur_t = cur_p = None
    for t, p in pts:
        if t <= a:
            cur_t, cur_p = t, p
            continue
        if t >= b:
            break
        if cur_p is not None:
            seg = t - max(a, cur_t)
            if seg > 0:
                total += cur_p * seg
                covered += seg
        cur_t, cur_p = t, p
    if cur_p is not None:
        seg = b - max(a, cur_t)
        if seg > 0:
            total += cur_p * seg
            covered += seg
    return (total / covered, covered) if covered > 0 else (None, 0.0)


def sampled_twap(end_ts: float, pts: Optional[List[Tuple[float, float]]] = None,
                 min_cover_s: float = 50.0) -> Optional[float]:
    """The 60-second average ending at end_ts, from recorded prices; None if under min_cover_s covered."""
    mean, cov = time_weighted_mean(samples() if pts is None else pts, end_ts - 60.0, end_ts)
    return mean if (mean is not None and cov >= min_cover_s) else None


_ptb_cache: Dict[int, Optional[float]] = {}


def price_to_beat(start_ts: float, now_ts: float) -> Optional[float]:
    """Sampled 60-second average at the window start. Cached once the start is 5 s past."""
    key = int(round(start_ts))
    if key in _ptb_cache:
        return _ptb_cache[key]
    val = sampled_twap(start_ts)
    if now_ts >= start_ts + 5.0:
        _ptb_cache[key] = val
        for k in [k for k in _ptb_cache if k < key - 7200]:
            del _ptb_cache[k]
    return val


# ── Models ────────────────────────────────────────────────────────────────────
def _phi(z: float) -> float:
    return 0.5 * math.erfc(-z / math.sqrt(2.0))


def vol_usd_per_sqrt_s(cfg: Dict[str, Any], price: float, sigma_5m: Optional[float]) -> float:
    fixed = float(cfg.get("vol_usd_per_sqrt_s") or 0.0)
    if fixed > 0:
        return fixed
    if sigma_5m and sigma_5m > 0 and price and price > 0:
        return sigma_5m * price / math.sqrt(300.0)
    return float(cfg.get("vol_fallback_usd_per_sqrt_s") or 6.0)


def prob_up_twap(now_ts: float, end_ts: float, strike: float, price_now: float,
                 sigma_usd: float, sd_floor: float,
                 pts: Optional[List[Tuple[float, float]]] = None) -> Dict[str, float]:
    """Chance that the closing 60-second average ends at or above `strike`.

    N seconds before the close (N <= 60), (60-N)/60 of the average is already fixed: it is
    taken from the recorded prices. The rest is a random walk from price_now, whose average
    has standard deviation sigma * sqrt(N^3/3) / 60. With more than a minute left the whole
    average is still ahead: sigma * sqrt(N - 40)."""
    n = max(0.0, end_ts - now_ts)
    if n <= 60.0:
        elapsed = 60.0 - n
        obs, cov = time_weighted_mean(samples() if pts is None else pts, end_ts - 60.0, now_ts)
        if obs is None or cov < 0.5 * elapsed:
            obs = price_now          # too little history: treat the elapsed part as flat at price_now
        exp_avg = (obs * elapsed + price_now * n) / 60.0
        sd = sigma_usd * math.sqrt(n ** 3 / 3.0) / 60.0
    else:
        exp_avg = price_now
        sd = sigma_usd * math.sqrt(n - 40.0)
    sd = max(sd, float(sd_floor or 0.0), 1e-9)
    p = _phi((exp_avg - strike) / sd)
    return {"p_up": p, "expected_avg": exp_avg, "sd": sd, "secs_left": n}


def prob_up_exact(spot: float, strike: float, secs_left: float,
                  sigma_5m: Optional[float], drift_5m: float = 0.0) -> float:
    """The bot's own GBM formula, run on the real time left (fractions of a 5-minute step)."""
    if not spot or not strike or spot <= 0 or strike <= 0:
        return 0.5
    if not sigma_5m or sigma_5m <= 0:
        return 1.0 if spot > strike else (0.0 if spot < strike else 0.5)
    n = max(secs_left, 0.5) / 300.0
    mu = (drift_5m - 0.5 * sigma_5m ** 2) * n
    sd = sigma_5m * math.sqrt(n)
    z = (math.log(strike / spot) - mu) / sd
    return float(min(1.0, max(0.0, 0.5 * math.erfc(z / math.sqrt(2)))))


# ── Decision ──────────────────────────────────────────────────────────────────
def decide(p_up: float, books: Dict[str, Dict[str, Optional[float]]], secs_left: float,
           cfg: Dict[str, Any], sides=None) -> Dict[str, Any]:
    """Pick at most one long-shot trade candidate (side, band) based on ask range.
    sides: the sides it may take, None = both (PATCH: longshot follow)."""
    if not sniper_on(cfg):                            # PATCH: longshot strategies — the sniper is switched off
        return {"action": "NO_TRADE", "reason": "longshot_sniper_off"}
    cutoff = float(cfg.get("cutoff_s", 3.0))
    if secs_left < cutoff:
        return {"action": "NO_TRADE", "reason": "longshot_past_cutoff"}
    best, reasons, skipped = None, [], []
    for side in ("UP", "DOWN"):
        p = p_up if side == "UP" else 1.0 - p_up
        bk = books.get(side) or {}
        ask = bk.get("ask")
        if ask is None or ask <= 0:
            continue
        for name, band in (cfg.get("bands") or {}).items():
            if not band.get("enabled", True):
                continue
            look = float(band.get("look_s") or cfg.get("look_s", 60.0))
            if secs_left > look:
                continue
            ask_min = float(band.get("ask_min", 0.01))
            ask_max = float(band.get("ask_max", 0.03))
            max_price = float(band.get("max_price", ask_max))
            if not (ask_min <= ask <= ask_max):
                continue
            if ask > max_price:
                continue
            if sides is not None and side not in sides:   # PATCH: longshot follow — in a band, not a side it may take
                if side not in skipped:
                    skipped.append(side)
                continue
            mult = p / ask if ask > 0 else 0.0
            cand = {"action": "ENTER", "side": side, "band": name, "prob": p, "price": ask,
                    "multiple": mult, "max_price": max_price,
                    "phase": "LONGSHOT", "strength": "LONGSHOT", "reason": f"longshot_{name}"}
            if best is None or cand["multiple"] > best["multiple"]:
                best = cand
    if best:
        return best
    if skipped:                                                  # PATCH: longshot follow
        return {"action": "NO_TRADE", "reason": "longshot_follow_skip_" + "_".join(skipped), "skipped": skipped}
    return {"action": "NO_TRADE", "reason": "; ".join(reasons[:4]) or "longshot_no_band"}


# ── Per-window "best seen" (for the end-of-window log line) ───────────────────
_window_best: Dict[int, Dict[str, Any]] = {}


def note_candidate(start_ms: int, side: str, p: float, ask: Optional[float], secs_left: float,
                   ask_ceiling: float, fee_rate: float = 0.0) -> None:
    if not ask or ask <= 0 or ask > ask_ceiling:
        return
    m = p / ask if ask > 0 else 0.0
    cur = _window_best.get(start_ms)
    if cur is None or m > cur["multiple"]:
        _window_best[start_ms] = {"side": side, "prob": p, "ask": ask, "multiple": m,
                                  "secs_left": secs_left}


def pop_window_best(start_ms: int) -> Optional[Dict[str, Any]]:
    for k in [k for k in _window_best if k < start_ms - 7_200_000]:
        del _window_best[k]
    return _window_best.pop(start_ms, None)


# ── Pricing and sizing ────────────────────────────────────────────────────────
def tick_for(price: float) -> float:
    """Polymarket's tick is 0.1¢ below 4¢ and above 96¢, 1¢ in between."""
    return 0.001 if (price < 0.04 or price > 0.96) else 0.01


def _whole_cent(x: float) -> bool:
    return abs(x * 100.0 - round(x * 100.0)) < 1e-6


def tick_size_str(price: float) -> str:
    """Rounding used when the order is signed: "0.01" only for whole-cent prices, so signing
    never moves the price (a 4.6¢ limit signed with "0.01" would become 5¢)."""
    return "0.01" if _whole_cent(price) else "0.001"


def limit_price(ask: float, slippage: float, max_price: float) -> float:
    """ask + slippage, capped at max_price, rounded down to the book's tick, never below the
    ask. The book is on the 0.1¢ tick when the ask is under 4¢ / over 96¢ or not a whole cent."""
    ask = float(ask)
    raw = min(float(max_price), ask + max(0.0, float(slippage)))
    t = 0.001 if (tick_for(ask) < 0.01 or not _whole_cent(ask)) else 0.01
    px = math.floor(raw / t + 1e-9) * t
    if px < ask - 1e-9:
        px = ask
    return round(px, 3)


def growth_fraction(prob: Optional[float], price: Optional[float]) -> float:
    """Growth-maximizing share of the balance for a $1 payout bought at `price` with win chance
    `prob`: (prob - price) / (1 - price). 0 when there's no edge."""
    if prob is None or not price or price <= 0 or price >= 1:
        return 0.0
    return max(0.0, (float(prob) - float(price)) / (1.0 - float(price)))


def stake_pct(cfg: Optional[Dict[str, Any]] = None, key: str = "sniper_stake_pct") -> float:
    """PATCH: longshot stake — the long shots' own % of the balance: longshot.sniper_stake_pct for a
    sniper buy, longshot.maker_stake_pct for a maker bid; 0 = the bot's risk value (the GTD stake)."""
    if cfg is None:
        try:
            from bot.config import settings
            cfg = settings.LONGSHOT
        except Exception:
            cfg = {}
    try:
        return max(0.0, float((cfg or {}).get(key) or 0.0))
    except (TypeError, ValueError):
        return 0.0


def stake_text(*args, **kwargs) -> str:
    """PATCH: longshot stake — e.g. "sniper 1% balance / maker 0.5% balance"."""
    cfg = args[0] if args and isinstance(args[0], dict) else None
    parts = []
    for name, key in (("sniper", "sniper_stake_pct"), ("maker", "maker_stake_pct")):
        pct = stake_pct(cfg, key)
        parts.append(f"{name} {pct:g}% balance" if pct > 0 else f"{name} GTD stake ({gtd_stake_text()})")
    return " / ".join(parts)


def gtd_stake_text() -> str:
    """The bot's risk value as text: what the long shots used before, and use at 0."""
    try:
        from bot.config import settings
        risk_type = (getattr(settings, "RISK_TYPE", "percent") or "percent").lower()
        risk_val = float(getattr(settings, "RISK_VALUE", 10.0) or 10.0)
    except Exception:
        risk_type, risk_val = "percent", 10.0
    return f"{risk_val:g}% balance" if risk_type == "percent" else f"${risk_val:g}"


def stake_for(balance: float, *args, **kwargs) -> float:
    """Dollar stake for one sniper buy: longshot.sniper_stake_pct % of the balance, or the bot's risk
    value when that is 0 (PATCH: longshot stake). Never under $1."""
    bal = max(0.0, float(balance or 0.0))
    if bal <= 0:
        return 0.0
    pct = stake_pct(args[0] if args and isinstance(args[0], dict) else None, "sniper_stake_pct")   # PATCH: longshot stake
    if pct > 0:
        return max(1.0, min(pct / 100.0 * bal, bal)) if bal >= 1.0 else 0.0
    try:
        from bot.config import settings
        risk_type = (getattr(settings, "RISK_TYPE", "percent") or "percent").lower()
        risk_val = float(getattr(settings, "RISK_VALUE", 10.0) or 10.0)
    except Exception:
        risk_type, risk_val = "percent", 10.0
    if risk_type == "fixed":
        amt = risk_val
    else:
        amt = (risk_val / 100.0) * bal
    return max(1.0, min(amt, bal)) if bal >= 1.0 else 0.0


def maker_stake(balance: float, *args, **kwargs) -> float:
    """Maker stake: longshot.maker_stake_pct % of the balance with no $1 minimum, since a resting bid
    only needs 5 shares (maker_shares); the bot's risk value when that is 0 (PATCH: longshot stake)."""
    pct = stake_pct(args[0] if args and isinstance(args[0], dict) else None, "maker_stake_pct")
    if pct > 0:
        return max(0.0, pct / 100.0 * float(balance or 0.0))
    return stake_for(balance, {"sniper_stake_pct": 0})


def stake_usd(balance: float, *args, **kwargs) -> float:
    return stake_for(balance)


def share_step(px: float) -> int:
    """Smallest whole number of shares whose cost at `px` is a whole number of cents: 1 at a
    whole-cent price, 2, 5 or 10 at a 0.1¢ price (2.3¢ -> 10 shares = 23¢)."""
    m = int(round(float(px) * 1000))
    return 10 // math.gcd(m, 10) if m > 0 else 1


def order_shares(usd: float, px: float, max_usd: Optional[float] = None) -> float:
    """Whole shares for about `usd` dollars at the limit price `px`: at least 5 shares and $1
    (Polymarket's minimums), in steps of share_step(px), so shares x price is a whole number of
    cents. Fill-and-kill orders need that (their dollar amount may have at most 2 decimals), and
    the amounts are then exact, so signing never rounds them. With max_usd (the balance), steps
    down to stay within it while the minimums allow; the caller checks the result against it."""
    if px <= 0:
        return 0.0
    step = share_step(px)
    shares = max(5, math.ceil(max(float(usd), 1.0) / px - 1e-9))
    shares = math.ceil(shares / step) * step
    while shares * px < 1.0 - 1e-9:
        shares += step
    if max_usd is not None:
        while (shares * px > float(max_usd) + 1e-9 and shares - step >= 5
               and (shares - step) * px >= 1.0 - 1e-9):
            shares -= step
    return float(shares)


# ── Fees and maker bids (PATCH: longshot maker) ───────────────────────────────
def fee_adjusted(price: Optional[float], rate: float) -> float:
    """What a share bought by taking the ask really costs: price + Polymarket's taker fee
    (shares x rate x price x (1 - price), so per share price x (1 + rate x (1 - price)))."""
    if not price or price <= 0:
        return float(price or 0.0)
    p = float(price)
    return p * (1.0 + max(0.0, float(rate or 0.0)) * max(0.0, 1.0 - p))


def taker_price(cfg: Dict[str, Any], ask: Optional[float]) -> float:
    """fee_adjusted() with the long-shot settings' taker_fee_rate."""
    return fee_adjusted(ask, float(cfg.get("taker_fee_rate", 0.0) or 0.0))


def on_price_grid(px: float) -> bool:
    """True for prices Polymarket accepts on the finest grid: 0.1¢ steps below 4¢, whole cents from 4¢."""
    m = float(px) * 1000.0
    if abs(m - round(m)) > 1e-6 or round(m) <= 0:
        return False
    return float(px) < 0.04 or int(round(m)) % 10 == 0


def maker_bid_price(cap: float, ask: Optional[float]) -> Optional[float]:
    """Where a maker bid rests: at `cap`, or one tick under the ask when the ask is at or under
    `cap` (a post-only bid at or above the ask would be refused, it would take the ask).
    Sub-cent prices need the 0.1¢ tick, which the book only uses while prices are under 4¢:
    with the ask at 4¢ or more (or no ask) a sub-cent price is rounded down to whole cents.
    None = no bid (nothing left above zero)."""
    cap = float(cap)
    px = cap
    if ask is not None and ask > 0 and float(ask) <= cap + 1e-9:
        px = float(ask) - tick_for(float(ask))
    fine = ask is not None and 0 < float(ask) < 0.04
    if not fine:
        px = math.floor(px * 100.0 + 1e-6) / 100.0
    px = math.floor(px * 1000.0 + 1e-6) / 1000.0
    return round(px, 3) if px >= 0.001 - 1e-9 else None


def maker_shares(usd: float, px: float, max_usd: Optional[float] = None) -> float:
    """Whole shares for about `usd` dollars at `px` for a resting bid: at least 5 (Polymarket's
    minimum size), in steps of share_step(px) so the cost is whole cents. A resting bid has no
    $1 minimum. With max_usd, steps down to stay within it while 5 shares allow."""
    if px <= 0:
        return 0.0
    step = share_step(px)
    shares = max(5, math.ceil(max(0.0, float(usd)) / px - 1e-9))
    shares = math.ceil(shares / step) * step
    if max_usd is not None:
        while shares * px > float(max_usd) + 1e-9 and shares - step >= 5:
            shares -= step
    return float(shares)


def ask_size_at_or_below(asks: List[Tuple[float, float]], px: float) -> float:
    """Shares offered at or below `px` (asks as (price, size) pairs)."""
    total = 0.0
    for a_px, a_sz in asks or []:
        try:
            if float(a_px) <= float(px) + 1e-9:
                total += max(0.0, float(a_sz))
        except (TypeError, ValueError):
            continue
    return total
