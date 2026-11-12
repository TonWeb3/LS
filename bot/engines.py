from typing import Dict, Any

# ─────────────────────────────────────────────────────────────────────────────
#  Latency-arb entry engine.
#
#  Backtest verdict: the model has NO predictive edge over the trivial "is spot
#  already above the 15m open?" baseline — that signal is fully priced by the
#  market. The only edge left is LATENCY: act on a Binance spot move before
#  Polymarket's thin book reprices.
#
#  The decision is purely a fast fair probability (from Binance spot) vs the
#  market's implied price. Enter when the gap (expected value) is large enough
#  that the book looks stale. RSI and Heiken-Ashi survive only as veto filters.
# ─────────────────────────────────────────────────────────────────────────────

EXHAUSTION_BARS = 6  # a Heiken-Ashi streak this long is over-extended → veto chasing it


def _no_ev(reason: str) -> Dict[str, Any]:
    return {"action": "NO_TRADE", "side": None, "phase": "EV", "strength": "EV", "reason": reason}


def decide_ev(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """EV gate: fair probability (Binance) vs market ask price (Polymarket).

    EV_side = p_side - ask_price_side. A positive EV beyond `evThreshold` means the
    book is underpricing the side our fast feed already favours — the latency edge.
    RSI / Heiken-Ashi are veto filters only; they do NOT distort the probability.
    Position sizing (percent/fixed of balance) is handled by the caller.
    """
    p_up = inputs.get("mcProbUp")
    price_up = inputs.get("priceUp")     # ask (buy) price for the UP share, 0..1
    price_down = inputs.get("priceDown") # ask (buy) price for the DOWN share, 0..1

    if p_up is None:
        return _no_ev("missing_model_data")
    if price_up is None or price_down is None:
        return _no_ev("missing_prices")

    p_down = 1.0 - p_up
    ev_up = p_up - price_up
    ev_down = p_down - price_down

    side = "UP" if ev_up >= ev_down else "DOWN"
    p = p_up if side == "UP" else p_down
    price = price_up if side == "UP" else price_down
    ev = ev_up if side == "UP" else ev_down

    min_prob = inputs.get("minProb", 0.55)
    ev_threshold = inputs.get("evThreshold", 0.04)

    # ── VETO filters — never chase a stretched move or trade into RSI extremes ──
    if side == "UP" and inputs.get("haExhaustedGreen"):
        return _no_ev("ha_exhausted_up")
    if side == "DOWN" and inputs.get("haExhaustedRed"):
        return _no_ev("ha_exhausted_down")

    rsi = inputs.get("rsi")
    if rsi is not None:
        if side == "UP" and rsi > 70:
            return _no_ev("rsi_overbought")
        if side == "DOWN" and rsi < 30:
            return _no_ev("rsi_oversold")

    # ── GATES ──
    if p < min_prob:
        return _no_ev(f"prob_{p:.2f}_below_{min_prob:.2f}")
    if ev < ev_threshold:
        return _no_ev(f"ev_{ev:.3f}_below_{ev_threshold:.3f}")

    strength = "HIGH_CONVICTION" if p >= 0.70 else "STRONG"
    return {
        "action": "ENTER", "side": side, "phase": "EV", "strength": strength,
        "prob": p, "price": price, "ev": ev, "reason": "ev_enter"
    }
