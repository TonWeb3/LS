"""
Maker long shots (PATCH: longshot maker).

A resting, post-only bid at a low price (settings longshot.maker.price, 1¢ by default) on a
side the long-shot model gives at least maker.min_prob, while that side's ask is at most
maker.max_ask (the cheap side). It rests only in the last maker.look_s seconds of the window
and is cancelled when:
  - the side's chance falls under min_prob - KEEP_MARGIN, or its ask rises over
    max_ask + ASK_KEEP_MARGIN,
  - maker.cutoff_s seconds before the close, or the window changes,
  - the long-shot check stops updating (stale price, bot stopped, withdraw...): VIEW_MAX_AGE_S,
  - maker bids or long shots are switched off.
A maker pays no taker fee and earns the maker rebate. A bid fills when someone sells into it,
or buys the other side at 1 - price (Polymarket pairs the two buys by minting a share of each).

Paper (maker.live off, long shots paper-only, or no private key): no order is sent. A fill is
counted when the book shows a seller at or below the bid (size beyond what was already counted),
or a trade prints through it: this side at or below the bid, the other side at or above
1 - bid. That assumes the bid is first in line at its price (usually true: the cheap side's
bid book is mostly empty), so paper fills are an upper bound on what a real bid gets.

Live: a post-only GTC order (refused instead of taking the ask). Its fills are read from the
order's size_matched every maker.poll_s seconds and once more after it is cancelled; a maker
always fills at its own price.

main.py owns everything else through `hooks` (orders, books, trades, balances, recording a
fill as a long-shot trade, logging). Nothing here needs the network, so it tests with fakes.
"""

import asyncio
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from bot import longshot as L

KEEP_MARGIN = 0.02      # a resting bid is kept while the chance is at least min_prob - this
ASK_KEEP_MARGIN = 0.05  # ... and its side's ask is at most max_ask + this
VIEW_MAX_AGE_S = 6.0    # no fresh long-shot check for this long -> every bid is cancelled
FINAL_CHECK_S = 0.5     # live: fills are read once more this long after a cancel
LOOP_S = 0.5            # the manager also runs this often without a wake-up
PAPER_FILLS_FROM_TRADES = True   # paper: trades through the bid fill it too (False = only sellers on the book)


class Bid:
    """One resting bid (paper or live) on one side of one window."""

    def __init__(self, start_ms: int, side: str, token_id: str, other_token_id: str, price: float,
                 shares: float, live: bool, prob: float, secs_left: float, now: float, meta: Dict[str, Any]):
        self.start_ms, self.side = int(start_ms), side
        self.token_id, self.other_token_id = token_id, other_token_id
        self.price, self.shares, self.live = float(price), float(shares), bool(live)
        self.prob_at_place = self.last_prob = float(prob)
        self.secs_left_at_place = float(secs_left)
        self.placed_ts = now
        self.meta = meta                 # market, strike, strike_src, end_ts, info (from main.py)
        self.order_id: Optional[str] = None
        self.state = "placing"           # placing -> resting -> (cancelled) -> closed
        self.filled = 0.0                # shares recorded so far
        self.via: List[str] = []         # how it filled: "live", "ask", "trade"
        self.cancel_reason = ""
        self.cancelled_ts = 0.0
        self.closed_ts = 0.0
        self.last_poll = 0.0
        self.final_tries = 0             # live: reads of the fills after the cancel
        self.touch_avail = 0.0           # paper: shares offered at/below the bid already counted
        self.trades_ts = now             # paper: trades received after this count
        self.trade: Optional[Dict[str, Any]] = None   # the long-shot trade main.py made from its fills

    @property
    def key(self) -> str:
        return f"{self.start_ms}:{self.side}"

    @property
    def remaining(self) -> float:
        return max(0.0, self.shares - self.filled)

    def text(self) -> str:
        return f"{self.side} {self.price * 100:g}¢ ×{self.shares:g} ({'live' if self.live else 'paper'})"


def _err_kind(err: str) -> str:
    e = (err or "").lower()
    if "post" in e and ("cross" in e or "only" in e or "match" in e):
        return "would_cross"
    if "tick" in e:
        return "tick"
    if "balance" in e or "allowance" in e:
        return "balance"
    return "error"


class MakerManager:
    """Keeps the maker bids in line with the latest long-shot check (see the module text)."""

    def __init__(self, hooks: Any):
        self.h = hooks
        self.view: Optional[Dict[str, Any]] = None
        self.bids: Dict[str, Bid] = {}                 # key "start_ms:SIDE" -> bid not yet closed
        self.last_place: Dict[str, float] = {}         # key -> time of the last placement
        self.windows: Dict[int, Dict[str, Any]] = {}   # start_ms -> stats for the window's log line
        self.wake: Optional[asyncio.Event] = None
        self._busy = False

    # ── helpers ──────────────────────────────────────────────────────────────
    def _now(self) -> float:
        f = getattr(self.h, "now", None)
        return f() if f else time.time()

    def _cfg(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        full = self.h.get_cfg() or {}
        return full, (full.get("maker") or {})

    def _win(self, start_ms: int, end_ts: Optional[float] = None) -> Dict[str, Any]:
        w = self.windows.get(int(start_ms))
        if w is None:
            w = self.windows[int(start_ms)] = {"end_ts": end_ts, "best": {}, "bids": [], "fills": 0,
                                               "notes": set(), "seen": False, "done": False}
        if end_ts and not w.get("end_ts"):
            w["end_ts"] = end_ts
        return w

    def _note_once(self, start_ms: int, kind: str, msg: str) -> None:
        w = self._win(start_ms)
        if kind not in w["notes"]:
            w["notes"].add(kind)
            self.h.log(msg)

    def _persist(self) -> None:
        ids = [b.order_id for b in self.bids.values() if b.live and b.order_id and b.state != "closed"]
        try:
            self.h.persist(ids)
        except Exception:
            pass

    def _poke(self) -> None:
        if self.wake is not None:
            self.wake.set()

    def active(self) -> List[Dict[str, Any]]:
        """Bids on the book now (for the dashboard / status line)."""
        now = self._now()
        return [{"side": b.side, "price": b.price, "shares": b.shares, "filled": round(b.filled, 4),
                 "live": b.live, "state": b.state, "age_s": round(now - b.placed_ts, 1), "order_id": b.order_id}
                for b in self.bids.values() if b.state in ("placing", "resting", "cancelling", "cancelled")]

    def has_active_bid(self, start_ms: int) -> bool:
        """True if any maker bid for start_ms is placing or resting on the book."""
        return any(b.start_ms == int(start_ms) and b.state in ("placing", "resting")
                   for b in self.bids.values())

    async def cancel_window(self, start_ms: int, reason: str = "cancelled") -> None:
        """Cancel all resting bids for a specific window immediately."""
        now = self._now()
        for b in list(self.bids.values()):
            if b.start_ms == int(start_ms) and b.state == "resting":
                await self._cancel(b, reason, now)

    # ── input from the long-shot check ───────────────────────────────────────
    async def on_view(self, view: Dict[str, Any]) -> None:
        """The latest chance and window, from evaluate_longshot (no network): stores it, counts
        paper fills against the current book and trades, and wakes the manager."""
        self.view = view
        full, mk = self._cfg()
        now = view.get("ts") or self._now()
        secs_left = float(view["end_ts"]) - now
        w = self._win(view["start_ms"], view["end_ts"])
        maker_look = float(mk.get("look_s") or full.get("look_s") or 60)
        if L.maker_on(full) and secs_left <= maker_look:
            w["seen"] = True
            for side in ("UP", "DOWN"):
                if view.get("sides") is not None and side not in view["sides"]:   # PATCH: longshot follow
                    w["follow"] = view.get("follow_why")
                    continue
                p = view["p_up"] if side == "UP" else 1.0 - view["p_up"]
                if p > w["best"].get(side, (-1.0, 0.0))[0]:
                    w["best"][side] = (p, secs_left)
        for b in list(self.bids.values()):
            if b.start_ms == int(view["start_ms"]):
                b.last_prob = view["p_up"] if b.side == "UP" else 1.0 - view["p_up"]
            if not b.live and b.state == "resting":
                await self._paper_check(b, now)
        self._poke()

    # ── paper fills ──────────────────────────────────────────────────────────
    async def _paper_check(self, b: Bid, now: float) -> None:
        if b.state != "resting" or b.remaining <= 1e-9:
            return
        got, via = 0.0, ""
        bk = self.h.book(b.token_id)
        if bk is not None:
            avail = L.ask_size_at_or_below(bk.get("asks") or [], b.price)
            if avail <= 1e-9:
                b.touch_avail = 0.0
            elif avail > b.touch_avail + 1e-9:          # a seller at or below the bid would hit it
                got, via = min(b.remaining, avail - b.touch_avail), "ask"
                b.touch_avail = avail
        since = b.trades_ts
        cut = self._now()                                # trades received up to now are handled here
        b.trades_ts = max(since, cut)
        same = [t for t in (self.h.trades_since(b.token_id, since) or []) if float(t[0]) <= cut] \
            if PAPER_FILLS_FROM_TRADES else []
        seen = set()
        for t in same:                                   # this side traded at or below the bid
            if b.remaining - got <= 1e-9:
                break
            if float(t[1]) <= b.price + 1e-9:
                take = min(b.remaining - got, float(t[2]))
                if take > 0:
                    got += take
                    via = via or "trade"
            seen.add((t[3], round(float(t[2]), 6)))
        if b.other_token_id and PAPER_FILLS_FROM_TRADES:
            for t in self.h.trades_since(b.other_token_id, since) or []:   # the other side bought at >= 1 - bid
                if float(t[0]) > cut:
                    continue
                if b.remaining - got <= 1e-9:
                    break
                if (t[3], round(float(t[2]), 6)) in seen:          # the same match reported on both sides
                    continue
                if float(t[1]) >= 1.0 - b.price - 1e-9:
                    take = min(b.remaining - got, float(t[2]))
                    if take > 0:
                        got += take
                        via = via or "trade"
        if got > 1e-9:
            await self._record(b, got, via)
            if b.remaining <= 1e-9:
                self._close(b, now, "filled")

    # ── recording a fill ─────────────────────────────────────────────────────
    async def _record(self, b: Bid, shares: float, via: str) -> None:
        shares = round(float(shares), 6)
        if shares <= 1e-9:
            return
        if b.filled <= 1e-9:
            self._win(b.start_ms)["fills"] += 1
        b.filled += shares
        if via not in b.via:
            b.via.append(via)
        try:
            await self.h.on_fill(b, shares, round(shares * b.price, 6), via)
        except Exception as e:
            self.h.log(f"LONGSHOT MAKER fill bookkeeping error: {type(e).__name__}: {e}")

    def _close(self, b: Bid, now: float, reason: str = "") -> None:
        if b.state == "closed":
            return
        b.state = "closed"
        b.closed_ts = now
        if reason and not b.cancel_reason:
            b.cancel_reason = reason
        if self.bids.get(b.key) is b:
            del self.bids[b.key]
        self._win(b.start_ms)["bids"].append({
            "side": b.side, "price": b.price, "shares": b.shares, "filled": b.filled, "live": b.live,
            "rested_s": max(0.0, now - b.placed_ts), "why": b.cancel_reason, "via": "+".join(b.via)})
        self._persist()

    # ── live orders ──────────────────────────────────────────────────────────
    async def _poll(self, b: Bid, now: float) -> bool:
        """Reads the order's fills; False when Polymarket didn't answer."""
        b.last_poll = now
        try:
            o = await self.h.get_order(b.order_id)
        except Exception:
            o = None
        if not o:
            return False
        try:
            matched = float(o.get("size_matched") or 0.0)
        except (TypeError, ValueError):
            matched = 0.0
        if matched > b.filled + 1e-9:
            await self._record(b, matched - b.filled, "live")
        st = str(o.get("status") or "").upper()
        if b.remaining <= 1e-9 or st in ("MATCHED", "FILLED"):
            self._close(b, self._now(), "filled")
        elif st in ("CANCELED", "CANCELLED", "EXPIRED") and b.state == "resting":
            self._close(b, self._now(), "cancelled on Polymarket")
        return True

    async def _cancel(self, b: Bid, reason: str, now: float) -> None:
        if b.state != "resting":
            return
        b.cancel_reason = reason
        if not b.live:
            await self._paper_check(b, now)               # fills up to this moment still count
            if b.state == "resting":
                self.h.log(f"LONGSHOT MAKER bid {b.text()} cancelled ({reason}) after "
                           f"{now - b.placed_ts:.1f}s, filled {b.filled:g}/{b.shares:g}")
                self._close(b, now)
            return
        b.state = "cancelling"                           # nothing else cancels it meanwhile
        try:
            r = await self.h.cancel(b.order_id)
            ok = bool(r and r.get("ok"))
        except Exception as e:
            r, ok = {"error": f"{type(e).__name__}: {e}"}, False
        if not ok:
            b.state = "resting"
            self._note_once(b.start_ms, f"cancel_fail:{b.side}",
                            f"LONGSHOT MAKER cancel of {b.order_id} failed ({(r or {}).get('error')}); trying again")
            return
        b.state, b.cancelled_ts = "cancelled", self._now()
        self.h.log(f"LONGSHOT MAKER bid {b.text()} cancelled ({reason}) after {b.cancelled_ts - b.placed_ts:.1f}s, "
                   f"order {b.order_id}")

    async def _place(self, view: Dict[str, Any], side: str, px: float, prob: float, now: float) -> None:
        full, mk = self._cfg()
        start_ms, end_ts = int(view["start_ms"]), float(view["end_ts"])
        toks = view.get("token_ids") or {}
        tok, other = toks.get(side), toks.get("DOWN" if side == "UP" else "UP")
        if not tok:
            return
        live = bool(self.h.is_live())
        bal = float(self.h.balance(live) or 0.0)
        usd = L.maker_stake(bal, full, prob, px)
        if usd <= 0:
            self._note_once(start_ms, f"stake0:{side}", f"LONGSHOT MAKER no {side} bid: the stake is $0 "
                                                        f"(balance ${bal:.2f})")
            return
        shares = L.maker_shares(usd, px, max_usd=bal)
        if shares * px > bal + 1e-9:
            self._note_once(start_ms, f"bal:{side}", f"LONGSHOT MAKER no {side} bid: balance ${bal:.2f} is under "
                                                     f"${shares * px:.2f} (5 shares minimum at {px * 100:g}¢)")
            return
        secs_left = end_ts - now
        b = Bid(start_ms, side, tok, other, px, shares, live, prob, secs_left, now, dict(view.get("meta") or {}, end_ts=end_ts))
        self.bids[b.key] = b
        self.last_place[b.key] = now
        if not live:
            b.state = "resting"
            self.h.log(f"LONGSHOT MAKER bid {b.text()} = ${shares * px:.2f}, chance {prob:.3f}, {secs_left:.0f}s left")
            await self._paper_check(b, now)               # a seller already at the price fills it now
            return
        try:
            res = await self.h.place(tok, shares, px)
        except Exception as e:
            res = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        if res and res.get("ok") and res.get("order_id"):
            b.order_id = str(res["order_id"])
            b.state = "resting"
            b.last_poll = 0.0 if str(res.get("status") or "").lower() == "matched" else self._now()
            self.h.log(f"LONGSHOT MAKER bid {b.text()} = ${shares * px:.2f}, chance {prob:.3f}, "
                       f"{secs_left:.0f}s left, order {b.order_id}")
            self._persist()
        else:
            err = str((res or {}).get("error") or "no order id")
            kind = _err_kind(err)
            msg = {"would_cross": f"LONGSHOT MAKER {side} bid at {px * 100:g}¢ refused: it would take the ask "
                                  f"(the ask moved); trying again",
                   "tick": f"LONGSHOT MAKER {side} bid at {px * 100:g}¢ refused by the tick size: {err}",
                   "balance": f"LONGSHOT MAKER {side} bid refused, balance: {err}"}.get(
                kind, f"LONGSHOT MAKER {side} bid failed: {err}")
            self._note_once(start_ms, f"{kind}:{side}", msg)
            b.state = "closed"
            if self.bids.get(b.key) is b:
                del self.bids[b.key]

    # ── the manager ──────────────────────────────────────────────────────────
    def wanted(self, now: float) -> Dict[str, Tuple[float, float]]:
        """{side: (price, chance)} for the bids that should rest now."""
        full, mk = self._cfg()
        v = self.view
        if not (L.maker_on(full) and v and self.h.running() and now - float(v.get("ts") or 0) <= VIEW_MAX_AGE_S):
            return {}
        secs_left = float(v["end_ts"]) - now
        c_val = mk.get("cutoff_s") if mk.get("cutoff_s") is not None else full.get("cutoff_s")
        maker_cutoff = float(c_val) if c_val is not None else 0.0
        l_val = mk.get("look_s") if mk.get("look_s") is not None else full.get("look_s")
        maker_look = float(l_val) if l_val is not None else 60.0
        if not (maker_cutoff <= secs_left <= maker_look):
            return {}
        start_ms = int(v["start_ms"])
        if full.get("one_at_a_time", True):
            has_other = getattr(self.h, "has_other_longshot", None)
            if has_other and has_other(start_ms):
                return {}
        filled_sides = {b.side for b in self.bids.values() if b.start_ms == start_ms and b.filled > 1e-9}
        cap_hit = self._win(start_ms)["fills"] >= int(mk.get("max_fills_per_window", 1))
        min_p = float(mk.get("min_prob", 0.2))
        max_ask = float(mk.get("max_ask", 1.0))
        out = {}
        allowed = v.get("sides")                  # PATCH: longshot follow — None = both sides
        for side in ("UP", "DOWN"):
            if allowed is not None and side not in allowed:
                continue
            if cap_hit and side not in filled_sides:
                continue
            p = float(v["p_up"]) if side == "UP" else 1.0 - float(v["p_up"])
            cur = self.bids.get(f"{start_ms}:{side}")
            holding = bool(cur and cur.state == "resting")
            if p < (min_p - KEEP_MARGIN if holding else min_p):
                continue
            tok = (v.get("token_ids") or {}).get(side)
            bk = self.h.book(tok) if tok else None
            ask = (bk or {}).get("best_ask")
            if ask is not None and float(ask) > (max_ask + ASK_KEEP_MARGIN if holding else max_ask) + 1e-9:
                continue
            cap = float(mk.get("price", 0.01))
            px = L.maker_bid_price(cap, ask)
            if holding and cur.price <= cap + 1e-9 and (px is None or px < cur.price):
                px = cur.price            # a seller at or under a resting bid fills it; it doesn't move down
            if px is not None:
                out[side] = (px, p)
        return out

    async def reconcile(self) -> None:
        if self._busy:
            return
        self._busy = True
        try:
            await self._reconcile()
        finally:
            self._busy = False

    async def _reconcile(self) -> None:
        now = self._now()
        full, mk = self._cfg()
        want = self.wanted(now)
        v = self.view
        cur_start = int(v["start_ms"]) if v else None
        requote_s = float(mk.get("requote_s", 2.0))
        poll_s = float(mk.get("poll_s", 1.0))
        cap = float(mk.get("price", 0.01))
        for b in list(self.bids.values()):
            if b.state == "resting" and not b.live:
                await self._paper_check(b, now)
            if b.state == "resting":
                w = want.get(b.side) if b.start_ms == cur_start else None
                tok_now = ((v or {}).get("token_ids") or {}).get(b.side)
                if w is None:
                    await self._cancel(b, self._why_not(b, now), now)
                elif tok_now and tok_now != b.token_id:
                    await self._cancel(b, "market changed", now)
                elif b.price > cap + 1e-9:
                    await self._cancel(b, "price setting lowered", now)
                elif w[0] > b.price + 1e-9 and now - self.last_place.get(b.key, 0.0) >= requote_s:
                    await self._cancel(b, f"moving up to {w[0] * 100:g}¢", now)
            if b.live and b.state == "resting" and now - b.last_poll >= poll_s:
                await self._poll(b, now)
            if b.live and b.state == "cancelled" and now - b.cancelled_ts >= FINAL_CHECK_S * (1 + b.final_tries):
                b.final_tries += 1
                read = await self._poll(b, now)
                if b.state != "closed" and (read or b.final_tries >= 3):
                    if not read:
                        self.h.log(f"LONGSHOT MAKER couldn't read the fills of {b.order_id} after cancelling it; "
                                   f"if it bought anything, the redeem sweep still claims those shares when they win")
                    self._close(b, self._now())
        if v is not None:
            want = self.wanted(now)                       # fills, closes and cancels above can change it
            for side, (px, p) in want.items():
                key = f"{cur_start}:{side}"
                if key in self.bids or now - self.last_place.get(key, -1e9) < requote_s:
                    continue
                await self._place(v, side, px, p, now)
        self._summaries(now)

    def _why_not(self, b: Bid, now: float) -> str:
        full, mk = self._cfg()
        if full.get("one_at_a_time", True):
            has_other = getattr(self.h, "has_other_longshot", None)
            if has_other and has_other(b.start_ms):
                return "one at a time (another longshot active)"
        v = self.view
        if not L.maker_on(full):
            return "maker bids off"
        if not self.h.running():
            return "bot stopped"
        if not v or now - float(v.get("ts") or 0) > VIEW_MAX_AGE_S:
            return "no fresh long-shot check"
        if b.start_ms != int(v["start_ms"]):
            return "window over"
        if v.get("sides") is not None and b.side not in v["sides"]:   # PATCH: longshot follow
            return f"not the side it follows ({v.get('follow_why') or 'follow'})"
        secs_left = float(v["end_ts"]) - now
        c_val = mk.get("cutoff_s") if mk.get("cutoff_s") is not None else full.get("cutoff_s")
        maker_cutoff = float(c_val) if c_val is not None else 0.0
        l_val = mk.get("look_s") if mk.get("look_s") is not None else full.get("look_s")
        maker_look = float(l_val) if l_val is not None else 60.0
        if secs_left < maker_cutoff:
            return f"cutoff, {secs_left:.1f}s left"
        if secs_left > maker_look:
            return "outside the look window"
        if self._win(b.start_ms)["fills"] >= int(mk.get("max_fills_per_window", 1)) and b.filled <= 1e-9:
            return "fill cap reached this window"
        if b.last_prob < float(mk.get("min_prob", 0.2)) - KEEP_MARGIN:
            return f"chance {b.last_prob:.3f} under {float(mk.get('min_prob', 0.2)) - KEEP_MARGIN:.2f}"
        ask = (self.h.book(b.token_id) or {}).get("best_ask")
        if ask is not None and float(ask) > float(mk.get("max_ask", 1.0)) + ASK_KEEP_MARGIN + 1e-9:
            return f"ask {float(ask) * 100:g}¢ over {(float(mk.get('max_ask', 1.0)) + ASK_KEEP_MARGIN) * 100:g}¢"
        return "no bid price left (ask at the lowest tick)"

    def _summaries(self, now: float) -> None:
        """One log line per finished window the maker watched."""
        full, mk = self._cfg()
        for start_ms in sorted(self.windows):
            w = self.windows[start_ms]
            if w["done"] or not w.get("end_ts") or now < float(w["end_ts"]) + 1.0:
                continue
            if any(b.start_ms == start_ms for b in self.bids.values()):
                continue                                   # a bid of that window is still being closed
            w["done"] = True
            if not w["seen"] and not w["bids"]:
                continue
            label = datetime.fromtimestamp(start_ms / 1000).strftime("%H:%M")
            if not w["bids"]:
                best = "; ".join(f"{s} {w['best'][s][0]:.3f}" for s in ("UP", "DOWN") if s in w["best"])
                if not best and w.get("follow"):                       # PATCH: longshot follow — no side it may take
                    self.h.log(f"LONGSHOT MAKER {label} window: no bid — follow: {w['follow']}")
                    continue
                self.h.log(f"LONGSHOT MAKER {label} window: no bid — highest chance {best or '?'} "
                           f"(needs {float(mk.get('min_prob', 0.2)):.2f})"
                           + (f" — follow: {w['follow']}" if w.get("follow") else ""))   # PATCH: longshot follow
                continue
            parts = []
            for r in w["bids"]:
                fill = (f"filled {r['filled']:g}/{r['shares']:g} ({r['via']})" if r["filled"] > 1e-9 else "no fill")
                parts.append(f"{r['side']} {r['price'] * 100:g}¢ {'live' if r['live'] else 'paper'} "
                             f"rested {r['rested_s']:.0f}s, {fill}" + (f" — {r['why']}" if r["why"] else ""))
            self.h.log(f"LONGSHOT MAKER {label} window: {len(w['bids'])} bid(s), {w['fills']} filled — " + "; ".join(parts))
        for k in [k for k in self.windows if self.windows[k]["done"] and k < (now - 7200) * 1000]:
            del self.windows[k]

    async def cancel_all(self, reason: str) -> None:
        """Cancels every resting bid now (trading stopped, shutdown)."""
        now = self._now()
        for b in list(self.bids.values()):
            if b.state == "resting":
                await self._cancel(b, reason, now)
        for b in list(self.bids.values()):
            if b.live and b.state == "cancelled":
                await asyncio.sleep(FINAL_CHECK_S)
                for _ in range(3):
                    if await self._poll(b, self._now()):
                        break
                    await asyncio.sleep(FINAL_CHECK_S)
                if b.state != "closed":
                    self._close(b, self._now())

    async def cancel_orphans(self) -> None:
        """Bids the last run left on the book (it stopped while they rested) are cancelled."""
        try:
            ids = [i for i in (self.h.orphans() or []) if i]
        except Exception:
            ids = []
        if not ids:
            return
        done = 0
        for oid in ids:
            try:
                r = await self.h.cancel(oid)
                done += 1 if (r and r.get("ok")) else 0
            except Exception:
                pass
        self.h.log(f"LONGSHOT MAKER: cancelled {done}/{len(ids)} bid(s) the last run left on the book")
        self._persist()

    async def run_forever(self) -> None:
        self.wake = asyncio.Event()
        await self.cancel_orphans()
        last_err = 0.0
        while True:
            try:
                try:
                    await asyncio.wait_for(self.wake.wait(), timeout=LOOP_S)
                except asyncio.TimeoutError:
                    pass
                self.wake.clear()
                await self.reconcile()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if time.time() - last_err > 60:
                    last_err = time.time()
                    self.h.log(f"LONGSHOT MAKER error: {type(e).__name__}: {e}")
                await asyncio.sleep(LOOP_S)
