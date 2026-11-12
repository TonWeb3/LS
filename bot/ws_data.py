import asyncio
import collections  # PATCH: longshot maker
import time
from typing import Optional, Callable, Dict, List
import aiohttp
from .config import settings
from .net_utils import get_proxy_url_for

# ── Low-Level Performance: Fast JSON Acceleration (orjson / std json fallback) ──
try:
    import orjson
    def fast_loads(s):
        if isinstance(s, (bytes, bytearray)):
            return orjson.loads(s)
        return orjson.loads(s.encode("utf-8"))
    def fast_dumps(obj):
        return orjson.dumps(obj).decode("utf-8")
except ImportError:
    import json
    def fast_loads(s):
        return json.loads(s)
    def fast_dumps(obj):
        return json.dumps(obj)


class BinanceTradeStream:
    """Ultra-fast spot price feed from Binance Spot index.
    Listens to Binance Spot (bookTicker and trade). Computes real-time mid-price
    from best bid and ask on every order book change with zero basis skew."""
    def __init__(self, symbol: str, on_update: Optional[Callable] = None):
        self.symbol = symbol.lower()
        self.on_update = on_update
        self.last_price = None
        self.last_spot_price = None
        self.last_ts = None
        self.last_source = None
        self.closed = False

    async def _spot_book_worker(self):
        """Binance Spot feed (bookTicker) - fast mid-price on Spot index.
        The mid-price of best bid and ask updates on every book change with zero basis skew."""
        endpoints = [
            f"wss://stream.binance.com:9443/ws/{self.symbol}@bookTicker",
            f"wss://data-stream.binance.vision/ws/{self.symbol}@bookTicker"
        ]
        endpoint_idx = 0
        while not self.closed:
            url = endpoints[endpoint_idx % len(endpoints)]
            endpoint_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=8.0, sock_read=20.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=15.0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance Spot Book WS ({url}): {self.symbol}@bookTicker")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = fast_loads(msg.data)
                                if "b" in data_msg and "a" in data_msg:
                                    try:
                                        bid = float(data_msg["b"])
                                        ask = float(data_msg["a"])
                                        if bid > 0 and ask > 0:
                                            mid = (bid + ask) / 2.0
                                            self._process_trade(mid, source="binance_spot_book")
                                    except (TypeError, ValueError):
                                        pass
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                break
            except Exception as e:
                if not self.closed:
                    await asyncio.sleep(2)

    async def _spot_worker(self):
        """Binance Spot feed (trade) - robust fallback and spot anchor."""
        endpoints = [
            f"wss://data-stream.binance.vision/ws/{self.symbol}@trade",
            f"wss://stream.binance.com:9443/ws/{self.symbol}@trade"
        ]
        endpoint_idx = 0
        while not self.closed:
            url = endpoints[endpoint_idx % len(endpoints)]
            endpoint_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=8.0, sock_read=20.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=15.0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance Spot WS ({url}): {self.symbol}")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = fast_loads(msg.data)
                                if "p" in data_msg:
                                    try:
                                        p = float(data_msg["p"])
                                        if p > 0:
                                            self._process_trade(p, source="binance_spot_trade")
                                    except (TypeError, ValueError):
                                        pass
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                break
            except Exception as e:
                if not self.closed:
                    await asyncio.sleep(2)

    async def start(self):
        await asyncio.gather(
            self._spot_book_worker(),
            self._spot_worker(),
            return_exceptions=True
        )

    def _process_trade(self, p: float, source: str = "binance_spot"):
        self.last_price = p
        self.last_spot_price = p
        self.last_ts = time.time()
        self.last_source = source

        if self.on_update:
            try:
                res = self.on_update({"price": p, "spot_price": p, "ts": self.last_ts, "source": source, "is_spot": True})
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception:
                pass

    def get_last(self):
        return {"price": self.last_spot_price or self.last_price, "ts": self.last_ts, "source": self.last_source}

    def close(self):
        self.closed = True


class BinanceKlineStream:
    """Binance Kline stream for 1m and 5m candles with sliding window buffer.
    Guarantees strict chronological ordering, gap detection and auto-backfill,
    and automatic reconnection with heartbeat to avoid hanging dead sockets."""
    def __init__(self, symbol: str, interval: str, limit: int = 240):
        self.symbol = symbol.lower()
        self.interval = interval
        self.limit = limit
        self.candles = []
        self.closed = False
        self._lock = asyncio.Lock()
        self._step_ms = 60_000 if interval == "1m" else 300_000
        self._backfilling = False

    async def start(self):
        endpoints = [
            f"wss://data-stream.binance.vision/ws/{self.symbol}@kline_{self.interval}",
            f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_{self.interval}"
        ]
        endpoint_idx = 0

        # Proactively backfill if candles are missing or insufficient
        if len(self.candles) < 20:
            asyncio.create_task(self._backfill_gap())

        while not self.closed:
            url = endpoints[endpoint_idx % len(endpoints)]
            endpoint_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                timeout = aiohttp.ClientTimeout(total=None, sock_read=30.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=20.0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance Kline WS ({url}): {self.symbol} {self.interval}")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = fast_loads(msg.data)
                                k = data_msg.get("k", {})
                                if k.get("t") is not None:
                                    candle = {
                                        "openTime": int(k.get("t")),
                                        "open": float(k.get("o")),
                                        "high": float(k.get("h")),
                                        "low": float(k.get("l")),
                                        "close": float(k.get("c")),
                                        "volume": float(k.get("v")),
                                        "closeTime": int(k.get("T")),
                                        "isClosed": bool(k.get("x"))
                                    }
                                    async with self._lock:
                                        self._update_candle(candle)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                break
            except Exception as e:
                if not self.closed:
                    print(f"Binance Kline WS reconnecting ({url}): {e}")
                    await asyncio.sleep(2)

    def _update_candle(self, candle: Dict):
        if not self.candles:
            self.candles.append(candle)
            asyncio.create_task(self._backfill_gap())
            return

        last = self.candles[-1]
        c_open = candle["openTime"]
        l_open = last["openTime"]

        if c_open == l_open:
            self.candles[-1] = candle
        elif c_open > l_open:
            if (c_open - l_open) > (self._step_ms * 1.5) or len(self.candles) < 15:
                asyncio.create_task(self._backfill_gap())
            self.candles.append(candle)
            if len(self.candles) > self.limit:
                self.candles.pop(0)
        else:
            for i, c in enumerate(self.candles):
                if c["openTime"] == c_open:
                    self.candles[i] = candle
                    return
            self.candles.append(candle)
            self.candles.sort(key=lambda x: x["openTime"])
            if len(self.candles) > self.limit:
                self.candles = self.candles[-self.limit:]

    async def _backfill_gap(self):
        if getattr(self, "_backfilling", False):
            return
        self._backfilling = True
        try:
            from . import data
            klines = await data.fetch_klines(self.symbol.upper(), self.interval, self.limit)
            if klines and len(klines) >= 15:
                async with self._lock:
                    self.set_candles(klines)
        except Exception:
            pass
        finally:
            self._backfilling = False

    def set_candles(self, candles: List[Dict]):
        if not candles:
            return
        seen = set()
        deduped = []
        for c in sorted(candles, key=lambda x: x["openTime"]):
            if c["openTime"] not in seen:
                seen.add(c["openTime"])
                deduped.append(c)
        self.candles = deduped[-self.limit:]

    def get_candles(self) -> List[Dict]:
        return list(self.candles)

    def close(self):
        self.closed = True


class PolymarketChainlinkStream:
    def __init__(self, ws_url: str, symbol_includes: str = "btc", on_update: Optional[Callable] = None):
        self.ws_url = ws_url
        self.symbol_includes = symbol_includes.lower()
        self.on_update = on_update
        self.last_price = None
        self.last_updated_at = None
        self.closed = False

    async def start(self):
        if not self.ws_url:
            return

        async def ping_loop(ws):
            while not self.closed:
                try:
                    await ws.send_str("PING")
                    await asyncio.sleep(5)
                except:
                    break

        while not self.closed:
            try:
                proxy = get_proxy_url_for(self.ws_url)
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                }
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=8.0, sock_read=25.0)
                async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                    print(f"Connecting to Polymarket WS: {self.ws_url}")
                    async with session.ws_connect(self.ws_url, heartbeat=20.0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Polymarket WS. Subscribing to topics...")

                        topics = ["crypto_prices_chainlink", "price_chainlink", "settlement_prices"]
                        for t in topics:
                            await ws.send_json({"action": "subscribe", "topic": t})

                        asyncio.create_task(ping_loop(ws))

                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_text = msg.data
                                if data_text in ("PONG", "OK", "PONG\n"):
                                    continue

                                try:
                                    data_msg = fast_loads(data_text)
                                except Exception:
                                    continue

                                topic = data_msg.get("topic")
                                if topic not in ("crypto_prices_chainlink", "price_chainlink", "settlement_prices"):
                                    continue

                                payload = data_msg.get("payload", {})
                                if isinstance(payload, str):
                                    try:
                                        payload = fast_loads(payload)
                                    except:
                                        continue

                                updates = payload if isinstance(payload, list) else [payload]

                                for update in updates:
                                    if not isinstance(update, dict): continue

                                    sym = str(update.get("symbol") or update.get("pair") or update.get("ticker") or update.get("asset") or "").lower()
                                    if self.symbol_includes:
                                        target = self.symbol_includes.lower()
                                        if target not in sym and not (target == "btc" and "bitcoin" in sym):
                                            continue

                                    try:
                                        price_val = update.get("price") or update.get("value") or update.get("current")
                                        if price_val is not None:
                                            self.last_price = float(price_val)
                                            ts_val = update.get("timestamp") or update.get("updated_at") or time.time()
                                            updated_at = float(ts_val)
                                            if updated_at < 10000000000: updated_at *= 1000
                                            self.last_updated_at = updated_at

                                            if self.on_update:
                                                res = self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "source": "polymarket_ws"})
                                                if asyncio.iscoroutine(res):
                                                    asyncio.create_task(res)
                                    except:
                                        continue

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                break
            except Exception as e:
                print(f"WS Error (Polymarket): {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def get_last(self):
        return {"price": self.last_price, "updatedAt": self.last_updated_at, "source": "polymarket_ws"}

    def close(self):
        self.closed = True


class PolymarketClobMarketStream:
    """Real-time orderbook and price feed for active Polymarket tokens over CLOB Market WS."""
    def __init__(self, ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market", on_update=None):
        self.ws_url = ws_url
        self.on_update = on_update
        self.asset_ids: List[str] = []
        self.books: Dict[str, Dict] = {} # asset_id -> {"bids": [...], "asks": [...], "best_bid": float, "best_ask": float}
        self.book_levels: Dict[str, Dict[str, Dict[float, float]]] = {} # asset_id -> {"bids": {px: sz}, "asks": {px: sz}}
        # PATCH: longshot maker — recent trades per asset_id: (received, price, size, Polymarket's timestamp)
        self.trades: Dict[str, "collections.deque"] = {}
        self.last_ts: float = 0
        self.closed = False
        self._ws = None

    def reset_books(self):
        self.books.clear()
        self.book_levels.clear()

    def update_assets(self, asset_ids: List[str]):
        new_ids = sorted(list(set(asset_ids)))
        if new_ids != self.asset_ids:
            self.asset_ids = new_ids
            if self._ws and not self._ws.closed and self.asset_ids:
                asyncio.create_task(self._subscribe(self._ws))

    async def _subscribe(self, ws):
        if not self.asset_ids:
            return
        sub_msg = {
            "assets_ids": self.asset_ids,
            "type": "market"
        }
        try:
            await ws.send_json(sub_msg)
            print(f"Subscribed to Polymarket CLOB Market WS for tokens: {self.asset_ids}")
        except Exception as e:
            print(f"Failed to subscribe to CLOB market WS: {e}")

    async def start(self):
        async def ping_loop(ws):
            while not self.closed:
                try:
                    await asyncio.sleep(10)
                    if not ws.closed:
                        await ws.send_str("PING")
                except:
                    break

        while not self.closed:
            try:
                proxy = get_proxy_url_for(self.ws_url)
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=8.0, sock_read=25.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(self.ws_url, heartbeat=20.0, proxy=proxy if proxy else None) as ws:
                        self._ws = ws
                        print(f"Connected to Polymarket CLOB Market WS: {self.ws_url}")
                        await self._subscribe(ws)
                        ping_task = asyncio.create_task(ping_loop(ws))

                        try:
                            while not self.closed:
                                msg = await ws.receive()
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data_text = msg.data.strip() if msg.data else ""
                                    if not data_text or data_text in ("PONG", "PING", "OK", "PONG\n"):
                                        continue
                                    try:
                                        data = fast_loads(data_text)
                                        self._process_msg(data)
                                    except Exception:
                                        continue
                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                    break
                        finally:
                            ping_task.cancel()
            except Exception as e:
                print(f"Polymarket CLOB Market WS error: {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def _sync_book_entry(self, aid: str):
        levels = self.book_levels.get(aid, {"bids": {}, "asks": {}})
        bids_map = levels.get("bids", {})
        asks_map = levels.get("asks", {})

        # Bids: descending by price (highest bid first)
        sorted_bids = sorted(
            [{"price": f"{p:.4f}", "size": s, "px": p} for p, s in bids_map.items() if s > 0],
            key=lambda x: x["px"],
            reverse=True
        )
        # Asks: ascending by price (lowest ask first)
        sorted_asks = sorted(
            [{"price": f"{p:.4f}", "size": s, "px": p} for p, s in asks_map.items() if s > 0],
            key=lambda x: x["px"]
        )

        best_bid = sorted_bids[0]["px"] if sorted_bids else None
        best_ask = sorted_asks[0]["px"] if sorted_asks else None

        self.books[aid] = {
            "bids": sorted_bids,
            "asks": sorted_asks,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "updated_at": self.last_ts
        }

    def _process_msg(self, data):
        self.last_ts = time.time()
        # Single book dict snapshot
        if isinstance(data, dict) and ("bids" in data or "asks" in data or data.get("event_type") in ("book", "market")):
            aid = str(data.get("asset_id") or data.get("market") or "")
            if aid:
                levels = self.book_levels.setdefault(aid, {"bids": {}, "asks": {}})
                if "bids" in data:
                    levels["bids"] = {}
                    for b in data.get("bids", []):
                        try:
                            levels["bids"][float(b["price"])] = float(b.get("size", 0))
                        except (TypeError, ValueError, KeyError):
                            pass
                if "asks" in data:
                    levels["asks"] = {}
                    for a in data.get("asks", []):
                        try:
                            levels["asks"][float(a["price"])] = float(a.get("size", 0))
                        except (TypeError, ValueError, KeyError):
                            pass
                self._sync_book_entry(aid)
                if data.get("best_bid") is not None:
                    try: self.books[aid]["best_bid"] = float(data["best_bid"])
                    except: pass
                if data.get("best_ask") is not None:
                    try: self.books[aid]["best_ask"] = float(data["best_ask"])
                    except: pass

        # Full book snapshot (list)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("event_type") == "last_trade_price":
                    self._note_trade(item)   # PATCH: longshot maker — a trade, not a book snapshot
                    continue
                if isinstance(item, dict) and "asset_id" in item:
                    aid = str(item["asset_id"])
                    levels = self.book_levels.setdefault(aid, {"bids": {}, "asks": {}})
                    levels["bids"] = {}
                    levels["asks"] = {}
                    for b in item.get("bids", []):
                        try:
                            levels["bids"][float(b["price"])] = float(b.get("size", 0))
                        except (TypeError, ValueError, KeyError):
                            pass
                    for a in item.get("asks", []):
                        try:
                            levels["asks"][float(a["price"])] = float(a.get("size", 0))
                        except (TypeError, ValueError, KeyError):
                            pass
                    self._sync_book_entry(aid)
                    if item.get("best_bid") is not None:
                        try: self.books[aid]["best_bid"] = float(item["best_bid"])
                        except: pass
                    if item.get("best_ask") is not None:
                        try: self.books[aid]["best_ask"] = float(item["best_ask"])
                        except: pass

        # Price / book delta updates
        elif isinstance(data, dict):
            price_changes = data.get("price_changes") or data.get("changes") or []
            if isinstance(price_changes, list):
                for pc in price_changes:
                    if not isinstance(pc, dict): continue
                    aid = str(pc.get("asset_id") or data.get("asset_id") or "")
                    if not aid: continue
                    levels = self.book_levels.setdefault(aid, {"bids": {}, "asks": {}})
                    side = str(pc.get("side", "")).upper()
                    try:
                        p = float(pc["price"])
                        s = float(pc.get("size", 0))
                        if side == "BUY":
                            if s > 0: levels["bids"][p] = s
                            else: levels["bids"].pop(p, None)
                        elif side == "SELL":
                            if s > 0: levels["asks"][p] = s
                            else: levels["asks"].pop(p, None)
                    except (TypeError, ValueError, KeyError):
                        pass

                    self._sync_book_entry(aid)
                    if pc.get("best_bid") is not None:
                        try: self.books[aid]["best_bid"] = float(pc["best_bid"])
                        except (TypeError, ValueError): pass
                    if pc.get("best_ask") is not None:
                        try: self.books[aid]["best_ask"] = float(pc["best_ask"])
                        except (TypeError, ValueError): pass

            aid = str(data.get("asset_id") or "")
            if aid:
                if aid in self.books:
                    self.books[aid]["updated_at"] = self.last_ts
                    if data.get("best_bid") is not None:
                        try: self.books[aid]["best_bid"] = float(data["best_bid"])
                        except (TypeError, ValueError): pass
                    if data.get("best_ask") is not None:
                        try: self.books[aid]["best_ask"] = float(data["best_ask"])
                        except (TypeError, ValueError): pass

            if data.get("event_type") == "last_trade_price":   # PATCH: longshot maker
                self._note_trade(data)

        if self.on_update:
            try:
                self.on_update(data)
            except Exception:
                pass

    def _note_trade(self, d: Dict) -> None:
        """PATCH: longshot maker — a last_trade_price event (a match on this token)."""
        aid = str(d.get("asset_id") or "")
        try:
            px, sz = float(d["price"]), float(d.get("size") or 0.0)
        except (TypeError, ValueError, KeyError):
            return
        if not aid or px <= 0 or sz <= 0:
            return
        q = self.trades.get(aid)
        if q is None:
            q = self.trades[aid] = collections.deque(maxlen=200)
        q.append((time.time(), px, sz, str(d.get("timestamp") or "")))

    def recent_trades(self, asset_id: str, since_ts: float) -> List[tuple]:
        """PATCH: longshot maker — trades on `asset_id` received after since_ts (unix seconds)."""
        return [t for t in self.trades.get(str(asset_id), ()) if t[0] > since_ts]

    def get_token_market(self, asset_id: str) -> Dict:
        return self.books.get(str(asset_id), {})

    def get_summary(self, asset_id: str, max_age_s: float = 5.0) -> Optional[Dict]:
        """Return best bid/ask, spread, liquidity and bids list for `asset_id` if fresh."""
        aid = str(asset_id)
        book = self.books.get(aid)
        if not book:
            return None
        updated_at = book.get("updated_at", 0)
        if max_age_s > 0 and (time.time() - updated_at) > max_age_s:
            return None
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        best_bid = book.get("best_bid")
        best_ask = book.get("best_ask")

        if best_bid is None and bids:
            try:
                best_bid = float(bids[0].get("px", bids[0]["price"]))
            except:
                pass
        if best_ask is None and asks:
            try:
                best_ask = float(asks[0].get("px", asks[0]["price"]))
            except:
                pass

        spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

        bid_liq = 0.0
        for b in bids[:5]:
            try:
                bid_liq += float(b.get("size", 0))
            except:
                pass

        ask_liq = 0.0
        for a in asks[:5]:
            try:
                ask_liq += float(a.get("size", 0))
            except:
                pass

        return {
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "spread": spread,
            "bidLiquidity": bid_liq,
            "askLiquidity": ask_liq,
            "bids": bids,
            "asks": asks,
            "updated_at": updated_at
        }

    def close(self):
        self.closed = True


class ChainlinkPriceStream:
    def __init__(self, aggregator: str, decimals: int = 8, on_update: Optional[Callable] = None):
        self.aggregator = aggregator
        self.decimals = decimals
        self.on_update = on_update
        self.last_price = None
        self.last_updated_at = None
        self.closed = False
        self.wss_urls = settings.POLYGON_WSS_URLS + ([settings.POLYGON_WSS_URL] if settings.POLYGON_WSS_URL else [])

    async def start(self):
        if not self.wss_urls or not self.aggregator:
            return

        url_idx = 0
        call_msg = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "eth_call",
            "params": [
                {"to": self.aggregator, "data": "0xfeaf968c"},
                "latest"
            ]
        }

        while not self.closed:
            url = self.wss_urls[url_idx % len(self.wss_urls)]
            url_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=8.0, sock_read=30.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=20.0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Chainlink RPC WS: {url}")
                        # 1. Subscribe to on-chain AnswerUpdated logs
                        sub_msg = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_subscribe",
                            "params": [
                                "logs",
                                {
                                    "address": self.aggregator,
                                    "topics": ["0x05598845ccd9c46647361c770d3023029a3514781ca1029c91d84f2913e79435"] # AnswerUpdated topic
                                }
                            ]
                        }
                        await ws.send_json(sub_msg)

                        # 2. Query initial latestRoundData() immediately over WS
                        await ws.send_json(call_msg)

                        # 3. Periodic refresh loop over WS to keep sync and serve as keepalive
                        async def _sync_loop():
                            while not self.closed:
                                try:
                                    await asyncio.sleep(5)
                                    if not ws.closed:
                                        await ws.send_json(call_msg)
                                except Exception:
                                    break

                        sync_task = asyncio.create_task(_sync_loop())

                        try:
                            while not self.closed:
                                msg = await ws.receive()
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data_res = fast_loads(msg.data)
                                    # Handle AnswerUpdated event logs
                                    if data_res.get("method") == "eth_subscription":
                                        log = data_res.get("params", {}).get("result", {})
                                        topics = log.get("topics", [])
                                        if len(topics) >= 2:
                                            answer = int(topics[1], 16)
                                            if answer >= 2**255:
                                                answer -= 2**256

                                            self.last_price = answer / (10 ** self.decimals)
                                            data_hex = log.get("data", "0x")
                                            if len(data_hex) >= 66:
                                                self.last_updated_at = int(data_hex[2:66], 16) * 1000
                                            else:
                                                self.last_updated_at = int(time.time() * 1000)

                                            if self.on_update:
                                                res = self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"})
                                                if asyncio.iscoroutine(res):
                                                    asyncio.create_task(res)

                                    # Handle latestRoundData() eth_call response
                                    elif data_res.get("id") == 2 or (isinstance(data_res.get("result"), str) and len(data_res.get("result", "")) >= 130):
                                        res_hex = data_res.get("result", "")
                                        if isinstance(res_hex, str) and len(res_hex) >= 130 and res_hex.startswith("0x"):
                                            try:
                                                hex_body = res_hex[2:]
                                                answer = int(hex_body[64:128], 16)
                                                if answer >= 2**255:
                                                    answer -= 2**256
                                                updated_at = int(hex_body[192:256], 16) * 1000
                                                self.last_price = answer / (10 ** self.decimals)
                                                self.last_updated_at = updated_at

                                                if self.on_update:
                                                    res = self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"})
                                                    if asyncio.iscoroutine(res):
                                                        asyncio.create_task(res)
                                            except Exception:
                                                pass

                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                    break
                        finally:
                            sync_task.cancel()
            except Exception as e:
                print(f"WS Error (Chainlink RPC): {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def get_last(self):
        return {"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"}

    def close(self):
        self.closed = True