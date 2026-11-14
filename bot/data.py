import httpx
from .config import settings
from typing import List, Dict, Optional
from .net_utils import get_proxy_url_for

_CLIENTS: Dict[str, httpx.AsyncClient] = {}

def get_client(proxy: Optional[str] = None) -> httpx.AsyncClient:
    key = proxy or "default"
    c = _CLIENTS.get(key)
    if c is None or c.is_closed:
        limits = httpx.Limits(max_keepalive_connections=30, max_connections=50, keepalive_expiry=120.0)
        _CLIENTS[key] = httpx.AsyncClient(proxy=proxy if proxy else None, limits=limits, timeout=10.0)
    return _CLIENTS[key]

def to_number(x) -> Optional[float]:
    try:
        if x is None:
            return None
        n = float(x)
        return n
    except (TypeError, ValueError):
        return None

async def fetch_klines(symbol: str, interval: str, limit: int) -> List[Dict]:
    """Fetch historical klines from Binance REST API (api.binance.com) with no fallbacks."""
    host = settings.BINANCE_BASE_URL or "https://api.binance.com"
    url = f"{host}/api/v3/klines"
    proxy = get_proxy_url_for(url)
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        client = get_client(proxy)
        res = await client.get(url, params=params)
        if res.status_code == 200:
            data = res.json()
            if isinstance(data, list) and len(data) > 0:
                klines = [{
                    "openTime": int(k[0]),
                    "open": to_number(k[1]),
                    "high": to_number(k[2]),
                    "low": to_number(k[3]),
                    "close": to_number(k[4]),
                    "volume": to_number(k[5]),
                    "closeTime": int(k[6])
                } for k in data if len(k) >= 7 and to_number(k[4]) is not None]
                if len(klines) >= min(limit, 15):
                    return klines
    except Exception as e:
        print(f"Warning: fetch_klines failed: {e}")
    return []

async def fetch_last_price(symbol: str) -> Optional[float]:
    """Fetch ticker price from Binance REST API (api.binance.com) with no fallbacks."""
    host = settings.BINANCE_BASE_URL or "https://api.binance.com"
    url = f"{host}/api/v3/ticker/price"
    proxy = get_proxy_url_for(url)
    params = {"symbol": symbol}
    try:
        client = get_client(proxy)
        res = await client.get(url, params=params)
        if res.status_code == 200:
            data = res.json()
            p = to_number(data.get("price"))
            if p and p > 0:
                return p
    except Exception:
        pass
    return None

async def fetch_market_by_slug(slug: str) -> Optional[Dict]:
    url = f"{settings.GAMMA_BASE_URL}/markets"
    params = {"slug": slug}
    proxy = get_proxy_url_for(url)
    client = get_client(proxy)
    res = await client.get(url, params=params)
    res.raise_for_status()
    data = res.json()
    market = data[0] if isinstance(data, list) and data else data
    return market if isinstance(market, dict) and market.get("id") else None

async def fetch_live_events_by_series_id(series_id: str) -> List[Dict]:
    url = f"{settings.GAMMA_BASE_URL}/events"
    params = {"series_id": series_id, "closed": "false", "limit": 20}
    proxy = get_proxy_url_for(url)
    client = get_client(proxy)
    res = await client.get(url, params=params)
    res.raise_for_status()
    data = res.json()
    return data if isinstance(data, list) else []

def flatten_event_markets(events: List[Dict]) -> List[Dict]:
    markets = []
    for event in events:
        for m in event.get("markets", []):
            if isinstance(m, dict):
                markets.append(m)
    return markets

async def fetch_clob_price(token_id: str, side: str = "buy") -> Optional[float]:
    if not token_id:
        return None
    url = f"{settings.CLOB_BASE_URL}/price"
    params = {"token_id": token_id, "side": side}
    proxy = get_proxy_url_for(url)
    try:
        client = get_client(proxy)
        res = await client.get(url, params=params)
        if res.status_code == 200:
            data = res.json()
            return to_number(data.get("price"))
    except Exception:
        pass
    return None

async def fetch_order_book(token_id: str) -> Dict:
    url = f"{settings.CLOB_BASE_URL}/book"
    params = {"token_id": token_id}
    proxy = get_proxy_url_for(url)
    client = get_client(proxy)
    res = await client.get(url, params=params)
    res.raise_for_status()
    return res.json()

def summarize_order_book(book: Dict) -> Dict:
    bids = book.get("bids", [])
    asks = book.get("asks", [])

    def parse_entry(entry):
        if isinstance(entry, dict):
            return to_number(entry.get("price")), to_number(entry.get("size"))
        elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
            return to_number(entry[0]), to_number(entry[1])
        return None, None

    valid_bids = []
    for b in bids:
        p, s = parse_entry(b)
        if p is not None and s is not None:
            valid_bids.append((p, s))

    valid_asks = []
    for a in asks:
        p, s = parse_entry(a)
        if p is not None and s is not None:
            valid_asks.append((p, s))

    valid_bids.sort(key=lambda x: x[0], reverse=True)
    valid_asks.sort(key=lambda x: x[0])

    best_bid = valid_bids[0][0] if valid_bids else None
    best_ask = valid_asks[0][0] if valid_asks else None
    spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

    bid_liq = sum(s for _, s in valid_bids[:5]) if valid_bids else 0.0
    ask_liq = sum(s for _, s in valid_asks[:5]) if valid_asks else 0.0

    return {
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "spread": spread,
        "bidLiquidity": bid_liq,
        "askLiquidity": ask_liq
    }

async def fetch_available_series(timeframe: Optional[str] = None) -> List[Dict]:
    """Fetch available Up or Down series for both 15m and 5m market strategies."""
    url = f"{settings.GAMMA_BASE_URL}/events"
    proxy = get_proxy_url_for(url)
    client = get_client(proxy)

    series_map = {
        "10192": {"series_id": "10192", "title": "[15M] BTC Up or Down 15m", "slug": "btc-up-or-down-15m", "timeframe": "15m", "candle_window_minutes": 15},
        "10212": {"series_id": "10212", "title": "[15M] ETH Up or Down 15m", "slug": "eth-up-or-down-15m", "timeframe": "15m", "candle_window_minutes": 15},
        "10423": {"series_id": "10423", "title": "[15M] SOL Up or Down 15m", "slug": "sol-up-or-down-15m", "timeframe": "15m", "candle_window_minutes": 15},
        "10422": {"series_id": "10422", "title": "[15M] XRP Up or Down 15m", "slug": "xrp-up-or-down-15m", "timeframe": "15m", "candle_window_minutes": 15},
        "10684": {"series_id": "10684", "title": "[5M] BTC Up or Down 5m", "slug": "btc-up-or-down-5m", "timeframe": "5m", "candle_window_minutes": 5},
        "10683": {"series_id": "10683", "title": "[5M] ETH Up or Down 5m", "slug": "eth-up-or-down-5m", "timeframe": "5m", "candle_window_minutes": 5},
        "10686": {"series_id": "10686", "title": "[5M] SOL Up or Down 5m", "slug": "sol-up-or-down-5m", "timeframe": "5m", "candle_window_minutes": 5},
        "10685": {"series_id": "10685", "title": "[5M] XRP Up or Down 5m", "slug": "xrp-up-or-down-5m", "timeframe": "5m", "candle_window_minutes": 5},
    }

    tags_to_check = [("102467", "15m", 15), ("102892", "5m", 5)]
    if timeframe == "15m":
        tags_to_check = [("102467", "15m", 15)]
    elif timeframe == "5m":
        tags_to_check = [("102892", "5m", 5)]

    for tag_id, tf, mins in tags_to_check:
        try:
            params = {"active": "true", "closed": "false", "limit": 100, "tag_id": tag_id}
            res = await client.get(url, params=params)
            if res.status_code == 200:
                events = res.json()
                for e in events:
                    if not isinstance(e, dict):
                        continue
                    series_slug = e.get("seriesSlug", "") or ""
                    sid = e.get("series_id")
                    if not sid and e.get("series") and isinstance(e["series"], list) and len(e["series"]) > 0:
                        sid = e["series"][0].get("id")
                    if sid and (f"-{tf}" in series_slug or "up-or-down" in series_slug):
                        asset = series_slug.split("-")[0].upper()
                        series_map[str(sid)] = {
                            "series_id": str(sid),
                            "title": f"[{tf.upper()}] {asset} Up or Down {tf}",
                            "slug": series_slug,
                            "timeframe": tf,
                            "candle_window_minutes": mins
                        }
        except Exception:
            pass

    items = list(series_map.values())
    if timeframe:
        items = [s for s in items if s.get("timeframe") == timeframe]
    return sorted(items, key=lambda x: (0 if x["timeframe"] == "15m" else 1, x["title"]))

async def fetch_available_15m_series() -> List[Dict]:
    return await fetch_available_series()

def sweep_sell(bids: List[Dict], size: float) -> Optional[float]:
    """Calculate the effective VWAP sell price for `size` shares against order book bids.
    Returns None if book has 0 liquidity."""
    if not bids or size <= 0:
        return None
    sorted_bids = []
    for b in bids:
        p = to_number(b.get("price"))
        s = to_number(b.get("size"))
        if p is not None and s is not None and s > 0:
            sorted_bids.append((p, s))
    sorted_bids.sort(key=lambda x: x[0], reverse=True)

    rem = size
    total_val = 0.0
    total_size = 0.0
    for p, s in sorted_bids:
        take = min(rem, s)
        total_val += take * p
        total_size += take
        rem -= take
        if rem <= 0:
            break
    if total_size <= 0:
        return None
    return total_val / total_size
