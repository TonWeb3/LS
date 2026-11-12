import os
import json
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict, Any, Optional, Tuple

CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config.json"))


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    MODE: str = "hybrid"  # Unified hybrid copy-trading engine
    PAPER_BALANCE_USD: float = 1000.0
    PRIVATE_KEY: str = ""

    # ── Copy-trader continuous GTD execution ───────────────────────────────────
    COPY_RETRY_INTERVAL_MS: int = 300
    COPY_MAX_RETRIES: int = 5
    COPY_RESTING_TIMEOUT_S: float = 2.0

    # ── Live trading (Polymarket CLOB V2) ───────────────────────────────────────
    # The wallet is DERIVED from PRIVATE_KEY (hex key or 12/24-word seed phrase) and
    # auto-detected: deposit-wallet (V2, signature_type 3) first, then legacy proxy /
    # safe — whichever actually holds pUSD. Nothing to pick by hand.
    CLOB_MAX_SLIPPAGE: float = 0.02  # marketable-limit buffer above the quote (probability units, 2¢)
    EXIT_MAX_RETRIES: int = 5
    FUNDER_ADDRESS: str = ""         # optional explicit deposit/funder wallet override (bypasses guessing)
    ORDER_SIGNER: str = "auto"       # order signer mode: auto, wallet, or eoa
    CLOB_ALLOW_PARTIAL_FILL: bool = True  # Allow partial fill for thin books
    RELAYER_API_KEY: str = ""        # Polymarket relayer API key (sponsors gasless on-chain setup)
    ALCHEMY_API_KEY: str = ""        # optional: dedicated Polygon RPC for chain reads

    SYMBOL: str = "BTCUSDT"
    BINANCE_BASE_URL: str = "https://api.binance.com"
    GAMMA_BASE_URL: str = "https://gamma-api.polymarket.com"
    CLOB_BASE_URL: str = "https://clob.polymarket.com"

    POLL_INTERVAL_MS: int = 1000
    CANDLE_WINDOW_MINUTES: int = 15

    # Risk per trade: "percent" = RISK_VALUE% of balance; "fixed" = RISK_VALUE dollars.
    RISK_TYPE: str = "percent"
    RISK_VALUE: float = 10.0

    # ── Latency-arb entry engine ────────────────────────────────────────────────
    # Fair probability (fast, from Binance spot) vs Polymarket's implied price.
    # Enter when EV = fair - ask_price clears EV_THRESHOLD (the book looks stale).
    EV_THRESHOLD: float = 0.04          # require >= this expected value per $1 share (after price)
    MIN_PROB_EV: float = 0.55           # don't bet near-coinflips even if EV looks positive
    MIN_BOOK_LIQUIDITY_USD: float = 20.0  # skip if the ask side can't absorb the stake
    MAX_BOOK_AGE_S: float = 3.0

    # After a window expires, wait this long for Polymarket to publish its OFFICIAL
    # outcome before falling back to our own close-vs-open comparison. Without this the
    # local fallback always won the race and the authoritative result was never used.
    AUTHORITATIVE_SETTLE_WAIT_S: float = 90.0
    # PATCH: official results — both off: the bot books and redeems exactly as before
    REDEEM_AFTER_RESOLUTION: bool = False   # redeem a win once Polymarket has resolved the market, and retry (main.py _or_pass)
    CONFIRM_OFFICIAL: bool = False          # check every booked result against Polymarket's own and correct the list

    # Close-and-flip the open position on a strong opposite signal
    FLIP_ENABLED: bool = False
    FLIP_MIN_CONVICTION: float = 0.80   # opposite side's adjusted prob must be >= this
    FLIP_MIN_MINUTES_LEFT: float = 9.0  # and at least this much time left in the window

    RSI_PERIOD: int = 14

    # ── Capital Extractor (Auto-Withdrawal) ─────────────────────────────────────
    AUTO_WITHDRAW_ENABLED: bool = False
    WITHDRAW_TRIGGER_BALANCE: float = 200.0
    WITHDRAW_AMOUNT: float = 100.0
    WITHDRAW_ADDRESS: str = ""
    WITHDRAW_AUTO_RESUME: bool = True
    WITHDRAW_RESUME_AFTER: str = "flat"  # "flat" or "next_window"

    # ── Telegram Notifications ─────────────────────────────────────────────────
    TELEGRAM_ENABLED: bool = False
    TELEGRAM_BOT_TOKEN: str = ""

    # Polymarket
    POLYMARKET_SLUG: str = os.getenv("POLYMARKET_SLUG", "")
    POLYMARKET_SERIES_ID: str = os.getenv("POLYMARKET_SERIES_ID", "10192")
    POLYMARKET_SERIES_SLUG: str = os.getenv("POLYMARKET_SERIES_SLUG", "btc-up-or-down-15m")
    POLYMARKET_AUTO_SELECT_LATEST: bool = os.getenv("POLYMARKET_AUTO_SELECT_LATEST", "true").lower() == "true"
    POLYMARKET_LIVE_DATA_WS_URL: str = os.getenv("POLYMARKET_LIVE_WS_URL", "wss://ws-live-data.polymarket.com")
    POLYMARKET_UP_LABEL: str = os.getenv("POLYMARKET_UP_LABEL", "Up")
    POLYMARKET_DOWN_LABEL: str = os.getenv("POLYMARKET_DOWN_LABEL", "Down")

    # Chainlink
    POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon.drpc.org")
    POLYGON_RPC_URLS: List[str] = [url.strip() for url in os.getenv("POLYGON_RPC_URLS", "").split(",") if url.strip()]
    POLYGON_WSS_URL: str = os.getenv("POLYGON_WSS_URL", "wss://polygon-bor-rpc.publicnode.com")
    POLYGON_WSS_URLS: List[str] = [url.strip() for url in os.getenv("POLYGON_WSS_URLS", "").split(",") if url.strip()]
    CHAINLINK_BTC_USD_AGGREGATOR: str = os.getenv("CHAINLINK_BTC_USD_AGGREGATOR", "0xc907E116054Ad103354f2D350FD2514433D57F6f")

    CHAINLINK_AGGREGATORS: Dict[str, str] = {
        "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
        "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
        "SOL": "0x39771505D18301D239916F4C88367A6010F7D2e3",
        "XRP": "0x3454796324D6469C3110996E2E10972688045F19",
        "DOGE": "0xbAf93Ba318f77363f82E8896a2E830206121D506",
        "BNB": "0x82a6C67606bdc0409f959f60608226064223A57c"
    }

    def alchemy_rpc_url(self) -> str:
        """Dedicated Polygon RPC for chain reads (pUSD balance, wallet derivation).
        Empty string => the library's default public RPC."""
        return f"https://polygon-mainnet.g.alchemy.com/v2/{self.ALCHEMY_API_KEY}" if self.ALCHEMY_API_KEY else ""

    def get_aggregator(self, symbol: str) -> str:
        s = symbol.upper()
        if s.endswith("USDT"): s = s[:-4]
        return self.CHAINLINK_AGGREGATORS.get(s, self.CHAINLINK_BTC_USD_AGGREGATOR)

    # Proxy
    # ── Long shots ───────────────────────────────────────────────────────────────
    LONGSHOT: Dict[str, Any] = {}

    HTTP_PROXY: str = os.getenv("HTTP_PROXY", os.getenv("http_proxy", ""))
    HTTPS_PROXY: str = os.getenv("HTTPS_PROXY", os.getenv("https_proxy", ""))
    ALL_PROXY: str = os.getenv("ALL_PROXY", os.getenv("all_proxy", ""))


def normalize_funder_address(addr: str) -> str:
    """Validate and checksum user-specified funder/deposit wallet address."""
    addr = (addr or "").strip()
    if not addr or not addr.startswith("0x") or len(addr) != 42:
        return ""
    try:
        from eth_utils import to_checksum_address
        return to_checksum_address(addr)
    except Exception:
        return addr

def normalize_private_key(secret: str) -> str:
    """Accept either a raw hex private key or a 12/24-word seed phrase and return a
    hex private key. EOA only — the trading wallet is derived from this secret and
    nothing else. Returns "" for empty input. Raises if a seed phrase can't be parsed."""
    secret = (secret or "").strip()
    if not secret:
        return ""
    # A mnemonic is several space-separated words; a private key is a single token.
    if len(secret.split()) >= 12:
        from eth_account import Account
        Account.enable_unaudited_hdwallet_features()
        key = Account.from_mnemonic(secret).key.hex()
        return key if key.startswith("0x") else "0x" + key
    return secret


def normalize_order_signer(val: Any) -> Optional[str]:
    """Validate order signer setting: auto, wallet, or eoa."""
    if not val:
        return "auto"
    s = str(val).strip().lower()
    if s in ("auto", "wallet", "eoa"):
        return s
    return None


def load_settings():
    base_settings = Settings()
    try:
        from bot import longshot as _longshot
        base_settings.LONGSHOT = _longshot.merged({})
    except Exception:
        base_settings.LONGSHOT = {}
    config_path = CONFIG_PATH if os.path.exists(CONFIG_PATH) else "config.json"
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                config_data = json.load(f)

            if "mode" in config_data: base_settings.MODE = config_data["mode"]
            if "paper_balance_usd" in config_data: base_settings.PAPER_BALANCE_USD = config_data["paper_balance_usd"]
            if "funder_address" in config_data:
                base_settings.FUNDER_ADDRESS = normalize_funder_address(config_data["funder_address"])
            if "live" in config_data and "funder_address" in config_data["live"]:
                base_settings.FUNDER_ADDRESS = normalize_funder_address(config_data["live"]["funder_address"])
            if "private_key" in config_data:
                base_settings.PRIVATE_KEY = normalize_private_key(config_data["private_key"])

            if "copy_trader" in config_data:
                ct = config_data["copy_trader"]
                if "retry_interval_ms" in ct: base_settings.COPY_RETRY_INTERVAL_MS = int(ct["retry_interval_ms"])
                if "max_retries" in ct: base_settings.COPY_MAX_RETRIES = int(ct["max_retries"])
                if "resting_timeout_s" in ct: base_settings.COPY_RESTING_TIMEOUT_S = float(ct["resting_timeout_s"])

            if "relayer" in config_data:
                rl = config_data["relayer"]
                if "api_key" in rl: base_settings.RELAYER_API_KEY = rl["api_key"]

            if "live" in config_data:
                live = config_data["live"]
                if "max_slippage" in live: base_settings.CLOB_MAX_SLIPPAGE = float(live["max_slippage"])
                if "exit_max_retries" in live: base_settings.EXIT_MAX_RETRIES = int(live["exit_max_retries"])
                if "allow_partial_fill" in live: base_settings.CLOB_ALLOW_PARTIAL_FILL = bool(live["allow_partial_fill"])
                if "order_signer" in live: base_settings.ORDER_SIGNER = normalize_order_signer(live["order_signer"]) or "auto"

            if "polymarket" in config_data:
                poly = config_data["polymarket"]
                if "gamma_base_url" in poly: base_settings.GAMMA_BASE_URL = poly["gamma_base_url"]
                if "clob_base_url" in poly: base_settings.CLOB_BASE_URL = poly["clob_base_url"]
                if "live_ws_url" in poly: base_settings.POLYMARKET_LIVE_DATA_WS_URL = poly["live_ws_url"]
                if "series_id" in poly: base_settings.POLYMARKET_SERIES_ID = poly["series_id"]
                if "series_slug" in poly: base_settings.POLYMARKET_SERIES_SLUG = poly["series_slug"]
                if "auto_select_latest" in poly: base_settings.POLYMARKET_AUTO_SELECT_LATEST = poly["auto_select_latest"]
                if "up_label" in poly: base_settings.POLYMARKET_UP_LABEL = poly["up_label"]
                if "down_label" in poly: base_settings.POLYMARKET_DOWN_LABEL = poly["down_label"]

            if "trading" in config_data:
                trading = config_data["trading"]
                raw_sym = str(trading.get("symbol") or "").strip().upper()
                base_settings.SYMBOL = raw_sym if raw_sym else "BTCUSDT"
                if "binance_base_url" in trading: base_settings.BINANCE_BASE_URL = trading["binance_base_url"]
                if "candle_window_minutes" in trading: base_settings.CANDLE_WINDOW_MINUTES = trading["candle_window_minutes"]
                if "poll_interval_ms" in trading: base_settings.POLL_INTERVAL_MS = trading["poll_interval_ms"]
                if "risk_type" in trading: base_settings.RISK_TYPE = trading["risk_type"]
                if "risk_value" in trading: base_settings.RISK_VALUE = trading["risk_value"]

            if "ev" in config_data:
                ev = config_data["ev"]
                if "ev_threshold" in ev: base_settings.EV_THRESHOLD = float(ev["ev_threshold"])
                if "min_prob" in ev: base_settings.MIN_PROB_EV = float(ev["min_prob"])
                if "min_book_liquidity_usd" in ev: base_settings.MIN_BOOK_LIQUIDITY_USD = float(ev["min_book_liquidity_usd"])
                if "max_book_age_s" in ev: base_settings.MAX_BOOK_AGE_S = float(ev["max_book_age_s"])

            if "settlement" in config_data:
                st = config_data["settlement"]
                if "authoritative_settle_wait_s" in st:
                    base_settings.AUTHORITATIVE_SETTLE_WAIT_S = float(st["authoritative_settle_wait_s"])
                # PATCH: official results — true/false, also when written as text ("false" is off)
                if "redeem_after_resolution" in st:
                    base_settings.REDEEM_AFTER_RESOLUTION = str(st["redeem_after_resolution"]).strip().lower() in ("true", "1", "yes", "on")
                if "confirm_official" in st:
                    base_settings.CONFIRM_OFFICIAL = str(st["confirm_official"]).strip().lower() in ("true", "1", "yes", "on")

            if "flip" in config_data:
                flip = config_data["flip"]
                if "enabled" in flip: base_settings.FLIP_ENABLED = bool(flip["enabled"])
                if "min_conviction" in flip: base_settings.FLIP_MIN_CONVICTION = float(flip["min_conviction"])
                if "min_minutes_left" in flip: base_settings.FLIP_MIN_MINUTES_LEFT = float(flip["min_minutes_left"])

            if "capital_extractor" in config_data:
                ce = config_data["capital_extractor"]
                if "enabled" in ce: base_settings.AUTO_WITHDRAW_ENABLED = bool(ce["enabled"])
                if "trigger_balance" in ce: base_settings.WITHDRAW_TRIGGER_BALANCE = float(ce["trigger_balance"])
                if "withdraw_amount" in ce: base_settings.WITHDRAW_AMOUNT = float(ce["withdraw_amount"])
                if "recipient_address" in ce: base_settings.WITHDRAW_ADDRESS = str(ce["recipient_address"]).strip()
                if "auto_resume" in ce: base_settings.WITHDRAW_AUTO_RESUME = bool(ce["auto_resume"])
                if "resume_after" in ce: base_settings.WITHDRAW_RESUME_AFTER = str(ce["resume_after"]).strip()

            if "telegram" in config_data:
                tg = config_data["telegram"]
                if "enabled" in tg: base_settings.TELEGRAM_ENABLED = bool(tg["enabled"])
                if "bot_token" in tg: base_settings.TELEGRAM_BOT_TOKEN = str(tg["bot_token"]).strip()

            if "chainlink" in config_data:
                cl = config_data["chainlink"]
                if "polygon_rpc_url" in cl: base_settings.POLYGON_RPC_URL = cl["polygon_rpc_url"]
                if "polygon_wss_url" in cl: base_settings.POLYGON_WSS_URL = cl["polygon_wss_url"]
                if "btc_usd_aggregator" in cl: base_settings.CHAINLINK_BTC_USD_AGGREGATOR = cl["btc_usd_aggregator"]
                if "alchemy_api_key" in cl: base_settings.ALCHEMY_API_KEY = cl["alchemy_api_key"]

            if "longshot" in config_data:
                try:
                    from bot import longshot as _longshot
                    base_settings.LONGSHOT = _longshot.normalize(config_data["longshot"])
                except Exception as e:
                    print(f"Warning: Failed to load longshot config: {e}")

            # Legacy flat keys fallback
            if "auto_withdraw_enabled" in config_data and "capital_extractor" not in config_data:
                base_settings.AUTO_WITHDRAW_ENABLED = bool(config_data["auto_withdraw_enabled"])
            if "withdraw_trigger_balance" in config_data and "capital_extractor" not in config_data:
                base_settings.WITHDRAW_TRIGGER_BALANCE = float(config_data["withdraw_trigger_balance"])
            if "withdraw_amount" in config_data and "capital_extractor" not in config_data:
                base_settings.WITHDRAW_AMOUNT = float(config_data["withdraw_amount"])
            if "withdraw_address" in config_data and "capital_extractor" not in config_data:
                base_settings.WITHDRAW_ADDRESS = str(config_data["withdraw_address"]).strip()
            if "telegram_enabled" in config_data and "telegram" not in config_data:
                base_settings.TELEGRAM_ENABLED = bool(config_data["telegram_enabled"])
            if "telegram_bot_token" in config_data and "telegram" not in config_data:
                base_settings.TELEGRAM_BOT_TOKEN = str(config_data["telegram_bot_token"]).strip()

        except Exception as e:
            print(f"Warning: Failed to load config.json: {e}")

    return base_settings

settings = load_settings()
