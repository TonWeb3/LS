"""
Live trading on Polymarket CLOB V2 via `polymarket-apis`.

Polymarket migrated to CLOB V2. New wallets trade through the gasless
**deposit-wallet** flow (signature_type=3 / POLY_1271):

  - Funds live in a **deposit wallet** derived from your EOA (deposit via Polymarket).
  - Your **private key / seed (EOA)** signs every order — it holds no funds or gas.
  - A **relayer API key** sponsors on-chain setup (wallet deploy + token approvals),
    so you never pay gas.

Legacy accounts (Polymarket proxy / Gnosis safe) are auto-detected and used as-is:
we pick whichever wallet actually holds pUSD. The EOA is derived from PRIVATE_KEY
(hex key or 12/24-word seed phrase), so there is no signature-type/funder to choose
by hand any more.

All clients are synchronous — call from the event loop via `asyncio.to_thread`.
"""

import asyncio
import concurrent.futures
import functools
import sys
import threading
from datetime import datetime
import time
from typing import Optional, Dict, Any, List, Tuple
import requests
from .config import settings

# Dedicated unshared thread pool for live order operations — orders NEVER queue behind
# state saves, balance calls, or status checks on asyncio's shared thread pool
ORDER_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="order-io")


def _tx_hash(tx: Any) -> str:
    """FIX: redeem log. The relayer calls give back the whole transaction receipt; this is its hash
    (what the log, the state file and is_tx_confirmed need). Anything else is passed on as text."""
    h = getattr(tx, "tx_hash", None) or getattr(tx, "transaction_hash", None)
    if h is None and isinstance(tx, dict):
        h = tx.get("tx_hash") or tx.get("transactionHash") or tx.get("transaction_hash")
    return str(h if h is not None else tx)


def _to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _patch_clob_models():
    """Relax over-strict fields in polymarket-apis' models. The live CLOB API omits
    some fields the library marks required (e.g. blockaid_check_enabled), which would
    otherwise crash order placement with a pydantic ValidationError."""
    try:
        import polymarket_apis.types.clob_types as ct
        changed = False
        for name in ("blockaid_check_enabled",):
            f = ct.ClobMarketInfo.model_fields.get(name)
            if f is not None and f.is_required():
                f.default = False
                changed = True
        if changed:
            ct.ClobMarketInfo.model_rebuild(force=True)
    except Exception:
        pass


def _patch_order_builder():
    """Polymarket CLOB V2 strictly enforces:
    - Market buy maker amount: max 2 decimals (collateral/USDC spent)
    - Market buy taker amount: max 4 decimals (tokens/shares received)
    The polymarket-apis library's default ROUNDING_CONFIG specifies amount=5 for 0.001
    and amount=6 for 0.0001 tick sizes, which produces taker amounts with > 4 decimals.
    Polymarket rejects these with:
      HTTP 400 - invalid amounts, the market buy orders maker amount supports a max accuracy of 2 decimals, taker amount a max of 4 decimals.
    This patch clamps ROUNDING_CONFIG to a maximum of 4 decimals and ensures exact integer divisibility:
      maker amount is snapped to 10,000 (10^(6-2) -> max 2 decimals)
      taker amount is snapped to 100 (10^(6-4) -> max 4 decimals)
    """
    try:
        from polymarket_apis.utilities.order_builder.builder import OrderBuilder, ROUNDING_CONFIG, RoundConfig
        for k, rc in list(ROUNDING_CONFIG.items()):
            if rc.amount > 4:
                ROUNDING_CONFIG[k] = RoundConfig(price=rc.price, size=rc.size, amount=min(4, rc.amount))

        orig_get_market = OrderBuilder.get_market_order_amounts

        def safe_get_market_order_amounts(self, side: str, amount: float, price: float, round_config):
            res_side, maker, taker = orig_get_market(self, side, amount, price, round_config)
            if side == "BUY":
                maker = (maker // 10_000) * 10_000
                taker = (taker // 100) * 100
            elif side == "SELL":
                maker = (maker // 100) * 100
                taker = (taker // 10_000) * 10_000
            return res_side, maker, taker

        OrderBuilder.get_market_order_amounts = safe_get_market_order_amounts

        # Polymarket CLOB V2 rule: order.signer MUST match the EOA owning the API key
        # OrderBuilder._v2_order_signer left unpatched: returns self.funder for POLY_1271 (signature_type 3)
    except Exception as e:
        print(f"Warning: Failed to patch OrderBuilder: {e}")


def _patch_crypto_signing():
    """Accelerate EIP-712 cryptographic signing using compiled C coincurve (libsecp256k1).
    Bypasses pure-Python eth-account modular arithmetic, dropping signature creation to sub-0.1ms."""
    try:
        import coincurve
        from eth_account import Account
        from eth_account.datastructures import SignedMessage

        orig_unsafe_sign_hash = Account.unsafe_sign_hash

        def fast_unsafe_sign_hash(message_hash, private_key):
            try:
                if isinstance(private_key, str):
                    pk_bytes = bytes.fromhex(private_key[2:] if private_key.startswith("0x") else private_key)
                elif hasattr(private_key, "key"):
                    pk_bytes = bytes(private_key.key)
                else:
                    pk_bytes = bytes(private_key)

                msg_bytes = bytes.fromhex(message_hash[2:] if str(message_hash).startswith("0x") else message_hash) if isinstance(message_hash, str) else bytes(message_hash)

                pk = coincurve.PrivateKey(pk_bytes)
                sig65 = pk.sign_recoverable(msg_bytes, hasher=None)
                r = int.from_bytes(sig65[:32], "big")
                s = int.from_bytes(sig65[32:64], "big")
                v = sig65[64] + 27
                sig_bytes = sig65[:64] + bytes([v])
                return SignedMessage(
                    messageHash=msg_bytes,
                    r=r,
                    s=s,
                    v=v,
                    signature=sig_bytes,
                )
            except Exception:
                return orig_unsafe_sign_hash(message_hash, private_key)

        Account.unsafe_sign_hash = staticmethod(fast_unsafe_sign_hash)
    except Exception as e:
        print(f"Note: coincurve C-signing patch fallback: {e}")


_patch_clob_models()
_patch_order_builder()
_patch_crypto_signing()


def _ensure_rust_clob():
    """Ensure the native Rust `rust_clob` extension is compiled and imported for the
    current OS/CPU architecture. If not yet installed or if source files were modified,
    automatically rebuilds `./rust_clob` via pip."""
    import os
    import sys
    import shutil
    import subprocess
    import importlib

    proj_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    crate_dir = os.path.join(proj_root, "rust_clob")
    src_file = os.path.join(crate_dir, "src", "lib.rs")
    cargo_file = os.path.join(crate_dir, "Cargo.toml")

    try:
        import rust_clob
        return rust_clob
    except ImportError:
        pass

    if not os.path.isdir(crate_dir):
        raise RuntimeError(f"Missing rust_clob crate directory at {crate_dir}")

    env = os.environ.copy()
    cargo_bin = os.path.expanduser("~/.cargo/bin")
    if os.path.isdir(cargo_bin) and cargo_bin not in env.get("PATH", ""):
        env["PATH"] = f"{cargo_bin}{os.pathsep}{env.get('PATH', '')}"

    if not shutil.which("cargo", path=env.get("PATH")):
        if os.name != "nt":
            print("[rust_clob] Rust toolchain not found — installing rustup automatically...")
            subprocess.run(
                "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal",
                shell=True,
                check=True,
                env=env,
            )
            env["PATH"] = f"{cargo_bin}{os.pathsep}{env.get('PATH', '')}"
        else:
            raise RuntimeError("Rust toolchain (cargo) not found in PATH.")

    print(f"[rust_clob] Compiling/reinstalling native Rust extension from {crate_dir} ...")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--no-deps", "--force-reinstall", crate_dir],
        check=True,
        env=env,
    )
    if "rust_clob" in sys.modules:
        import rust_clob
        return importlib.reload(rust_clob)
    import rust_clob
    return rust_clob


DEPOSIT_WALLET_FACTORY = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07"
DEPOSIT_WALLET_BEACON = "0x7A18EDfe055488A3128f01F563e5B479D92ffc3a"


def derive_beacon_deposit_wallet(eoa_address: str) -> str:
    """Deposit wallet of an account whose wallet was deployed after Polymarket's
    June 29, 2026 upgrade (ERC-1967 beacon proxy). Formula from
    docs.polymarket.com/trading/deposit-wallets:

        walletId = bytes32(owner)                 # owner = the EOA, left-padded
        args     = abi.encode(factory, walletId)
        salt     = keccak256(args)
        wallet   = CREATE2(factory, salt, LibClone.initCodeHashERC1967BeaconProxy(beacon, args))

    The init code includes `args` (Solady's beacon proxy *with* args). The previous
    version hashed the init code without args and returned a different address.
    Checked against real deposit wallets (accounts in the bot's own logs)."""
    from eth_utils import keccak, to_checksum_address

    factory = bytes.fromhex(DEPOSIT_WALLET_FACTORY[2:])
    beacon = bytes.fromhex(DEPOSIT_WALLET_BEACON[2:])
    owner = bytes.fromhex(to_checksum_address(eoa_address)[2:])
    args = factory.rjust(32, b"\x00") + owner.rjust(32, b"\x00")
    init_code = (
        (0x6100523D8160233D3973 + (len(args) << 56)).to_bytes(10, "big")
        + beacon
        + bytes.fromhex("60195155f3363d3d373d3d363d602036600436635c60da")
        + bytes.fromhex("1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6c")
        + bytes.fromhex("b3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3")
        + args
    )
    salt = keccak(args)
    return to_checksum_address(keccak(b"\xff" + factory + salt + keccak(init_code))[12:])


def _is_signer_key_error(text: Any) -> bool:
    t = str(text or "").lower()
    return "order signer address" in t and "api key" in t


class ClobTrader:
    _FILLED_STATUSES = ("matched", "delayed")
    _PARTIAL_FILL_HINTS = (
        "fully filled", "could not be filled", "fill or kill", "killed",
        "unfilled", "insufficient liquidity", "not filled"
    )

    def __init__(self):
        self.clob = None          # PolymarketClobClient — order placement
        self.gasless = None       # PolymarketGaslessWeb3Client — relayer on-chain ops
        self.funder: Optional[str] = None         # funded wallet (deposit/proxy/safe)
        self.signature_type: Optional[int] = None
        self.ready = False
        self.last_error: Optional[str] = None
        self._approvals_done = False
        self._api_creds = None
        self._rust_ready = False
        self._cached_balance = None
        self._last_balance_ts = 0.0
        self._balance_fetching = False
        self._lock = threading.Lock()
        self.wallet_source: str = ""           # "configured" or how auto-detect found the wallet
        self._signer_mode: str = ""            # type-3 order signer in the Rust state: "wallet" | "eoa"
        self._signer_ok: Optional[Tuple[str, str, str]] = None  # (eoa, wallet, mode) Polymarket accepted
        self._rust_can_switch = False          # rust_clob built from the new lib.rs
        self._setup_bg_running = False         # PATCH latency: wallet setup tried in the background
        self._setup_bg_last = 0.0
        self._log_hook = None

    # ── logging into the app log (main.py registers log_message) ────────────────
    def set_log_hook(self, fn):
        self._log_hook = fn

    def _log(self, msg: str):
        fn = self._log_hook
        if fn is not None:
            try:
                fn(msg)
                return
            except Exception:
                pass
        print(msg)

    def reset(self):
        """Drop cached clients so the next call re-initialises with fresh
        credentials (call after the key / relayer settings change)."""
        with self._lock:
            self.clob = None
            self.gasless = None
            self.funder = None
            self.signature_type = None
            self.ready = False
            self.last_error = None
            self._approvals_done = False
            self._api_creds = None
            self._rust_ready = False
            self._cached_balance = None
            self._last_balance_ts = 0.0
            self._balance_fetching = False
            self.wallet_source = ""
            self._signer_mode = ""
            self._rust_can_switch = False
            # _signer_ok is kept: it's tied to (EOA, wallet) and only reused for the same pair
    # ── wallet derivation / detection ───────────────────────────────────────────
    def _derive_creds(self):
        """CLOB API creds derived from the key (L1 auth). Cached. Doubles as the
        gasless client's builder_creds when no relayer key is set."""
        if self._api_creds is not None:
            return self._api_creds
        from polymarket_apis.clients.clob_client import PolymarketClobClient
        from eth_account import Account
        eoa = Account.from_key(settings.PRIVATE_KEY).address
        c = PolymarketClobClient(private_key=settings.PRIVATE_KEY, address=eoa,
                                 chain_id=137, signature_type=3)
        self._api_creds = c.create_or_derive_api_creds()
        return self._api_creds

    def _bind_gasless_funder(self, gasless, signature_type: int, funder: Optional[str]):
        if not funder:
            return gasless
        try:
            from web3 import Web3
            chk = Web3.to_checksum_address(funder)
            gasless.address = chk
            if signature_type == 3:
                gasless.get_expected_deposit_wallet = lambda: chk
                gasless.get_deposit_wallet_address = lambda: chk
            elif signature_type == 2:
                gasless.get_safe_proxy_wallet_address = lambda: chk
                if hasattr(gasless, "safe_abi"):
                    gasless.safe = gasless._contract(chk, gasless.safe_abi)
            elif signature_type == 1:
                gasless.get_poly_proxy_wallet_address = lambda: chk
        except Exception:
            pass
        return gasless

    def _new_gasless(self, signature_type: int, funder: Optional[str] = None):
        # Gasless on-chain client. Two distinct roles:
        #   - relayer key (or builder creds): SUBMITS txs and pays the gas
        #   - rpc_url: READS chain state (pUSD / USDC.e balance, wallet derivation). Use Alchemy
        #     when configured, else POLYGON_RPC_URL or library default.
        from polymarket_apis.clients.web3_client import PolymarketGaslessWeb3Client
        kwargs = {"private_key": settings.PRIVATE_KEY, "signature_type": signature_type}
        if settings.RELAYER_API_KEY:
            kwargs["relayer_api_key"] = settings.RELAYER_API_KEY
        else:
            kwargs["builder_creds"] = self._derive_creds()
        rpc = settings.alchemy_rpc_url() or getattr(settings, "POLYGON_RPC_URL", "")
        if rpc:
            kwargs["rpc_url"] = rpc
        client = PolymarketGaslessWeb3Client(**kwargs)
        if funder:
            self._bind_gasless_funder(client, signature_type, funder)
        return client

    def _get_onchain_balances(self, gasless, address: str) -> Tuple[float, float]:
        """Return (pusd_balance, usdc_e_balance) on-chain for `address`."""
        from web3 import Web3
        chk = Web3.to_checksum_address(address)
        pusd_bal = 0.0
        usdc_e_bal = 0.0
        try:
            pusd_bal = float(gasless.get_pusd_balance(address=chk))
        except Exception:
            pusd_bal = 0.0
        try:
            usdc_e_contract = gasless._contract(gasless.usdc_e_address, gasless.pusd_abi)
            usdc_e_bal = float(usdc_e_contract.functions.balanceOf(chk).call() / 1e6)
        except Exception:
            usdc_e_bal = 0.0
        return pusd_bal, usdc_e_bal

    def _configured_wallet(self, gasless=None) -> Optional[Tuple[int, str]]:
        """(signature_type, address) from Settings → "Funder / Deposit Wallet Address",
        or None when blank. Treated as a deposit wallet (type 3) unless it is the EOA
        itself (type 0) or its on-chain code is a Polymarket proxy (1) or Safe (2)."""
        addr = (getattr(settings, "FUNDER_ADDRESS", "") or "").strip()
        if not (addr.startswith("0x") and len(addr) == 42):
            return None
        try:
            from web3 import Web3
            chk = Web3.to_checksum_address(addr)
        except Exception:
            return None
        eoa = self.get_eoa_address()
        if eoa and chk.lower() == eoa.lower():
            return 0, chk
        st = 3
        if gasless is not None:
            try:
                detected = gasless.detect_wallet_signature_type(chk)
                if detected in (1, 2):
                    st = detected
            except Exception:
                pass
        return st, chk

    def _candidate_wallets(self, gasless) -> List[Tuple[int, str, str]]:
        """(signature_type, address, label) for the wallets this key can own:
        the configured wallet if set (only that one), else the addresses computed
        from the EOA. Addresses read from trade history are NOT used: on a fill where
        you were the resting order, `maker_address` is the other trader's wallet."""
        from web3 import Web3
        out: List[Tuple[int, str, str]] = []

        def add(st, addr, label):
            try:
                if not addr:
                    return
                chk = Web3.to_checksum_address(addr)
                if not any(a.lower() == chk.lower() for _, a, _ in out):
                    out.append((st, chk, label))
            except Exception:
                pass

        configured = self._configured_wallet(gasless)
        if configured:
            add(configured[0], configured[1], "Configured Wallet (Settings)")
            return out

        eoa = self.get_eoa_address()
        if eoa:
            try:
                add(3, derive_beacon_deposit_wallet(eoa), "Deposit Wallet (accounts after Jun 29 2026)")
            except Exception:
                pass
        for st, name, label in (
            (3, "get_expected_deposit_wallet", "Deposit Wallet (accounts before Jun 29 2026)"),
            (1, "get_poly_proxy_wallet_address", "Polymarket Proxy Wallet"),
            (2, "get_safe_proxy_wallet_address", "Gnosis Safe Proxy"),
        ):
            try:
                getter = getattr(gasless, name, None)
                if getter is not None:
                    add(st, getter(), label)
            except Exception:
                pass
        if eoa:
            add(0, eoa, "Personal Signer (EOA)")
        return out

    def _derived_wallets(self, gasless=None) -> Dict[str, str]:
        """label -> address computed from the EOA (no network). Used to check a
        configured wallet against the key. Pass a gasless client that has NOT been
        bound to a funder yet (binding replaces its address getters)."""
        out: Dict[str, str] = {}
        eoa = self.get_eoa_address()
        if not eoa:
            return out
        try:
            out["deposit wallet (after Jun 29 2026)"] = derive_beacon_deposit_wallet(eoa)
        except Exception:
            pass
        try:
            probe = gasless if gasless is not None else self._new_gasless(3)
            for name, label in (("get_expected_deposit_wallet", "deposit wallet (before Jun 29 2026)"),
                                ("get_poly_proxy_wallet_address", "proxy wallet"),
                                ("get_safe_proxy_wallet_address", "safe wallet")):
                try:
                    out[label] = getattr(probe, name)()
                except Exception:
                    pass
        except Exception:
            pass
        return out

    def _pick_funded_wallet(self, gasless) -> Tuple[int, str]:
        """The configured wallet if set. Otherwise the computed wallet holding the most
        pUSD/USDC.e; if none reads above 0, a deposit wallet that is deployed; else the
        post-Jun-29 deposit wallet, logged as a guess. Sets self.wallet_source."""
        configured = self._configured_wallet(gasless)
        if configured:
            self.wallet_source = "configured"
            return configured

        candidates = self._candidate_wallets(gasless)
        best = None  # (st, addr, balance, label)
        for st, addr, label in candidates:
            pusd_bal, usdc_e_bal = self._get_onchain_balances(gasless, addr)
            bal = max(pusd_bal, usdc_e_bal)
            if bal > 0 and (best is None or bal > best[2]):
                best = (st, addr, bal, label)
        if best:
            self.wallet_source = f"auto: {best[3]} holds ${best[2]:,.2f}"
            return best[0], best[1]

        for st, addr, label in candidates:
            if st != 3:
                continue
            try:
                code = gasless.w3.eth.get_code(addr)
                if code and len(code) > 0:
                    self.wallet_source = f"auto: {label}, deployed, balance 0"
                    return st, addr
            except Exception:
                pass

        if candidates:
            st, addr, label = candidates[0]
        else:
            st, addr, label = 3, derive_beacon_deposit_wallet(self.get_eoa_address()), "Deposit Wallet"
        self.wallet_source = f"auto GUESS: {label} (no balance found) — put your wallet address in Settings"
        return st, addr

    def _init_clients(self):
        from polymarket_apis.clients.clob_client import PolymarketClobClient

        # Any gasless client can derive every candidate address; probe with deposit.
        probe = self._new_gasless(3)
        derived = self._derived_wallets(probe)  # before _bind_gasless_funder() rewires the probe
        sig_type, funder = self._pick_funded_wallet(probe)

        gasless = probe if sig_type == 3 else self._new_gasless(sig_type, funder=funder)
        self._bind_gasless_funder(gasless, sig_type, funder)

        clob = PolymarketClobClient(
            private_key=settings.PRIVATE_KEY,
            address=funder,
            chain_id=137,
            signature_type=sig_type,
        )
        clob.set_api_creds(clob.create_or_derive_api_creds())

        # Optimize HTTP client transport: persistent Keep-Alive pool with HTTP/1.1 (bypasses HTTP/2 framing)
        try:
            import httpx
            limits = httpx.Limits(max_keepalive_connections=20, max_connections=30, keepalive_expiry=300.0)
            transport = httpx.HTTPTransport(http1=True, http2=False, limits=limits)
            clob.client = httpx.Client(base_url=clob.base_url, transport=transport, timeout=10.0)
        except Exception:
            pass

        # Initialize native Rust order signer & persistent HTTP/1.1 Keep-Alive sender
        rust_mod = _ensure_rust_clob()
        pk_hex = clob.signer.account.key.hex() if hasattr(clob.signer, "account") else clob.signer.private_key
        creds = clob.creds
        eoa = self.get_eoa_address() or ""
        mode = self._start_signer_mode(sig_type, funder)
        signer = funder if (sig_type == 3 and mode == "wallet") else eoa
        base_args = (
            str(pk_hex),
            str(funder),
            int(sig_type),
            str(creds.key),
            str(creds.secret),
            str(creds.passphrase),
            str(clob.base_url),
            137,
        )
        try:
            rust_mod.init_client(*base_args, signer_address=str(signer))
            self._rust_can_switch = hasattr(rust_mod, "set_signer")
        except TypeError:
            # rust_clob still built from the previous lib.rs: no signer choice there,
            # it always puts the deposit wallet in order.signer for type 3.
            rust_mod.init_client(*base_args)
            self._rust_can_switch = False
            mode = "wallet" if sig_type == 3 else "eoa"
            self._log("WARNING: rust_clob was not rebuilt from the new lib.rs, so the order signer "
                      "can't be switched. Rebuild it: pip install --no-deps --force-reinstall ./rust_clob")
        self._signer_mode = mode
        self._rust_ready = True
        if not all(hasattr(rust_mod, n) for n in ("post_order_async", "warm_connection")):
            self._log("WARNING: rust_clob is an older build without the fast order sender: "
                      "orders wait for a Python thread. If rust_clob was just "
                      "rebuilt, restart the bot to load the new build; otherwise rebuild it: "
                      "pip install --no-deps --force-reinstall ./rust_clob")

        self.gasless = gasless
        self.clob = clob
        self.funder = funder
        self.signature_type = sig_type
        self.ready = True
        self.last_error = None

        self._log(f"LIVE WALLET: EOA {eoa} (from your key) | trading wallet {funder} "
                  f"(sig type {sig_type}, {self.wallet_source or 'auto'}) | order signer: "
                  f"{self._signer_label(mode, sig_type)} (setting: {self._signer_setting()}) | "
                  f"API key {str(creds.key)[:7]} made by the EOA")
        self._check_configured_wallet(funder, sig_type, derived)

    # ── order signer (type 3) ───────────────────────────────────────────────────
    @staticmethod
    def _signer_setting() -> str:
        v = str(getattr(settings, "ORDER_SIGNER", "auto") or "auto").strip().lower()
        return v if v in ("auto", "wallet", "eoa") else "auto"

    def _start_signer_mode(self, sig_type: int, funder: str = "") -> str:
        """"wallet" = order.signer is the deposit wallet (Polymarket's docs);
        "eoa" = order.signer is the key's own address (how this bot ran before).
        Types 0/1/2 always sign as the EOA. In auto, reuse what Polymarket accepted
        earlier in this run for the same EOA + wallet."""
        if sig_type != 3:
            return "eoa"
        setting = self._signer_setting()
        if setting in ("wallet", "eoa"):
            return setting
        ok = self._signer_ok
        eoa = (self.get_eoa_address() or "").lower()
        if ok and ok[0] == eoa and ok[1] == str(funder).lower():
            return ok[2]
        return "wallet"

    @staticmethod
    def _signer_label(mode: str, sig_type: Optional[int] = 3) -> str:
        if sig_type != 3 or mode == "eoa":
            return "EOA"
        return "deposit wallet"

    def _set_rust_signer(self, rust_mod, mode: str) -> bool:
        if not self._rust_can_switch or self.signature_type != 3:
            return False
        try:
            rust_mod.set_signer(str(self.funder) if mode == "wallet" else str(self.get_eoa_address() or ""))
            self._signer_mode = mode
            return True
        except Exception as e:
            self._log(f"LIVE SIGNER: could not switch the order signer: {type(e).__name__}: {e}")
            return False

    def _order_context(self, mode: Optional[str] = None) -> str:
        """What an order carries (maker, signer, type, key), for error lines. `mode`
        names the signer used when it differs from the current Rust state."""
        eoa = self.get_eoa_address() or "?"
        ident = None
        try:
            import sys
            mod = sys.modules.get("rust_clob")
            if mod is not None and hasattr(mod, "get_identity") and mod.is_initialized():
                ident = mod.get_identity()
        except Exception:
            ident = None
        st = (ident or {}).get("signature_type", self.signature_type)
        maker = (ident or {}).get("maker") or self.funder or "?"
        if mode is not None:
            signer = maker if (st == 3 and mode == "wallet") else eoa
        else:
            signer = (ident or {}).get("signer")
        if not signer:
            signer = maker if (st == 3 and self._signer_mode != "eoa") else eoa
        who = "EOA" if str(signer).lower() == str(eoa).lower() else (
            "deposit wallet" if str(signer).lower() == str(maker).lower() else "other")
        key = (self.get_api_key() or "")[:7] or "unknown"
        return f"maker {maker}, signer {signer} ({who}), sig type {st}, API key {key} made by EOA {eoa}"

    def _check_configured_wallet(self, funder: str, sig_type: int, derived: Optional[Dict[str, str]] = None):
        """A configured wallet that isn't computed from this key is the usual cause of
        'the order signer address has to be the address of the API KEY'."""
        if self.wallet_source != "configured" or sig_type == 0:
            return
        if derived is None:
            derived = self._derived_wallets()
        if not derived or any(str(a).lower() == str(funder).lower() for a in derived.values()):
            return
        listed = "; ".join(f"{k} {v}" for k, v in derived.items())
        self._log(f"WARNING: the wallet in Settings ({funder}) is not one of the wallets computed from "
                  f"this key: {listed}. Polymarket refuses orders whose wallet doesn't belong to the key "
                  f"with 'the order signer address has to be the address of the API KEY'.")

    def is_ready(self) -> bool:
        """Check if CLOB client is initialized and ready without triggering blocking setup."""
        return bool(self.ready and self.clob is not None and self._rust_ready)

    def ensure_ready(self) -> bool:
        if self.ready and self.clob is not None and self._rust_ready:
            return True
        with self._lock:
            if self.ready and self.clob is not None and self._rust_ready:
                return True
            if not settings.PRIVATE_KEY:
                self.last_error = "missing_private_key"
                return False
            try:
                self._init_clients()
                return True
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
                self.ready = False
                self.clob = None
                return False

    def ensure_setup(self) -> Dict[str, Any]:
        """One-time gasless on-chain setup: deploy the deposit wallet (if needed) and
        set token approvals, sponsored by the relayer key. Required once before the
        first live order on a fresh deposit wallet."""
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if self._approvals_done:
            return {"ok": True, "skipped": "already_done"}
        if not settings.RELAYER_API_KEY:
            return {"ok": False, "error": "missing_relayer_api_key"}
        try:
            receipts = self.gasless.set_all_approvals()
            self._approvals_done = True
            return {"ok": True, "approvals": len(receipts or [])}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def warm_token(self, token_id: str, tick_size: str = "0.01", neg_risk: bool = False):
        """Pre-warm token fee, tick size, and neg_risk metadata in the CLOB client
        so order creation and EIP-712 signing execute in ~7ms with zero network round trips."""
        if not self.clob or not token_id:
            return
        tid = str(token_id)
        try:
            from polymarket_apis.types.clob_types import FeeInfo
            if hasattr(self.clob, "_PolymarketReadOnlyClobClient__fee_infos"):
                if tid not in self.clob._PolymarketReadOnlyClobClient__fee_infos:
                    self.clob._PolymarketReadOnlyClobClient__fee_infos[tid] = FeeInfo(rate=0, exponent=0)
            if hasattr(self.clob, "_PolymarketReadOnlyClobClient__tick_sizes"):
                self.clob._PolymarketReadOnlyClobClient__tick_sizes[tid] = (tick_size, time.monotonic() + 86400)
            if hasattr(self.clob, "_PolymarketReadOnlyClobClient__neg_risk"):
                self.clob._PolymarketReadOnlyClobClient__neg_risk[tid] = neg_risk
        except Exception:
            pass

    # ── orders ──────────────────────────────────────────────────────────────────
    def _post_order_fast(
        self,
        token_id: str,
        maker_amount: int,
        taker_amount: int,
        side_int: int,
        order_type_str: str,
        expiration_str: str = "0",
        neg_risk: bool = False,
        post_only: bool = False,
    ) -> Dict[str, Any]:
        """Sign EIP-712 / Solady ERC-1271 order and POST via native Rust (`rust_clob`),
        releasing the GIL during network I/O."""
        rust_mod = _ensure_rust_clob()
        post_args = self._post_args(token_id, maker_amount, taker_amount, side_int, order_type_str, expiration_str, neg_risk,
                                    post_only)
        try:
            res = rust_mod.post_order(*post_args)
        except TypeError:
            try:
                res = rust_mod.post_order(*post_args[:-1])
            except TypeError:
                res = rust_mod.post_order(*post_args[:-2])
        return self._finish_post(res, post_args, rust_mod)

    @staticmethod
    def _post_args(token_id, maker_amount, taker_amount, side_int, order_type_str, expiration_str="0", neg_risk=False,
                   post_only=False):
        # PATCH: longshot maker — post_only: Polymarket refuses the order instead of letting it
        # take the book (GTC / GTD only), so a resting bid never pays the taker fee
        return (
            str(token_id),
            int(maker_amount),
            int(taker_amount),
            int(side_int),
            str(order_type_str),
            str(expiration_str),
            bool(neg_risk),
            bool(post_only),
            False,
        )

    def _finish_post(self, res: Dict[str, Any], post_args: tuple, rust_mod) -> Dict[str, Any]:
        """Polymarket's answer to a posted order -> its JSON (raises on refusal). A refused
        order signer is retried once with the other signer, as before."""
        import json

        def detail_of(r) -> str:
            text = r.get("response_text") or ""
            try:
                j = json.loads(text) if text else {}
                if isinstance(j, dict):
                    return str(j.get("error") or j.get("errorMsg") or j.get("message") or text)
            except Exception:
                pass
            return text

        # Type-3 orders: if Polymarket refuses the order signer, try the other signer
        # once (deposit wallet <-> EOA). A refused order was never placed, so this
        # can't double up. Whichever is accepted is kept for the rest of the session.
        retry_note = ""
        second_mode = None
        if (not res.get("ok_http") and int(res.get("http_status") or 0) in (400, 401, 403)
                and self._signer_setting() == "auto" and self.signature_type == 3
                and _is_signer_key_error(detail_of(res))):
            first_mode = self._signer_mode or "wallet"
            first_line = f"HTTP {int(res.get('http_status') or 0)} - {detail_of(res)} [{self._order_context()}]"
            other = "eoa" if first_mode == "wallet" else "wallet"
            if self._set_rust_signer(rust_mod, other):
                try:
                    res = rust_mod.post_order(*post_args)
                except TypeError:
                    try:
                        res = rust_mod.post_order(*post_args[:-1])
                    except TypeError:
                        res = rust_mod.post_order(*post_args[:-2])
                if res.get("ok_http"):
                    self._signer_ok = ((self.get_eoa_address() or "").lower(), str(self.funder).lower(), other)
                    self._log(f"LIVE SIGNER: Polymarket refused order signer = {self._signer_label(first_mode)} "
                              f"and accepted order signer = {self._signer_label(other)}; keeping that. "
                              f"(Refused: {first_line})")
                else:
                    second_mode = other
                    retry_note = f" | first try: {first_line}"
                    self._set_rust_signer(rust_mod, first_mode)

        http_status = int(res.get("http_status") or 0)
        if not res.get("ok_http"):
            if http_status == 0:
                raise RuntimeError((res.get("transport_error") or "rust_clob transport error") + retry_note)
            if second_mode:
                err_detail = f"{detail_of(res)} [{self._order_context(second_mode)}]{retry_note}"
            else:
                err_detail = self.enrich_order_error(detail_of(res))
            raise RuntimeError(f"OrderPlacementError: Order placement failed: HTTP {http_status} - {err_detail}")
        resp_text = res.get("response_text") or ""
        data = json.loads(resp_text) if resp_text else {}
        if isinstance(data, dict):
            data["rust_sign_us"] = res.get("sign_time_us")
            data["rust_total_us"] = res.get("total_time_us")
        return data

    def _market_order(self, token_id, amount, side: str, price, order_type=None) -> Dict[str, Any]:
        from polymarket_apis.types.clob_types import OrderType
        from polymarket_apis.utilities.order_builder.builder import ROUNDING_CONFIG
        ot = order_type if order_type is not None else OrderType.FAK
        order_amount = max(1.00, round(float(amount), 2)) if side == "BUY" else round(float(amount), 4)
        order_price = round(float(price), 4) if price else 0
        self.warm_token(token_id)

        if order_price <= 0:
            order_price = round(
                float(self.clob.calculate_market_price(str(token_id), side, order_amount, ot)),
                4,
            )
        # Polymarket rule: taker amount for BUY must be at least 5 shares
        if side == "BUY" and order_price > 0:
            import math
            min_usd = round(max(1.00, math.ceil(5.0 * order_price * 100.0) / 100.0), 2)
            if order_amount < min_usd:
                order_amount = min_usd
        side_int, maker_amt, taker_amt = self.clob.builder.get_market_order_amounts(
            side, order_amount, order_price, ROUNDING_CONFIG["0.01"]
        )
        ot_str = str(ot.value if hasattr(ot, "value") else ot)
        data = self._post_order_fast(
            token_id=str(token_id),
            maker_amount=maker_amt,
            taker_amount=taker_amt,
            side_int=side_int,
            order_type_str=ot_str,
            expiration_str="0",
            neg_risk=False,
        )
        order_id = (data.get("order_id") or data.get("orderID")
                    or data.get("orderId") or data.get("id"))
        status = str(data.get("status", "")).lower()
        err = data.get("error_msg") or data.get("errorMsg")

        making = _to_float(data.get("making_amount") or data.get("makingAmount"))
        taking = _to_float(data.get("taking_amount") or data.get("takingAmount"))

        if side == "BUY":
            usd, shares = making, taking
        else:
            shares, usd = making, taking

        filled = (data.get("success") is not False
                  and (status in self._FILLED_STATUSES or status in ("matched", "delayed", "filled", "closed"))
                  and shares is not None and shares > 0
                  and usd is not None and usd > 0)

        if not filled:
            if order_id and status in ("delayed", "open"):
                fills = self.get_order_fills(str(order_id), str(token_id))
                if fills:
                    total_shares = sum(float(f["size"]) for f in fills)
                    total_usd = sum(float(f["size"]) * float(f["price"]) for f in fills)
                    return {
                        "ok": True, "response": data, "order_id": order_id, "status": "filled",
                        "fill_price": (total_usd / total_shares) if total_shares else None,
                        "fill_size": total_shares,
                        "fill_usd": total_usd,
                    }
                try:
                    self.cancel_order(str(order_id))
                except Exception:
                    pass

            return {"ok": False, "response": data, "order_id": order_id,
                    "status": status,
                    "error": err or f"not_filled(status={status or 'unknown'})"}

        return {
            "ok": True, "response": data, "order_id": order_id, "status": status,
            "fill_price": (usd / shares) if shares else None,
            "fill_size": shares,
            "fill_usd": usd,
        }

    def place_market_buy(self, token_id: str, usdc_amount: float, price: Optional[float] = None, slippage: Optional[float] = None, order_type: Optional[Any] = None) -> Dict[str, Any]:
        """Marketable BUY for `usdc_amount` USDC of `token_id`. `price`
        is the current quote; the limit is quote + slippage buffer (capped < $1).
        Supports FAK (Fill-And-Kill) and FOK (Fill-Or-Kill)."""
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if not self._approvals_done and settings.RELAYER_API_KEY:
            try:
                self.ensure_setup()
            except Exception:
                pass
        try:
            from polymarket_apis.types.clob_types import OrderType
            slip = slippage if slippage is not None else settings.CLOB_MAX_SLIPPAGE
            limit = min(0.99, float(price) + slip) if price and price > 0 else 0
            cfg_fak = getattr(settings, "COPY_FAK", {}) or {}
            cfg_ot = str(cfg_fak.get("order_type") or "").upper() if isinstance(cfg_fak, dict) else ""
            default_ot = OrderType.FAK if getattr(settings, "CLOB_ALLOW_PARTIAL_FILL", True) else OrderType.FOK
            if order_type is not None:
                ot_str = str(getattr(order_type, "value", order_type)).upper()
                ot = OrderType.FOK if "FOK" in ot_str else OrderType.FAK
            elif cfg_ot:
                ot = OrderType.FOK if "FOK" in cfg_ot else OrderType.FAK
            else:
                ot = default_ot
            return self._market_order(token_id, usdc_amount, "BUY", limit, order_type=ot)
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def place_limit_buy(self, token_id: str, size: float, price: float, expiration: Optional[int] = None,
                        tick_size: str = "0.01", order_type: str = "GTD", post_only: bool = False) -> Dict[str, Any]:
        """Place a Good 'Til Date (GTD) limit BUY order for `size` shares at `price`.
        Sweeps asks up to `price` and rests any remaining unfilled size on the book
        until filled, cancelled, or the expiration timestamp is reached.
        PATCH: longshot — `tick_size` "0.001" keeps sub-cent prices (below 4¢ / above 96¢)
        instead of rounding them to whole cents; `order_type` "FAK" (fill-and-kill) buys
        what's offered up to `price` at once and cancels the rest (no expiration).
        PATCH: longshot maker — `post_only` (GTC / GTD): refused instead of taking the book."""
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if not self._approvals_done and settings.RELAYER_API_KEY:
            self._setup_in_background()
        try:
            tick, otype = self._tick_and_type(tick_size, order_type)
            self.warm_token(token_id, tick)
            order_price, order_size, exp, side_int, maker_amt, taker_amt = self._limit_buy_amounts(size, price, expiration, tick)
            data = self._post_order_fast(
                token_id=str(token_id),
                maker_amount=maker_amt,
                taker_amount=taker_amt,
                side_int=side_int,
                order_type_str=otype,
                expiration_str=str(exp) if otype == "GTD" else "0",
                neg_risk=False,
                post_only=bool(post_only),
            )
            return self._limit_buy_result(data, order_price, order_size, exp)
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    @staticmethod
    def _tick_and_type(tick_size: str, order_type: str) -> Tuple[str, str]:
        """PATCH: longshot — the rounding to sign with ("0.01" unless "0.001" is asked for)
        and the order type ("GTD" unless "FAK", "FOK" or "GTC" is asked for)."""
        from polymarket_apis.utilities.order_builder.builder import ROUNDING_CONFIG
        tick = tick_size if tick_size in ROUNDING_CONFIG else "0.01"
        otype = str(order_type or "GTD").upper()
        return tick, (otype if otype in ("GTD", "FAK", "FOK", "GTC") else "GTD")

    def _limit_buy_amounts(self, size: float, price: float, expiration: Optional[int], tick_size: str = "0.01"):
        from polymarket_apis.utilities.order_builder.builder import ROUNDING_CONFIG
        order_price = round(float(price), 4)
        order_size = round(float(size), 2)
        exp = int(expiration) if expiration and expiration > (time.time() + 180) else int(time.time() + 300)
        side_int, maker_amt, taker_amt = self.clob.builder.get_order_amounts(
            "BUY", order_size, order_price, ROUNDING_CONFIG[tick_size if tick_size in ROUNDING_CONFIG else "0.01"]
        )
        return order_price, order_size, exp, side_int, maker_amt, taker_amt

    @staticmethod
    def _limit_buy_result(data: Dict[str, Any], order_price: float, order_size: float, exp: int) -> Dict[str, Any]:
        order_id = data.get("order_id") or data.get("orderID") or data.get("orderId") or data.get("id")
        status = str(data.get("status", "")).lower()
        return {
            "ok": bool(order_id),
            "order_id": order_id,
            "status": status or "open",
            "price": order_price,
            "size": order_size,
            "expiration": exp,
            "response": data
        }

    def _rust_mod_or_none(self):
        try:
            return _ensure_rust_clob()
        except Exception:
            return None

    def has_fast_sender(self) -> bool:
        """True when the loaded rust_clob build has its own sender threads (post_order_async)."""
        mod = self._rust_mod_or_none()
        return mod is not None and hasattr(mod, "post_order_async") and hasattr(mod, "post_order_wait")

    def _setup_in_background(self):
        """The one-time wallet setup (relayer approvals: several seconds of on-chain calls) used
        to run inside the order call whenever it wasn't marked done, so that order waited for
        it. The fast path sends the order at once and tries the setup in a background thread
        instead, at most once a minute."""
        if self._approvals_done or not settings.RELAYER_API_KEY:
            return
        now = time.time()
        if self._setup_bg_running or now - self._setup_bg_last < 60.0:
            return
        self._setup_bg_running, self._setup_bg_last = True, now
        def run():
            try:
                self.ensure_setup()
            finally:
                self._setup_bg_running = False
        threading.Thread(target=run, name="wallet-setup", daemon=True).start()

    def _order_json(self, res: Dict[str, Any]) -> Dict[str, Any]:
        """rust_clob's answer dict -> Polymarket's JSON for the order; raises on a refusal or a
        transport error. Split out so order response parsing runs cleanly across threads."""
        import json
        http_status = int(res.get("http_status") or 0)
        resp_text = res.get("response_text") or ""
        if not res.get("ok_http"):
            err_detail = resp_text
            try:
                err_json = json.loads(resp_text)
                err_detail = err_json.get("error") or err_json.get("errorMsg") or err_json.get("message") or resp_text
            except Exception:
                pass
            raise RuntimeError(f"OrderPlacementError: Order placement failed: HTTP {http_status} - {err_detail}")
        try:
            return json.loads(resp_text)
        except Exception as e:
            raise RuntimeError(f"Failed to parse order response: {resp_text} ({e})")

    def run_io(self, fn, *args):
        """Run a blocking order call on the dedicated order threads (ORDER_POOL)."""
        return asyncio.get_running_loop().run_in_executor(ORDER_POOL, functools.partial(fn, *args))

    async def place_limit_buy_async(self, token_id: str, size: float, price: float,
                                    expiration: Optional[int] = None, tick_size: str = "0.01",
                                    order_type: str = "GTD", post_only: bool = False) -> Dict[str, Any]:
        """place_limit_buy() without waiting for a Python thread: the order is signed right
        here (about 0.1 ms) and sent by rust_clob's own threads, so it leaves at once even
        while Python is busy with something else. Same result dict. Falls back to
        place_limit_buy() in a dedicated order thread when the client isn't set up yet or rust_clob was
        built before post_order_async existed."""
        import asyncio
        mod = sys.modules.get("rust_clob")
        if (not token_id or not (self.ready and self._rust_ready) or mod is None
                or not hasattr(mod, "post_order_async")
                or (not self._approvals_done and settings.RELAYER_API_KEY)):
            return await self.run_io(self.place_limit_buy, token_id, size, price, expiration, tick_size, order_type,
                                     post_only)
        try:
            tick, otype = self._tick_and_type(tick_size, order_type)
            self.warm_token(token_id, tick)
            order_price, order_size, exp, side_int, maker_amt, taker_amt = self._limit_buy_amounts(size, price, expiration, tick)
            post_args = self._post_args(token_id, maker_amt, taker_amt, side_int, otype,
                                        str(exp) if otype == "GTD" else "0", False,
                                        bool(post_only))
            try:
                ticket = mod.post_order_async(*post_args)
            except TypeError:
                try:
                    ticket = mod.post_order_async(*post_args[:-1])
                except TypeError:
                    ticket = mod.post_order_async(*post_args[:-2])
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}
        try:
            res = await self.run_io(mod.post_order_wait, ticket, 30.0)
            data = await self.run_io(self._finish_post, res, post_args, mod)
            return self._limit_buy_result(data, order_price, order_size, exp)
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def keep_warm(self) -> Optional[Dict[str, Any]]:
        """A small request over the order connection (main.py's order_conn_keeper), so
        it's open when the next order goes out."""
        if not (self.ready and self._rust_ready):
            return None
        mod = sys.modules.get("rust_clob")
        if mod is not None and hasattr(mod, "warm_connection"):
            try:
                return mod.warm_connection("/time")
            except Exception as e:
                pass
        # Fallback: keep connection pool / TLS session to Polymarket CLOB hot
        try:
            if not hasattr(self, "_warm_session") or self._warm_session is None:
                self._warm_session = requests.Session()
            url = f"{getattr(settings, 'CLOB_BASE_URL', 'https://clob.polymarket.com')}/time"
            r = self._warm_session.get(url, timeout=2.0)
            return {"http_status": r.status_code}
        except Exception as e:
            return {"http_status": 0, "transport_error": f"{type(e).__name__}: {e}"}

    def place_limit_sell(self, token_id: str, size: float, price: float, expiration: Optional[int] = None) -> Dict[str, Any]:
        """Place a Good 'Til Date (GTD) limit SELL order for `size` shares at `price`."""
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if not self._approvals_done and settings.RELAYER_API_KEY:
            self._setup_in_background()
        try:
            from polymarket_apis.utilities.order_builder.builder import ROUNDING_CONFIG
            order_price = round(float(price), 4)
            order_size = round(float(size), 2)
            exp = int(expiration) if expiration and expiration > (time.time() + 180) else int(time.time() + 300)
            self.warm_token(token_id)
            side_int, maker_amt, taker_amt = self.clob.builder.get_order_amounts(
                "SELL", order_size, order_price, ROUNDING_CONFIG["0.01"]
            )
            data = self._post_order_fast(
                token_id=str(token_id),
                maker_amount=maker_amt,
                taker_amount=taker_amt,
                side_int=side_int,
                order_type_str="GTD",
                expiration_str=str(exp),
                neg_risk=False,
            )
            order_id = data.get("order_id") or data.get("orderID") or data.get("orderId") or data.get("id")
            status = str(data.get("status", "")).lower()
            return {
                "ok": bool(order_id),
                "order_id": order_id,
                "status": status or "open",
                "price": order_price,
                "size": order_size,
                "expiration": exp,
                "response": data
            }
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel an open order on the CLOB."""
        if not order_id or not self.ensure_ready():
            return {"ok": False, "error": "missing_order_id_or_client"}
        try:
            resp = self.clob.cancel_order(str(order_id))
            data = resp.model_dump() if hasattr(resp, "model_dump") else dict(resp or {})
            return {"ok": True, "response": data}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Fetch order details from Polymarket CLOB /data/order/<id> with Level 2 authentication."""
        if not order_id or not self.ensure_ready() or not self.clob:
            return None
        try:
            from polymarket_apis.types.clob_types import RequestArgs
            from polymarket_apis.utilities.headers import create_level_2_headers
            endpoint = f"/data/order/{order_id}"
            request_args = RequestArgs(method="GET", request_path=endpoint)
            headers = create_level_2_headers(self.clob.signer, self.clob.creds, request_args)
            resp = self.clob.client.get(self.clob._build_url(endpoint), headers=headers)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return None

    def check_order_status(self, order_id: str) -> Dict[str, Any]:
        """Check if an order is still open or has been filled on the CLOB."""
        if not order_id or not self.ensure_ready():
            return {"status": "unknown"}
        try:
            # 1. Check open orders first
            orders = self.clob.get_orders(order_id=str(order_id))
            if orders and len(orders) > 0:
                ord_info = orders[0]
                d = ord_info.model_dump() if hasattr(ord_info, "model_dump") else dict(ord_info)
                size_matched = float(d.get("size_matched") or 0.0)
                original_size = float(d.get("original_size") or 0.0)
                if original_size > 0 and size_matched >= original_size:
                    return {"status": "FILLED", "size_matched": size_matched, "price": d.get("price")}
                return {"status": "OPEN", "size_matched": size_matched, "original_size": original_size, "price": d.get("price")}

            # 2. If not in open orders, query order directly from CLOB /data/order/<id>
            direct = self.get_order(str(order_id))
            if direct:
                status_raw = str(direct.get("status") or "").upper()
                size_matched = float(direct.get("size_matched") or 0.0)
                original_size = float(direct.get("original_size") or 0.0)
                price = float(direct.get("price") or 0.0)
                if status_raw in ("MATCHED", "FILLED") or (original_size > 0 and size_matched >= original_size):
                    return {"status": "FILLED", "size_matched": size_matched if size_matched > 0 else original_size, "price": price}
                elif status_raw in ("CANCELLED", "CANCELED"):
                    return {"status": "CANCELLED", "size_matched": size_matched}
                elif size_matched > 0:
                    return {"status": "OPEN", "size_matched": size_matched, "original_size": original_size, "price": price, "partial": True}

            # 3. Check actual executed trade fills on the CLOB (summing all partial pieces)
            fills = self.get_order_fills(str(order_id))
            if fills:
                total_shares = sum(float(f["size"]) for f in fills)
                total_usd = sum(float(f["size"]) * float(f["price"]) for f in fills)
                avg_price = total_usd / total_shares if total_shares > 0 else 0.0
                return {"status": "FILLED", "size_matched": total_shares, "price": avg_price}

            # IMPORTANT: Newly placed orders take 1-3 seconds to appear in indexer endpoints.
            # Do NOT assume CLOSED on empty/unindexed queries; return OPEN (pending propagation).
            return {"status": "OPEN", "pending": True}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_last_fill(self, token_id: str) -> Optional[Dict[str, Any]]:
        """Most recent on-chain trade for this token from the CLOB's own record —
        an independent confirmation that a reported fill really happened."""
        if not self.ensure_ready():
            return None
        try:
            trades = self.clob.get_trades(token_id=str(token_id))
        except Exception:
            return None
        if not trades:
            return None
        t = trades[0]
        d = t.model_dump() if hasattr(t, "model_dump") else dict(t)
        return {"trade_id": d.get("trade_id"), "side": d.get("side"),
                "price": _to_float(d.get("price")), "size": _to_float(d.get("size")),
                "status": d.get("status"), "match_time": d.get("match_time")}

    def get_order_fills(self, order_id: str, token_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Find all executed fills (including multi-piece fills) matching an order_id."""
        if not order_id or not self.ensure_ready() or not self.clob:
            return []
        try:
            trades = self.clob.get_trades(token_id=str(token_id)) if token_id else self.clob.get_trades()
            if not trades:
                return []
            target = str(order_id).lower()
            matching_fills = []
            for t in trades[:50]:
                td = t.model_dump() if hasattr(t, "model_dump") else dict(t)
                taker_id = str(td.get("taker_order_id") or "").lower()
                maker_orders = td.get("maker_orders") or []

                if taker_id == target:
                    matching_fills.append({
                        "size": float(td.get("size") or 0.0),
                        "price": float(td.get("price") or 0.0),
                        "side": td.get("side"),
                        "match_time": td.get("match_time")
                    })
                    continue

                for m in maker_orders:
                    md = m.model_dump() if hasattr(m, "model_dump") else dict(m)
                    if str(md.get("order_id") or "").lower() == target:
                        matching_fills.append({
                            "size": float(md.get("matched_amount") or td.get("size") or 0.0),
                            "price": float(md.get("price") or td.get("price") or 0.0),
                            "side": td.get("side"),
                            "match_time": td.get("match_time")
                        })
            return matching_fills
        except Exception:
            return []

    def get_token_fills_in_window(self, token_id: str, start_ts: float, end_ts: float,
                                  skip_order_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """This wallet's own BUY fills of one token inside a time window.

        FIX: wallet reconciliation. A record in Polymarket's trades list is the TAKER's trade: its
        size and price are the taker's. When this wallet's order was only one of the makers in it,
        this wallet's part is in the record's maker_orders (matched_amount, price), and that order
        can be on the market's other token. So only these count:
          * a record in which this wallet was the taker and bought: the record's size and price;
          * this wallet's own maker orders in a record, when they bought this token: their own
            matched amount and price.
        skip_order_ids: the orders of the bot's other trades and the maker's open bids; they are
        never counted here."""
        if not token_id or not self.ensure_ready() or not self.clob:
            return []
        try:
            trades = self.clob.get_trades(token_id=str(token_id))
            if not trades:
                return []
            skip = {str(o).lower() for o in (skip_order_ids or []) if o}
            mine = {str(a).lower() for a in (self.funder, self.get_eoa_address()) if a}
            matching = []
            for t in trades[:200]:
                td = t.model_dump() if hasattr(t, "model_dump") else dict(t)
                mt = td.get("match_time")
                trade_ts = None
                if mt:
                    try:
                        if isinstance(mt, (int, float)):
                            trade_ts = float(mt) if mt < 1e11 else float(mt) / 1000.0
                        else:
                            trade_ts = datetime.fromisoformat(str(mt).replace('Z', '+00:00')).timestamp()
                    except Exception:
                        pass
                if not (trade_ts is None or (start_ts - 60 <= trade_ts <= end_ts + 60)):
                    continue
                taker_side = str(td.get("side") or "").upper()
                if str(td.get("trader_side") or "").upper() == "TAKER":
                    # this wallet's own order took: the record's size and price are its own
                    if taker_side == "BUY" and str(td.get("taker_order_id") or "").lower() not in skip:
                        matching.append({"size": float(td.get("size") or 0.0), "price": float(td.get("price") or 0.0),
                                         "side": "BUY", "trade_id": td.get("trade_id")})
                    continue
                # this wallet's order(s) were among the makers: only their own amounts, only on this token
                for m in (td.get("maker_orders") or []):
                    md = m.model_dump() if hasattr(m, "model_dump") else dict(m)
                    if str(md.get("maker_address") or "").lower() not in mine:
                        continue
                    if str(md.get("token_id") or md.get("asset_id") or "") != str(token_id):
                        continue
                    if str(md.get("order_id") or "").lower() in skip:
                        continue
                    # on the taker's own token the maker is on the opposite side; on the market's other
                    # token (matched by minting or merging) it is on the same side
                    same = str(md.get("outcome") or "").strip().lower() == str(td.get("outcome") or "").strip().lower()
                    maker_side = taker_side if not same else ("SELL" if taker_side == "BUY" else "BUY")
                    amount = float(md.get("matched_amount") or 0.0)
                    if maker_side == "BUY" and amount > 0:
                        matching.append({"size": amount, "price": float(md.get("price") or 0.0),
                                         "side": "BUY", "trade_id": td.get("trade_id")})
            return matching
        except Exception:
            return []

    # ── redemption ──────────────────────────────────────────────────────────────
    def enable_auto_redeem(self) -> Dict[str, Any]:
        """Ask Polymarket to auto-redeem resolved positions. Best-effort: if the
        account doesn't support it we fall back to redeeming explicitly."""
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if not hasattr(self.gasless, "auto_redeem_enable"):
            return {"ok": False, "error": "auto_redeem_unsupported"}
        try:
            res = self.gasless.auto_redeem_enable()
            return {"ok": True, "result": str(res)}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def redeem(self, condition_id: str, amounts, neg_risk: bool = False) -> Dict[str, Any]:
        """Redeem a resolved position into pUSD.

        `amounts` is the per-outcome share list the CTF expects ([up, down]); the
        losing leg is simply zero.
        """
        if not condition_id:
            return {"ok": False, "error": "missing_condition_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if not hasattr(self.gasless, "redeem_position"):
            return {"ok": False, "error": "redeem_unsupported_by_client"}
        try:
            amts = [float(a) for a in (amounts if isinstance(amounts, (list, tuple)) else [amounts])]
            tx = self.gasless.redeem_position(condition_id, amts, neg_risk)
            return {"ok": True, "tx": _tx_hash(tx)}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ── capital extractor / withdrawal ─────────────────────────────────────────
    def get_funder(self) -> Optional[str]:
        """Return the active funded wallet address (deposit/proxy/safe)."""
        if self.funder:
            return self.funder
        if not settings.PRIVATE_KEY:
            return None
        try:
            probe = self._new_gasless(3)
            _, funder = self._pick_funded_wallet(probe)
            return funder
        except Exception:
            return None

    def get_funder_address(self) -> Optional[str]:
        return self.get_funder()

    def withdraw_pusd(self, recipient: str, amount: float) -> Dict[str, Any]:
        """Transfer pUSD or USDC.e from the funded wallet to `recipient` via gasless relayer."""
        if not recipient:
            return {"ok": False, "error": "missing_recipient_address"}
        try:
            from web3 import Web3
            recipient = Web3.to_checksum_address(recipient)
        except Exception as e:
            return {"ok": False, "error": f"invalid_recipient_address: {e}"}
        if amount <= 0:
            return {"ok": False, "error": "invalid_withdraw_amount"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        try:
            from web3 import Web3
            amount_int = int(round(float(amount) * 1e6))
            target_gasless = self.gasless
            target_funder = self.funder or self.gasless.address
            pusd_bal, usdc_e_bal = self._get_onchain_balances(target_gasless, target_funder)

            # If active funder appears below amount on-chain, scan all candidate wallets (Deposit, Proxy, Safe)
            if pusd_bal < amount and usdc_e_bal < amount:
                try:
                    scan = self._new_gasless(3)  # unbound: self.gasless's getters now return the funder
                except Exception:
                    scan = self.gasless
                for item in self._candidate_wallets(scan):
                    cand_st, cand_addr = item[0], item[1]
                    if cand_addr.lower() == str(target_funder).lower():
                        continue
                    c_pusd, c_usdce = self._get_onchain_balances(self.gasless, cand_addr)
                    if c_pusd >= amount or c_usdce >= amount:
                        target_funder = cand_addr
                        pusd_bal, usdc_e_bal = c_pusd, c_usdce
                        target_gasless = self._new_gasless(cand_st, funder=cand_addr)
                        break

            # 1. If wallet holds pUSD >= amount, transfer pUSD
            if pusd_bal >= amount:
                data = target_gasless._encode_transfer_pusd(recipient, amount_int)
                tx = target_gasless._execute(
                    target_gasless.pusd_address,
                    data,
                    "pUSD Transfer",
                    metadata="pusd_transfer",
                )
                return {"ok": True, "tx": _tx_hash(tx), "token": "pUSD", "from": target_funder}

            # 2. If wallet holds bridged USDC.e >= amount, transfer USDC.e
            if usdc_e_bal >= amount:
                data = target_gasless._encode_transfer_pusd(recipient, amount_int)
                tx = target_gasless._execute(
                    target_gasless.usdc_e_address,
                    data,
                    "pUSD Transfer",
                    metadata="pusd_transfer",
                )
                return {"ok": True, "tx": _tx_hash(tx), "token": "USDC.e", "from": target_funder}

            # 3. Fallback if RPC balance read returned 0.0 but CLOB API reports sufficient collateral
            clob_bal = 0.0
            try:
                if self.clob is not None:
                    clob_bal = float(self.clob.get_pusd_balance())
            except Exception:
                clob_bal = 0.0

            if clob_bal >= amount:
                data = target_gasless._encode_transfer_pusd(recipient, amount_int)
                try:
                    tx = target_gasless._execute(
                        target_gasless.pusd_address,
                        data,
                        "pUSD Transfer",
                        metadata="pusd_transfer",
                    )
                    return {"ok": True, "tx": _tx_hash(tx), "token": "pUSD", "from": target_funder}
                except Exception:
                    tx = target_gasless._execute(
                        target_gasless.usdc_e_address,
                        data,
                        "pUSD Transfer",
                        metadata="pusd_transfer",
                    )
                    return {"ok": True, "tx": _tx_hash(tx), "token": "USDC.e", "from": target_funder}

            return {
                "ok": False,
                "error": f"Insufficient on-chain balance in {target_funder}: pUSD={pusd_bal:.2f}, USDC.e={usdc_e_bal:.2f} (need {amount:.2f})"
            }
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def is_tx_confirmed(self, tx_hash: str) -> bool:
        """Check whether an on-chain transaction has confirmed on Polygon."""
        if not tx_hash:
            return False
        try:
            if self.gasless and hasattr(self.gasless, "web3") and self.gasless.web3:
                receipt = self.gasless.web3.eth.get_transaction_receipt(tx_hash)
                return receipt is not None and receipt.get("status") == 1
            from web3 import Web3
            rpc = settings.alchemy_rpc_url() or settings.POLYGON_RPC_URL
            w3 = Web3(Web3.HTTPProvider(rpc))
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            return receipt is not None and receipt.get("status") == 1
        except Exception:
            return False

    # ── diagnostics / balance ───────────────────────────────────────────────────
    def get_api_key(self) -> Optional[str]:
        """Return the active L2 CLOB API key string if available."""
        if self.clob and hasattr(self.clob, "creds") and getattr(self.clob.creds, "key", None):
            return str(self.clob.creds.key).strip()
        if self._api_creds and getattr(self._api_creds, "key", None):
            return str(self._api_creds.key).strip()
        return None

    def enrich_order_error(self, err: Any) -> str:
        """Polymarket's error text exactly as sent, plus what the order carried:
        [maker …, signer … (EOA|deposit wallet), sig type …, API key … made by EOA …].
        (The previous version replaced the address in the message with the EOA, so the
        log named the EOA as "the order signer" even when the order was signed as the
        deposit wallet.)"""
        if not err:
            return ""
        s = str(err)
        if "[maker " in s:
            return s
        try:
            return f"{s} [{self._order_context()}]"
        except Exception:
            return s

    def get_eoa_address(self) -> Optional[str]:
        """The EOA address derived from PRIVATE_KEY — the wallet you control (the
        key/seed owner). It signs orders but holds no funds or gas."""
        if not settings.PRIVATE_KEY:
            return None
        try:
            from eth_account import Account
            return Account.from_key(settings.PRIVATE_KEY).address
        except Exception:
            return None

    def test_connection(self) -> Dict[str, Any]:
        """Derive the EOA + its candidate wallets and report pUSD balances — shows
        which wallet holds the funds and which signature type will be used. Read-only
        (no relayer key required)."""
        if not settings.PRIVATE_KEY:
            return {"ok": False, "error": "missing_private_key"}
        try:
            from eth_account import Account
            eoa = Account.from_key(settings.PRIVATE_KEY).address
        except Exception as e:
            return {"ok": False, "error": f"invalid_key: {type(e).__name__}: {e}"}
        try:
            probe = self._new_gasless(3)
            derived = self._derived_wallets(probe)
            wallets = []
            candidates = self._candidate_wallets(probe)
            for item in candidates:
                st, addr = item[0], item[1]
                lbl = item[2] if len(item) > 2 else "Wallet"
                try:
                    p_bal, u_bal = self._get_onchain_balances(probe, addr)
                    bal = max(p_bal, u_bal)
                except Exception:
                    bal = None
                wallets.append({"signature_type": st, "address": addr, "label": lbl, "pusd_balance": bal})
            sig_type, funder = self._pick_funded_wallet(probe)

            # Query CLOB API directly for true tradeable balance
            clob_bal = None
            try:
                from polymarket_apis.clients.clob_client import PolymarketClobClient
                c = PolymarketClobClient(
                    private_key=settings.PRIVATE_KEY,
                    address=funder,
                    chain_id=137,
                    signature_type=sig_type
                )
                c.set_api_creds(c.create_or_derive_api_creds())
                clob_bal = float(c.get_pusd_balance())
            except Exception:
                pass

            if clob_bal is not None and clob_bal >= 0:
                for w in wallets:
                    if w["address"].lower() == funder.lower():
                        w["pusd_balance"] = clob_bal

            warning = ""
            if (self.wallet_source == "configured" and sig_type != 0 and derived
                    and not any(str(a).lower() == str(funder).lower() for a in derived.values())):
                listed = "; ".join(f"{k} {v}" for k, v in derived.items())
                warning = (f"WARNING: the wallet in Settings ({funder}) is not one of the wallets computed "
                           f"from this key: {listed}.")
            return {
                "ok": True,
                "eoa": eoa,
                "chosen_signature_type": sig_type,
                "funder": funder,
                "wallet_source": self.wallet_source,
                "wallet_warning": warning,
                "expected_deposit_wallet": derived.get("deposit wallet (after Jun 29 2026)", ""),
                "relayer_key_set": bool(settings.RELAYER_API_KEY),
                "wallets": wallets,
            }
        except Exception as e:
            return {"ok": False, "eoa": eoa, "error": f"{type(e).__name__}: {e}"}

    def get_usdc_balance(self) -> Optional[float]:
        """pUSD balance of the funded wallet (dollars), or None. pUSD is Polymarket's
        V2 collateral; this is the deposit wallet's tradeable balance."""
        if not settings.PRIVATE_KEY:
            return None
        now = time.time()
        if getattr(self, "_cached_balance", None) is not None and (now - getattr(self, "_last_balance_ts", 0)) < 15.0:
            return self._cached_balance

        if getattr(self, "_balance_fetching", False):
            return getattr(self, "_cached_balance", None)
        self._balance_fetching = True
        try:
            # 1. Direct CLOB API query
            if self.ensure_ready() and self.clob is not None:
                try:
                    c_bal = float(self.clob.get_pusd_balance())
                    if c_bal is not None and c_bal >= 0:
                        self._cached_balance = c_bal
                        self._last_balance_ts = now
                        return c_bal
                except Exception:
                    pass

            # 2. Gasless on-chain query if funder is known
            if self.ready and self.gasless is not None and self.funder:
                try:
                    p_bal, u_bal = self._get_onchain_balances(self.gasless, self.funder)
                    bal = max(p_bal, u_bal)
                    self._cached_balance = bal
                    self._last_balance_ts = now
                    return bal
                except Exception:
                    pass

            # 3. Derive funder once if unknown
            if not self.funder:
                try:
                    probe = self._new_gasless(3)
                    sig_type, funder = self._pick_funded_wallet(probe)
                    self.funder = funder
                    self.signature_type = sig_type
                    p_bal, u_bal = self._get_onchain_balances(probe, funder)
                    bal = max(p_bal, u_bal)
                    self._cached_balance = bal
                    self._last_balance_ts = now
                    return bal
                except Exception:
                    pass

            return getattr(self, "_cached_balance", None)
        finally:
            self._balance_fetching = False

clob_trader = ClobTrader()
