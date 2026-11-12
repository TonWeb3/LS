use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use hmac::{Hmac, Mac};
use k256::ecdsa::SigningKey;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tiny_keccak::{Hasher, Keccak};

type HmacSha256 = Hmac<Sha256>;

const ORDER_TYPE_STRING: &str = "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";
const SOLADY_TYPE_STRING: &str = "TypedDataSign(Order contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt)Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";
const DOMAIN_TYPE_STRING: &str = "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";

const CTF_EXCHANGE_NAME: &str = "Polymarket CTF Exchange";
const CTF_EXCHANGE_VERSION: &str = "2";
const DEPOSIT_WALLET_NAME: &str = "DepositWallet";
const DEPOSIT_WALLET_VERSION: &str = "1";

const EXCHANGE_DEFAULT: &str = "0xE111180000d2663C0091e4f400237545B87B996B";
const EXCHANGE_NEG_RISK: &str = "0xe2222d279d744050d28e00520010520000310F59";
const BYTES32_ZERO_STR: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut k = Keccak::v256();
    let mut out = [0u8; 32];
    k.update(data);
    k.finalize(&mut out);
    out
}

fn parse_address(hex_addr: &str) -> Result<[u8; 20], String> {
    let clean = hex_addr.trim().strip_prefix("0x").or_else(|| hex_addr.trim().strip_prefix("0X")).unwrap_or(hex_addr.trim());
    let bytes = hex::decode(clean).map_err(|e| format!("Invalid address hex '{}': {}", hex_addr, e))?;
    if bytes.len() != 20 {
        return Err(format!("Address must be 20 bytes, got {}", bytes.len()));
    }
    let mut arr = [0u8; 20];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}

fn to_checksum_address(addr: &[u8; 20]) -> String {
    let lower_hex = hex::encode(addr);
    let hash = keccak256(lower_hex.as_bytes());
    let mut out = String::with_capacity(42);
    out.push_str("0x");
    for (i, c) in lower_hex.chars().enumerate() {
        let nibble = if i % 2 == 0 {
            (hash[i / 2] >> 4) & 0x0f
        } else {
            hash[i / 2] & 0x0f
        };
        if c.is_ascii_alphabetic() && nibble >= 8 {
            out.push(c.to_ascii_uppercase());
        } else {
            out.push(c);
        }
    }
    out
}

fn parse_u256_be(dec: &str) -> Result<[u8; 32], String> {
    let s = dec.trim();
    if s.is_empty() {
        return Err("Empty uint256 string".to_string());
    }
    let mut limbs = [0u64; 4];
    for b in s.bytes() {
        if !b.is_ascii_digit() {
            return Err(format!("Invalid digit in uint256: {}", s));
        }
        let digit = (b - b'0') as u128;
        let mut carry = digit;
        for limb in limbs.iter_mut() {
            let prod = (*limb as u128) * 10u128 + carry;
            *limb = prod as u64;
            carry = prod >> 64;
        }
        if carry != 0 {
            return Err("uint256 overflow".to_string());
        }
    }
    let mut out = [0u8; 32];
    out[0..8].copy_from_slice(&limbs[3].to_be_bytes());
    out[8..16].copy_from_slice(&limbs[2].to_be_bytes());
    out[16..24].copy_from_slice(&limbs[1].to_be_bytes());
    out[24..32].copy_from_slice(&limbs[0].to_be_bytes());
    Ok(out)
}

fn compute_domain_separator(chain_id: u64, verifying_contract: &[u8; 20]) -> [u8; 32] {
    let domain_type_hash = keccak256(DOMAIN_TYPE_STRING.as_bytes());
    let name_hash = keccak256(CTF_EXCHANGE_NAME.as_bytes());
    let version_hash = keccak256(CTF_EXCHANGE_VERSION.as_bytes());

    let mut buf = [0u8; 160];
    buf[0..32].copy_from_slice(&domain_type_hash);
    buf[32..64].copy_from_slice(&name_hash);
    buf[64..96].copy_from_slice(&version_hash);
    buf[120..128].copy_from_slice(&chain_id.to_be_bytes());
    buf[140..160].copy_from_slice(verifying_contract);
    keccak256(&buf)
}

struct RustClobState {
    signing_key: SigningKey,
    eoa_checksum: String,
    eoa_bytes: [u8; 20],
    funder_checksum: String,
    funder_bytes: [u8; 20],
    // Address written into order.signer. Types 0/1/2: always the EOA.
    // Type 3: the deposit wallet by default; Python can switch it (set_signer).
    signer_checksum: String,
    signer_bytes: [u8; 20],
    signature_type: u8,
    chain_id: u64,
    api_key: String,
    api_secret_bytes: Vec<u8>,
    api_passphrase: String,
    base_url: String,
    domain_sep_default: [u8; 32],
    domain_sep_neg_risk: [u8; 32],
    order_type_hash: [u8; 32],
    solady_type_hash: [u8; 32],
    deposit_name_hash: [u8; 32],
    deposit_version_hash: [u8; 32],
    order_type_hex_suffix: String,
    agent: ureq::Agent,
}

static STATE: OnceLock<Mutex<Option<RustClobState>>> = OnceLock::new();

fn get_state_mutex() -> &'static Mutex<Option<RustClobState>> {
    STATE.get_or_init(|| Mutex::new(None))
}

fn build_signed_order_internal(
    st: &RustClobState,
    token_id: &str,
    maker_amount: u64,
    taker_amount: u64,
    side: u8,
    expiration: &str,
    neg_risk: bool,
    order_type: &str,
    post_only: bool,
    defer_exec: bool,
    custom_salt: Option<u64>,
    custom_timestamp_ms: Option<u64>,
) -> Result<(String, String), String> {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_nanos() as u64;
    let timestamp_ms = custom_timestamp_ms.unwrap_or(now_ns / 1_000_000);

    // Pseudo-random salt matching polymarket-apis scale
    let salt: u64 = custom_salt.unwrap_or_else(|| {
        let mixed = now_ns.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let frac = (mixed >> 11) as f64 / ((1u64 << 53) as f64);
        (frac * (timestamp_ms as f64)) as u64
    });

    // order.signer comes from the state (see init_client / set_signer):
    // types 0/1/2 -> the EOA; type 3 -> the deposit wallet (Polymarket docs) or,
    // if Python switched it, the EOA.
    let signer_bytes = &st.signer_bytes;
    let signer_checksum = &st.signer_checksum;

    let token_id_be = parse_u256_be(token_id)?;

    // 1. ABI-encode Order struct (12 * 32 = 384 bytes)
    let mut order_buf = [0u8; 384];
    order_buf[0..32].copy_from_slice(&st.order_type_hash);
    order_buf[56..64].copy_from_slice(&salt.to_be_bytes());
    order_buf[76..96].copy_from_slice(&st.funder_bytes);
    order_buf[108..128].copy_from_slice(signer_bytes);
    order_buf[128..160].copy_from_slice(&token_id_be);
    order_buf[184..192].copy_from_slice(&maker_amount.to_be_bytes());
    order_buf[216..224].copy_from_slice(&taker_amount.to_be_bytes());
    order_buf[255] = side;
    order_buf[287] = st.signature_type;
    order_buf[312..320].copy_from_slice(&timestamp_ms.to_be_bytes());
    // word 10 (320..352) = metadata (0x00..00)
    // word 11 (352..384) = builder  (0x00..00)

    let contents_hash = keccak256(&order_buf);
    let app_domain_sep = if neg_risk {
        &st.domain_sep_neg_risk
    } else {
        &st.domain_sep_default
    };

    // 2. Compute digest & signature
    let signature_hex = if st.signature_type == 3 {
        let mut solady_buf = [0u8; 224];
        solady_buf[0..32].copy_from_slice(&st.solady_type_hash);
        solady_buf[32..64].copy_from_slice(&contents_hash);
        solady_buf[64..96].copy_from_slice(&st.deposit_name_hash);
        solady_buf[96..128].copy_from_slice(&st.deposit_version_hash);
        solady_buf[152..160].copy_from_slice(&st.chain_id.to_be_bytes());
        // Word 5 (160..192): verifyingContract in Solady is the deposit wallet contract
        solady_buf[172..192].copy_from_slice(&st.funder_bytes);
        // word 6 (192..224) = DEPOSIT_WALLET_DOMAIN_SALT (0x00..00)

        let typed_data_sign_hash = keccak256(&solady_buf);

        let mut digest_input = [0u8; 66];
        digest_input[0] = 0x19;
        digest_input[1] = 0x01;
        digest_input[2..34].copy_from_slice(app_domain_sep);
        digest_input[34..66].copy_from_slice(&typed_data_sign_hash);
        let digest = keccak256(&digest_input);

        let (sig, recid) = st
            .signing_key
            .sign_prehash_recoverable(&digest)
            .map_err(|e| format!("ECDSA sign failed: {}", e))?;

        let mut sig65 = [0u8; 65];
        sig65[0..64].copy_from_slice(&sig.to_bytes());
        sig65[64] = recid.to_byte() + 27;

        let mut full_sig = String::with_capacity(2 + 130 + 64 + 64 + st.order_type_hex_suffix.len());
        full_sig.push_str("0x");
        full_sig.push_str(&hex::encode(sig65));
        full_sig.push_str(&hex::encode(app_domain_sep));
        full_sig.push_str(&hex::encode(contents_hash));
        full_sig.push_str(&st.order_type_hex_suffix);
        full_sig
    } else {
        let mut digest_input = [0u8; 66];
        digest_input[0] = 0x19;
        digest_input[1] = 0x01;
        digest_input[2..34].copy_from_slice(app_domain_sep);
        digest_input[34..66].copy_from_slice(&contents_hash);
        let digest = keccak256(&digest_input);

        let (sig, recid) = st
            .signing_key
            .sign_prehash_recoverable(&digest)
            .map_err(|e| format!("ECDSA sign failed: {}", e))?;

        let mut sig65 = [0u8; 65];
        sig65[0..64].copy_from_slice(&sig.to_bytes());
        sig65[64] = recid.to_byte() + 27;

        format!("0x{}", hex::encode(sig65))
    };

    let side_str = if side == 0 { "BUY" } else { "SELL" };

    // 3. Serialize compact JSON matching order_to_json + json.dumps(..., separators=(",", ":"))
    let serialized_body = format!(
        "{{\"order\":{{\"salt\":{},\"maker\":\"{}\",\"signer\":\"{}\",\"tokenId\":\"{}\",\"makerAmount\":\"{}\",\"takerAmount\":\"{}\",\"side\":\"{}\",\"expiration\":\"{}\",\"signatureType\":{},\"timestamp\":\"{}\",\"metadata\":\"{}\",\"builder\":\"{}\",\"signature\":\"{}\"}},\"owner\":\"{}\",\"orderType\":\"{}\",\"deferExec\":{},\"postOnly\":{}}}",
        salt,
        st.funder_checksum,
        signer_checksum,
        token_id,
        maker_amount,
        taker_amount,
        side_str,
        expiration,
        st.signature_type,
        timestamp_ms,
        BYTES32_ZERO_STR,
        BYTES32_ZERO_STR,
        signature_hex,
        st.api_key,
        order_type,
        if defer_exec { "true" } else { "false" },
        if post_only { "true" } else { "false" }
    );

    Ok((serialized_body, signature_hex))
}

fn build_l2_hmac(secret_bytes: &[u8], timestamp_sec: &str, method: &str, path: &str, body: &str) -> Result<String, String> {
    let mut mac = HmacSha256::new_from_slice(secret_bytes).map_err(|e| e.to_string())?;
    mac.update(timestamp_sec.as_bytes());
    mac.update(method.as_bytes());
    mac.update(path.as_bytes());
    mac.update(body.as_bytes());
    let result = mac.finalize().into_bytes();
    Ok(URL_SAFE.encode(result))
}

/// The order signer actually used: always the EOA for types 0/1/2; for type 3 the
/// requested address (EOA or deposit wallet), defaulting to the deposit wallet.
fn resolve_signer(
    signature_type: u8,
    requested: &str,
    eoa_bytes: &[u8; 20],
    funder_bytes: &[u8; 20],
) -> Result<[u8; 20], String> {
    if signature_type != 3 {
        return Ok(*eoa_bytes);
    }
    let req = requested.trim();
    if req.is_empty() {
        return Ok(*funder_bytes);
    }
    let b = parse_address(req)?;
    if b != *eoa_bytes && b != *funder_bytes {
        return Err(format!(
            "signer must be the EOA {} or the deposit wallet {}, got {}",
            to_checksum_address(eoa_bytes),
            to_checksum_address(funder_bytes),
            req
        ));
    }
    Ok(b)
}

#[pyfunction]
#[pyo3(signature = (private_key, funder_address, signature_type, api_key, api_secret, api_passphrase, base_url="https://clob.polymarket.com", chain_id=137, signer_address=""))]
fn init_client(
    private_key: &str,
    funder_address: &str,
    signature_type: u8,
    api_key: &str,
    api_secret: &str,
    api_passphrase: &str,
    base_url: &str,
    chain_id: u64,
    signer_address: &str,
) -> PyResult<bool> {
    let clean_pk = private_key.trim().strip_prefix("0x").or_else(|| private_key.trim().strip_prefix("0X")).unwrap_or(private_key.trim());
    let pk_bytes = hex::decode(clean_pk).map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Invalid private key hex: {}", e)))?;
    let signing_key = SigningKey::from_slice(&pk_bytes).map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Invalid secp256k1 key: {}", e)))?;

    let verifying_key = signing_key.verifying_key();
    let pubkey_uncompressed = verifying_key.to_encoded_point(false);
    let pubkey_hash = keccak256(&pubkey_uncompressed.as_bytes()[1..]);
    let mut eoa_bytes = [0u8; 20];
    eoa_bytes.copy_from_slice(&pubkey_hash[12..32]);
    let eoa_checksum = to_checksum_address(&eoa_bytes);

    let funder_bytes = parse_address(funder_address).map_err(pyo3::exceptions::PyValueError::new_err)?;
    let funder_checksum = to_checksum_address(&funder_bytes);
    let signer_bytes = resolve_signer(signature_type, signer_address, &eoa_bytes, &funder_bytes)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    let signer_checksum = to_checksum_address(&signer_bytes);

    let api_secret_bytes = URL_SAFE
        .decode(api_secret.trim())
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Invalid base64 api_secret: {}", e)))?;

    let ex_default_bytes = parse_address(EXCHANGE_DEFAULT).unwrap();
    let ex_neg_risk_bytes = parse_address(EXCHANGE_NEG_RISK).unwrap();

    let domain_sep_default = compute_domain_separator(chain_id, &ex_default_bytes);
    let domain_sep_neg_risk = compute_domain_separator(chain_id, &ex_neg_risk_bytes);

    let order_type_hash = keccak256(ORDER_TYPE_STRING.as_bytes());
    let solady_type_hash = keccak256(SOLADY_TYPE_STRING.as_bytes());
    let deposit_name_hash = keccak256(DEPOSIT_WALLET_NAME.as_bytes());
    let deposit_version_hash = keccak256(DEPOSIT_WALLET_VERSION.as_bytes());

    let contents_type_hex = hex::encode(ORDER_TYPE_STRING.as_bytes());
    let contents_type_len_hex = hex::encode((ORDER_TYPE_STRING.len() as u16).to_be_bytes());
    let order_type_hex_suffix = format!("{}{}", contents_type_hex, contents_type_len_hex);

    let agent = ureq::AgentBuilder::new()
        .timeout_connect(std::time::Duration::from_secs(5))
        .timeout_read(std::time::Duration::from_secs(10))
        .timeout_write(std::time::Duration::from_secs(5))
        .max_idle_connections(20)
        .max_idle_connections_per_host(20)
        .build();

    let state = RustClobState {
        signing_key,
        eoa_checksum,
        eoa_bytes,
        funder_checksum,
        funder_bytes,
        signer_checksum,
        signer_bytes,
        signature_type,
        chain_id,
        api_key: api_key.trim().to_string(),
        api_secret_bytes,
        api_passphrase: api_passphrase.trim().to_string(),
        base_url: base_url.trim_end_matches('/').to_string(),
        domain_sep_default,
        domain_sep_neg_risk,
        order_type_hash,
        solady_type_hash,
        deposit_name_hash,
        deposit_version_hash,
        order_type_hex_suffix,
        agent,
    };

    let mut guard = get_state_mutex().lock().unwrap();
    *guard = Some(state);
    drop(guard);
    let _ = sender(); // start the order sender threads now, not on the first order
    Ok(true)
}

#[pyfunction]
fn is_initialized() -> bool {
    let guard = get_state_mutex().lock().unwrap();
    guard.is_some()
}

/// Switch order.signer without re-initialising. "" = default (deposit wallet for
/// type 3). Types 0/1/2 always stay on the EOA. Returns the signer now in use.
#[pyfunction]
fn set_signer(signer_address: &str) -> PyResult<String> {
    let mut guard = get_state_mutex().lock().unwrap();
    let st = guard.as_mut().ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("rust_clob not initialized"))?;
    let b = resolve_signer(st.signature_type, signer_address, &st.eoa_bytes, &st.funder_bytes)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    st.signer_bytes = b;
    st.signer_checksum = to_checksum_address(&b);
    Ok(st.signer_checksum.clone())
}

/// What the next order will carry: EOA (POLY_ADDRESS header), maker, signer, type.
#[pyfunction]
fn get_identity(py: Python<'_>) -> PyResult<PyObject> {
    let guard = get_state_mutex().lock().unwrap();
    let st = guard.as_ref().ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("rust_clob not initialized"))?;
    let dict = PyDict::new_bound(py);
    dict.set_item("eoa", st.eoa_checksum.clone())?;
    dict.set_item("maker", st.funder_checksum.clone())?;
    dict.set_item("signer", st.signer_checksum.clone())?;
    dict.set_item("signature_type", st.signature_type)?;
    Ok(dict.into())
}

#[pyfunction]
#[pyo3(signature = (token_id, maker_amount, taker_amount, side, order_type="FAK", expiration="0", neg_risk=false, post_only=false, defer_exec=false, custom_salt=None, custom_timestamp_ms=None))]
fn build_signed_order(
    py: Python<'_>,
    token_id: &str,
    maker_amount: u64,
    taker_amount: u64,
    side: u8,
    order_type: &str,
    expiration: &str,
    neg_risk: bool,
    post_only: bool,
    defer_exec: bool,
    custom_salt: Option<u64>,
    custom_timestamp_ms: Option<u64>,
) -> PyResult<PyObject> {
    let t0 = Instant::now();
    let guard = get_state_mutex().lock().unwrap();
    let st = guard.as_ref().ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("rust_clob not initialized"))?;

    let (body, sig) = build_signed_order_internal(
        st,
        token_id,
        maker_amount,
        taker_amount,
        side,
        expiration,
        neg_risk,
        order_type,
        post_only,
        defer_exec,
        custom_salt,
        custom_timestamp_ms,
    )
    .map_err(pyo3::exceptions::PyValueError::new_err)?;

    let ts_sec = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let hmac_sig = build_l2_hmac(&st.api_secret_bytes, &ts_sec, "POST", "/order", &body)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    let elapsed_us = t0.elapsed().as_micros() as u64;

    let dict = PyDict::new_bound(py);
    dict.set_item("body", body)?;
    dict.set_item("signature", sig)?;
    dict.set_item("hmac_signature", hmac_sig)?;
    dict.set_item("timestamp", ts_sec)?;
    dict.set_item("sign_time_us", elapsed_us)?;
    Ok(dict.into())
}

/// A signed order, ready to send.
struct PreparedPost {
    agent: ureq::Agent,
    url: String,
    eoa_checksum: String,
    api_key: String,
    api_passphrase: String,
    ts_sec: String,
    hmac_sig: String,
    body: String,
    sign_us: u64,
    t0: Instant,
}

#[allow(clippy::too_many_arguments)]
fn prepare_post(
    token_id: &str,
    maker_amount: u64,
    taker_amount: u64,
    side: u8,
    order_type: &str,
    expiration: &str,
    neg_risk: bool,
    post_only: bool,
    defer_exec: bool,
) -> PyResult<PreparedPost> {
    let t0 = Instant::now();
    // Sign inside the lock (~40 microseconds); the network part runs without it.
    let guard = get_state_mutex().lock().unwrap();
    let st = guard.as_ref().ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("rust_clob not initialized"))?;

    let (body, _sig) = build_signed_order_internal(
        st,
        token_id,
        maker_amount,
        taker_amount,
        side,
        expiration,
        neg_risk,
        order_type,
        post_only,
        defer_exec,
        None,
        None,
    )
    .map_err(pyo3::exceptions::PyValueError::new_err)?;

    let ts_sec = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let hmac_sig = build_l2_hmac(&st.api_secret_bytes, &ts_sec, "POST", "/order", &body)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    let sign_us = t0.elapsed().as_micros() as u64;

    Ok(PreparedPost {
        agent: st.agent.clone(),
        url: format!("{}/order", st.base_url),
        eoa_checksum: st.eoa_checksum.clone(),
        api_key: st.api_key.clone(),
        api_passphrase: st.api_passphrase.clone(),
        ts_sec,
        hmac_sig,
        body,
        sign_us,
        t0,
    })
}

/// HTTP POST of a signed order over the agent's pooled keep-alive connection.
fn send_prepared(p: &PreparedPost) -> Result<(u16, String), String> {
    let resp = p
        .agent
        .post(&p.url)
        .set("POLY_ADDRESS", &p.eoa_checksum)
        .set("POLY_SIGNATURE", &p.hmac_sig)
        .set("POLY_TIMESTAMP", &p.ts_sec)
        .set("POLY_API_KEY", &p.api_key)
        .set("POLY_PASSPHRASE", &p.api_passphrase)
        .set("Content-Type", "application/json")
        .send_bytes(p.body.as_bytes());

    match resp {
        Ok(r) => {
            let status = r.status();
            let text = r.into_string().unwrap_or_default();
            Ok((status, text))
        }
        Err(ureq::Error::Status(code, r)) => {
            let text = r.into_string().unwrap_or_default();
            Ok((code, text))
        }
        Err(ureq::Error::Transport(t)) => Err(format!("TransportError: {}", t)),
    }
}

fn result_dict(py: Python<'_>, sign_us: u64, total_us: u64, net_res: Result<(u16, String), String>) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("sign_time_us", sign_us)?;
    dict.set_item("total_time_us", total_us)?;

    match net_res {
        Ok((status, text)) => {
            dict.set_item("http_status", status)?;
            dict.set_item("response_text", text)?;
            dict.set_item("ok_http", (200..300).contains(&status))?;
        }
        Err(err_str) => {
            dict.set_item("http_status", 0)?;
            dict.set_item("response_text", "")?;
            dict.set_item("ok_http", false)?;
            dict.set_item("transport_error", err_str)?;
        }
    }
    Ok(dict.into())
}

#[pyfunction]
#[pyo3(signature = (token_id, maker_amount, taker_amount, side, order_type="FAK", expiration="0", neg_risk=false, post_only=false, defer_exec=false))]
#[allow(clippy::too_many_arguments)]
fn post_order(
    py: Python<'_>,
    token_id: &str,
    maker_amount: u64,
    taker_amount: u64,
    side: u8,
    order_type: &str,
    expiration: &str,
    neg_risk: bool,
    post_only: bool,
    defer_exec: bool,
) -> PyResult<PyObject> {
    let p = prepare_post(token_id, maker_amount, taker_amount, side, order_type, expiration, neg_risk, post_only, defer_exec)?;
    // Release Python GIL while executing HTTP POST over pre-warmed connection pool
    let (sign_us, t0) = (p.sign_us, p.t0);
    let net_res = py.allow_threads(move || send_prepared(&p));
    let total_us = t0.elapsed().as_micros() as u64;
    result_dict(py, sign_us, total_us, net_res)
}

// ── Sending without a Python thread ─────────────────────────────────────────────
// post_order_async() signs on the calling thread and hands the HTTP request to one of
// these Rust threads, so the order leaves without waiting for a Python worker thread or
// the GIL. post_order_wait() collects the answer.
struct SendJob {
    id: u64,
    prep: PreparedPost,
}

struct SendDone {
    sign_us: u64,
    total_us: u64,
    res: Result<(u16, String), String>,
}

const SENDER_THREADS: usize = 4;
static NEXT_SEND_ID: AtomicU64 = AtomicU64::new(1);
static SEND_TX: OnceLock<Mutex<mpsc::Sender<SendJob>>> = OnceLock::new();
static SEND_DONE: OnceLock<(Mutex<HashMap<u64, SendDone>>, Condvar)> = OnceLock::new();

fn send_done() -> &'static (Mutex<HashMap<u64, SendDone>>, Condvar) {
    SEND_DONE.get_or_init(|| (Mutex::new(HashMap::new()), Condvar::new()))
}

fn sender() -> &'static Mutex<mpsc::Sender<SendJob>> {
    SEND_TX.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<SendJob>();
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..SENDER_THREADS {
            let rx = rx.clone();
            let _ = std::thread::Builder::new()
                .name(format!("rust_clob-send-{}", i))
                .spawn(move || loop {
                    let job = {
                        let r = rx.lock().unwrap();
                        r.recv()
                    };
                    let job = match job {
                        Ok(j) => j,
                        Err(_) => break,
                    };
                    let res = send_prepared(&job.prep);
                    let total_us = job.prep.t0.elapsed().as_micros() as u64;
                    let (m, cv) = send_done();
                    let mut done = m.lock().unwrap();
                    if done.len() > 256 {
                        done.clear(); // answers nobody collected (shouldn't happen)
                    }
                    done.insert(job.id, SendDone { sign_us: job.prep.sign_us, total_us, res });
                    cv.notify_all();
                });
        }
        Mutex::new(tx)
    })
}

/// Sign now and start sending on a Rust thread. Returns a ticket for post_order_wait().
#[pyfunction]
#[pyo3(signature = (token_id, maker_amount, taker_amount, side, order_type="FAK", expiration="0", neg_risk=false, post_only=false, defer_exec=false))]
#[allow(clippy::too_many_arguments)]
fn post_order_async(
    token_id: &str,
    maker_amount: u64,
    taker_amount: u64,
    side: u8,
    order_type: &str,
    expiration: &str,
    neg_risk: bool,
    post_only: bool,
    defer_exec: bool,
) -> PyResult<u64> {
    let prep = prepare_post(token_id, maker_amount, taker_amount, side, order_type, expiration, neg_risk, post_only, defer_exec)?;
    let id = NEXT_SEND_ID.fetch_add(1, Ordering::Relaxed);
    sender()
        .lock()
        .unwrap()
        .send(SendJob { id, prep })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("order sender stopped: {}", e)))?;
    Ok(id)
}

/// The answer to a post_order_async() ticket (same dict as post_order). Waits up to
/// timeout_s without holding the GIL.
#[pyfunction]
#[pyo3(signature = (ticket, timeout_s=30.0))]
fn post_order_wait(py: Python<'_>, ticket: u64, timeout_s: f64) -> PyResult<PyObject> {
    let done = py.allow_threads(move || {
        let (m, cv) = send_done();
        let deadline = Instant::now() + Duration::from_secs_f64(timeout_s.max(0.0));
        let mut map = m.lock().unwrap();
        loop {
            if let Some(d) = map.remove(&ticket) {
                return Some(d);
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            map = cv.wait_timeout(map, deadline - now).unwrap().0;
        }
    });
    match done {
        Some(d) => result_dict(py, d.sign_us, d.total_us, d.res),
        None => result_dict(py, 0, 0, Err("TransportError: no answer from the order sender in time".to_string())),
    }
}

/// A small GET over the order connection (default: the server time), so the connection is
/// open when the next order goes out (an idle one gets closed by the server, and opening a
/// new one costs DNS, TCP and TLS round trips).
#[pyfunction]
#[pyo3(signature = (path="/time"))]
fn warm_connection(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let (agent, url) = {
        let guard = get_state_mutex().lock().unwrap();
        let st = guard.as_ref().ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("rust_clob not initialized"))?;
        (st.agent.clone(), format!("{}{}", st.base_url, path))
    };
    let t0 = Instant::now();
    let res = py.allow_threads(move || match agent.get(&url).call() {
        Ok(r) => {
            let s = r.status();
            let _ = r.into_string();
            Ok(s)
        }
        Err(ureq::Error::Status(code, r)) => {
            let _ = r.into_string();
            Ok(code)
        }
        Err(ureq::Error::Transport(t)) => Err(format!("TransportError: {}", t)),
    });
    let dict = PyDict::new_bound(py);
    dict.set_item("total_time_us", t0.elapsed().as_micros() as u64)?;
    match res {
        Ok(s) => {
            dict.set_item("http_status", s)?;
        }
        Err(e) => {
            dict.set_item("http_status", 0)?;
            dict.set_item("transport_error", e)?;
        }
    }
    Ok(dict.into())
}

#[pymodule]
fn rust_clob(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_client, m)?)?;
    m.add_function(wrap_pyfunction!(is_initialized, m)?)?;
    m.add_function(wrap_pyfunction!(set_signer, m)?)?;
    m.add_function(wrap_pyfunction!(get_identity, m)?)?;
    m.add_function(wrap_pyfunction!(build_signed_order, m)?)?;
    m.add_function(wrap_pyfunction!(post_order, m)?)?;
    m.add_function(wrap_pyfunction!(post_order_async, m)?)?;
    m.add_function(wrap_pyfunction!(post_order_wait, m)?)?;
    m.add_function(wrap_pyfunction!(warm_connection, m)?)?;
    Ok(())
}

