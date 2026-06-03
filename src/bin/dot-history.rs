//! Backfill a year (or any window) of DOT spot prices and 24h rolling VWAP from
//! seven exchanges (same set as the live `dot-price` binary, minus Gate.io).
//! Writes NDJSON in the
//! exact same schema as `results.ndjson` / `vwap.ndjson` / `errors.ndjson` so the
//! existing `chart/generate.ts` can render the output unchanged.
//!
//! Strategy: fetch each exchange's 15m candle history in parallel, align all
//! candles on a single 15m UTC grid, then walk the grid emitting one row per
//! 15-minute slot. The VWAP row at slot `t` is the volume-weighted price over
//! the prior 96 slots (= 24 hours) per exchange.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

use dot_price::{get_json, open_sink, parse_price, unix_secs, write_line, TIMEOUT, USER_AGENT};

/// 15m in seconds.
const STEP: u64 = 900;
/// Number of trailing slots that make up the rolling 24h window.
const VWAP_WINDOW: usize = 96;
/// Per-call pacing for each exchange's pagination loop. Keeps us comfortably
/// under every venue's documented rate limit while eight exchanges run
/// concurrently.
const PER_CALL_DELAY_MS: u64 = 500;
/// Attempts before giving up on a single paginated request.
const RETRY_ATTEMPTS: u32 = 4;

/// One aligned candle from one exchange.
#[derive(Clone, Copy, Debug)]
struct Candle {
    ts: u64,
    close: f64,
    quote_volume: f64,
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Config {
    from: u64,
    to: u64,
    results_path: String,
    vwap_path: String,
    errors_path: String,
    quiet: bool,
}

fn parse_args() -> Result<Config> {
    let now = unix_secs();
    let mut cfg = Config {
        from: now.saturating_sub(365 * 86_400),
        to: now,
        results_path: "results_historical.ndjson".to_string(),
        vwap_path: "vwap_historical.ndjson".to_string(),
        errors_path: "errors_historical.ndjson".to_string(),
        quiet: false,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--from" => cfg.from = take_u64(&mut args, "--from")?,
            other if other.starts_with("--from=") => {
                cfg.from = other.trim_start_matches("--from=").parse()?
            }
            "--to" => cfg.to = take_u64(&mut args, "--to")?,
            other if other.starts_with("--to=") => {
                cfg.to = other.trim_start_matches("--to=").parse()?
            }
            "--results" | "-o" => cfg.results_path = take_str(&mut args, "--results")?,
            other if other.starts_with("--results=") => {
                cfg.results_path = other.trim_start_matches("--results=").to_string()
            }
            "--vwap-results" => cfg.vwap_path = take_str(&mut args, "--vwap-results")?,
            other if other.starts_with("--vwap-results=") => {
                cfg.vwap_path = other.trim_start_matches("--vwap-results=").to_string()
            }
            "--errors" | "-e" => cfg.errors_path = take_str(&mut args, "--errors")?,
            other if other.starts_with("--errors=") => {
                cfg.errors_path = other.trim_start_matches("--errors=").to_string()
            }
            "--quiet" | "-q" => cfg.quiet = true,
            "-h" | "--help" => {
                println!(
                    "usage: dot-history [--from <unix>] [--to <unix>]\n\
                     \x20                 [--results <file>] [--vwap-results <file>] [--errors <file>]\n\
                     \x20                 [--quiet]\n\
                     \n\
                     Backfills 15m DOT candles across 7 exchanges for the window\n\
                     [from, to) (defaults: now-365d .. now) and writes NDJSON in the\n\
                     same schema as the live dot-price logs."
                );
                std::process::exit(0);
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }
    if cfg.to <= cfg.from {
        return Err(anyhow!("--to must be greater than --from"));
    }
    Ok(cfg)
}

fn take_str(it: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    it.next().ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn take_u64(it: &mut impl Iterator<Item = String>, flag: &str) -> Result<u64> {
    take_str(it, flag)?
        .parse()
        .with_context(|| format!("invalid value for {flag}"))
}

// ---------------------------------------------------------------------------
// Per-exchange fetchers
// ---------------------------------------------------------------------------

/// Snap a unix timestamp to the start of its 15m grid slot.
fn snap(ts: u64) -> u64 {
    ts - ts % STEP
}

/// Retry a fallible async fetch with exponential backoff.
async fn with_retry<F, Fut, T>(mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay_ms = 500u64;
    for i in 0..RETRY_ATTEMPTS {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if i + 1 == RETRY_ATTEMPTS => return Err(e),
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms * 2).min(8_000);
            }
        }
    }
    unreachable!()
}

// --- Binance: forward-paginate by startTime ---------------------------------

async fn fetch_binance(client: &reqwest::Client, from: u64, to: u64) -> Result<Vec<Candle>> {
    // Kline rows: [openTime, open, high, low, close, volume, closeTime, quoteVolume, ...]
    let mut out = Vec::new();
    let mut start_ms = from * 1000;
    let end_ms = to * 1000;
    loop {
        let url = format!(
            "https://data-api.binance.vision/api/v3/klines?symbol=DOTUSDT&interval=15m&startTime={start_ms}&endTime={end_ms}&limit=1000"
        );
        let rows: Vec<serde_json::Value> = with_retry(|| get_json(client, &url)).await?;
        if rows.is_empty() {
            break;
        }
        let mut newest_open_ms = start_ms;
        for r in &rows {
            let arr = r.as_array().ok_or_else(|| anyhow!("non-array kline row"))?;
            let open_ms = arr[0].as_u64().ok_or_else(|| anyhow!("missing openTime"))?;
            let close = parse_price(arr[4].as_str().unwrap_or(""))?;
            let qvol = parse_price(arr[7].as_str().unwrap_or(""))?;
            newest_open_ms = newest_open_ms.max(open_ms);
            out.push(Candle {
                ts: snap(open_ms / 1000),
                close,
                quote_volume: qvol,
            });
        }
        if rows.len() < 1000 {
            break;
        }
        start_ms = newest_open_ms + STEP * 1000;
        if start_ms >= end_ms {
            break;
        }
        tokio::time::sleep(Duration::from_millis(PER_CALL_DELAY_MS)).await;
    }
    Ok(out)
}

// --- OKX history-candles: reverse-paginate by `after` -----------------------

#[derive(Deserialize)]
struct OkxResp {
    data: Vec<Vec<String>>,
}

async fn fetch_okx(client: &reqwest::Client, from: u64, to: u64) -> Result<Vec<Candle>> {
    // Each candle: [ts(ms), open, high, low, close, vol(base), volCcy(quote), ...]
    // history-candles returns up to 100 newest-first; pass `after` = older bound
    // (exclusive) to step further back in time.
    let mut out = Vec::new();
    let mut after_ms = to * 1000;
    let from_ms = from * 1000;
    loop {
        let url = format!(
            "https://www.okx.com/api/v5/market/history-candles?instId=DOT-USDT&bar=15m&after={after_ms}&limit=100"
        );
        let resp: OkxResp = with_retry(|| get_json(client, &url)).await?;
        if resp.data.is_empty() {
            break;
        }
        let mut oldest_ms = after_ms;
        for r in &resp.data {
            let open_ms: u64 = r
                .first()
                .ok_or_else(|| anyhow!("missing ts"))?
                .parse()
                .context("ts parse")?;
            if open_ms < from_ms {
                continue;
            }
            let close = parse_price(r.get(4).ok_or_else(|| anyhow!("missing close"))?)?;
            let qvol = parse_price(r.get(6).ok_or_else(|| anyhow!("missing volCcy"))?)?;
            oldest_ms = oldest_ms.min(open_ms);
            out.push(Candle {
                ts: snap(open_ms / 1000),
                close,
                quote_volume: qvol,
            });
        }
        // Stop once we've crossed the lower bound or the page wasn't full.
        if oldest_ms <= from_ms || resp.data.len() < 100 {
            break;
        }
        after_ms = oldest_ms;
        tokio::time::sleep(Duration::from_millis(PER_CALL_DELAY_MS)).await;
    }
    Ok(out)
}

// --- Coinbase: forward by `start`/`end` (epoch seconds accepted) ------------

async fn fetch_coinbase(client: &reqwest::Client, from: u64, to: u64) -> Result<Vec<Candle>> {
    // Each candle: [time, low, high, open, close, volume]. Base volume only;
    // approximate quote_volume ≈ close × volume (same workaround as the live
    // VWAP fetcher in dot-price).
    let mut out = Vec::new();
    let mut start = from;
    let chunk = STEP * 300; // 300 candles per call max
    while start < to {
        let end = (start + chunk).min(to);
        let url = format!(
            "https://api.exchange.coinbase.com/products/DOT-USD/candles?start={start}&end={end}&granularity=900"
        );
        let rows: Vec<Vec<f64>> = with_retry(|| get_json(client, &url)).await?;
        for r in &rows {
            if r.len() < 6 {
                continue;
            }
            let t = r[0] as u64;
            if t < from || t >= to {
                continue;
            }
            let close = r[4];
            let vol = r[5];
            out.push(Candle {
                ts: snap(t),
                close,
                quote_volume: close * vol,
            });
        }
        start = end;
        tokio::time::sleep(Duration::from_millis(PER_CALL_DELAY_MS)).await;
    }
    Ok(out)
}

// --- Kraken: forward by `since` ---------------------------------------------

#[derive(Deserialize)]
struct KrakenResp {
    result: serde_json::Value,
    #[allow(dead_code)]
    #[serde(default)]
    error: Vec<String>,
}

async fn fetch_kraken(client: &reqwest::Client, from: u64, to: u64) -> Result<Vec<Candle>> {
    // Each candle: [time, open, high, low, close, vwap, volume(base), count].
    // quote_volume = vwap × volume.
    let mut out = Vec::new();
    let mut since = from;
    loop {
        let url =
            format!("https://api.kraken.com/0/public/OHLC?pair=DOTUSD&interval=15&since={since}");
        let resp: KrakenResp = with_retry(|| get_json(client, &url)).await?;
        let obj = resp
            .result
            .as_object()
            .ok_or_else(|| anyhow!("kraken: result not an object"))?;
        let last = obj
            .get("last")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow!("kraken: missing last"))?;
        let pair_rows = obj
            .iter()
            .find(|(k, _)| *k != "last")
            .and_then(|(_, v)| v.as_array())
            .ok_or_else(|| anyhow!("kraken: no pair array"))?;
        if pair_rows.is_empty() {
            break;
        }
        let mut newest_t = since;
        for r in pair_rows {
            let arr = r.as_array().ok_or_else(|| anyhow!("kraken row not array"))?;
            let t = arr[0].as_u64().ok_or_else(|| anyhow!("kraken row ts"))?;
            if t < from || t >= to {
                continue;
            }
            let close = parse_price(arr[4].as_str().unwrap_or(""))?;
            let vwap = parse_price(arr[5].as_str().unwrap_or(""))?;
            let base = parse_price(arr[6].as_str().unwrap_or(""))?;
            newest_t = newest_t.max(t);
            out.push(Candle {
                ts: snap(t),
                close,
                quote_volume: vwap * base,
            });
        }
        // `last` is the recommended cursor for the next call.
        if last <= since || last >= to {
            break;
        }
        since = last;
        if newest_t == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(PER_CALL_DELAY_MS)).await;
    }
    Ok(out)
}

// --- Bybit: reverse by `end` ------------------------------------------------

#[derive(Deserialize)]
struct BybitResp {
    result: BybitResult,
}
#[derive(Deserialize)]
struct BybitResult {
    list: Vec<Vec<String>>,
}

async fn fetch_bybit(client: &reqwest::Client, from: u64, to: u64) -> Result<Vec<Candle>> {
    // Each candle (newest-first): [startTime(ms), open, high, low, close, volume, turnover]
    // turnover = quote volume.
    let mut out = Vec::new();
    let mut end_ms = to * 1000;
    let from_ms = from * 1000;
    loop {
        let url = format!(
            "https://api.bybit.com/v5/market/kline?category=spot&symbol=DOTUSDT&interval=15&end={end_ms}&limit=1000"
        );
        let resp: BybitResp = with_retry(|| get_json(client, &url)).await?;
        if resp.result.list.is_empty() {
            break;
        }
        let mut oldest_ms = end_ms;
        for r in &resp.result.list {
            if r.len() < 7 {
                continue;
            }
            let open_ms: u64 = r[0].parse().context("bybit ts parse")?;
            if open_ms < from_ms || open_ms >= to * 1000 {
                continue;
            }
            let close = parse_price(&r[4])?;
            let qvol = parse_price(&r[6])?;
            oldest_ms = oldest_ms.min(open_ms);
            out.push(Candle {
                ts: snap(open_ms / 1000),
                close,
                quote_volume: qvol,
            });
        }
        if oldest_ms <= from_ms || resp.result.list.len() < 1000 {
            break;
        }
        end_ms = oldest_ms - 1;
        tokio::time::sleep(Duration::from_millis(PER_CALL_DELAY_MS)).await;
    }
    Ok(out)
}

// --- KuCoin: reverse by `endAt` ---------------------------------------------

#[derive(Deserialize)]
struct KucoinResp {
    data: Vec<Vec<String>>,
}

async fn fetch_kucoin(client: &reqwest::Client, from: u64, to: u64) -> Result<Vec<Candle>> {
    // Each candle: [time(s), open, close, high, low, volume(base), turnover(quote)]
    let mut out = Vec::new();
    let mut end_s = to;
    loop {
        let start_window = end_s.saturating_sub(STEP * 1500).max(from);
        let url = format!(
            "https://api.kucoin.com/api/v1/market/candles?symbol=DOT-USDT&type=15min&startAt={start_window}&endAt={end_s}"
        );
        let resp: KucoinResp = with_retry(|| get_json(client, &url)).await?;
        if resp.data.is_empty() {
            break;
        }
        let mut oldest = end_s;
        for r in &resp.data {
            if r.len() < 7 {
                continue;
            }
            let t: u64 = r[0].parse().context("kucoin ts parse")?;
            if t < from || t >= to {
                continue;
            }
            let close = parse_price(&r[2])?;
            let qvol = parse_price(&r[6])?;
            oldest = oldest.min(t);
            out.push(Candle {
                ts: snap(t),
                close,
                quote_volume: qvol,
            });
        }
        if oldest <= from || start_window == from {
            break;
        }
        end_s = oldest.saturating_sub(1);
        tokio::time::sleep(Duration::from_millis(PER_CALL_DELAY_MS)).await;
    }
    Ok(out)
}

// --- Crypto.com: forward by `start_ts` --------------------------------------

#[derive(Deserialize)]
struct CryptoComResp {
    result: CryptoComResult,
}
#[derive(Deserialize)]
struct CryptoComResult {
    data: Vec<CryptoComCandle>,
}
#[derive(Deserialize)]
struct CryptoComCandle {
    t: u64,
    c: String,
    v: String,
}

async fn fetch_cryptocom(client: &reqwest::Client, from: u64, to: u64) -> Result<Vec<Candle>> {
    // Each candle has fields t(ms), o, h, l, c, v (base). Approximate
    // quote_volume ≈ c × v (mirrors the live VWAP path).
    let mut out = Vec::new();
    let mut start_ms = from * 1000;
    let end_ms = to * 1000;
    let chunk_ms = STEP * 1000 * 300;
    while start_ms < end_ms {
        let win_end = (start_ms + chunk_ms).min(end_ms);
        let url = format!(
            "https://api.crypto.com/exchange/v1/public/get-candlestick?instrument_name=DOT_USD&timeframe=15m&start_ts={start_ms}&end_ts={win_end}&count=300"
        );
        let resp: CryptoComResp = with_retry(|| get_json(client, &url)).await?;
        if resp.result.data.is_empty() {
            start_ms = win_end;
            tokio::time::sleep(Duration::from_millis(PER_CALL_DELAY_MS)).await;
            continue;
        }
        for c in &resp.result.data {
            if c.t < from * 1000 || c.t >= end_ms {
                continue;
            }
            let close = parse_price(&c.c)?;
            let base = parse_price(&c.v)?;
            out.push(Candle {
                ts: snap(c.t / 1000),
                close,
                quote_volume: close * base,
            });
        }
        start_ms = win_end;
        tokio::time::sleep(Duration::from_millis(PER_CALL_DELAY_MS)).await;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Alignment + output
// ---------------------------------------------------------------------------

/// `EXCHANGES[i].0` is the name used as the JSON key, matching `dot-price`'s
/// table exactly (so chart datasets line up byte-for-byte).
const EXCHANGES: &[&str] = &[
    "Binance",
    "OKX",
    "Coinbase",
    "Kraken",
    "Bybit",
    "KuCoin",
    "Crypto.com",
];

/// Result of fetching one exchange's full history.
type FetchResult = std::result::Result<Vec<Candle>, String>;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cfg = parse_args()?;
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(TIMEOUT)
        .build()
        .context("building HTTP client")?;

    let from = snap(cfg.from);
    let to = snap(cfg.to);

    if !cfg.quiet {
        eprintln!(
            "backfilling 15m candles from {from} to {to} ({} slots)",
            (to - from) / STEP
        );
        eprintln!("results -> {}", cfg.results_path);
        eprintln!("vwap    -> {}", cfg.vwap_path);
        eprintln!("errors  -> {}", cfg.errors_path);
    }

    // Run all seven fetchers concurrently. Map each Result to a stringified
    // error so a single failed venue doesn't sink the whole run.
    let (binance, okx, coinbase, kraken, bybit, kucoin, cryptocom) = tokio::join!(
        wrap(fetch_binance(&client, from, to), "Binance", cfg.quiet),
        wrap(fetch_okx(&client, from, to), "OKX", cfg.quiet),
        wrap(fetch_coinbase(&client, from, to), "Coinbase", cfg.quiet),
        wrap(fetch_kraken(&client, from, to), "Kraken", cfg.quiet),
        wrap(fetch_bybit(&client, from, to), "Bybit", cfg.quiet),
        wrap(fetch_kucoin(&client, from, to), "KuCoin", cfg.quiet),
        wrap(fetch_cryptocom(&client, from, to), "Crypto.com", cfg.quiet),
    );

    let fetched: Vec<(&'static str, FetchResult)> = vec![
        ("Binance", binance),
        ("OKX", okx),
        ("Coinbase", coinbase),
        ("Kraken", kraken),
        ("Bybit", bybit),
        ("KuCoin", kucoin),
        ("Crypto.com", cryptocom),
    ];

    // Index: per-exchange (ts → candle), so VWAP lookups can walk a 96-slot
    // window without scanning the whole vector.
    let mut by_exchange: HashMap<&'static str, BTreeMap<u64, Candle>> = HashMap::new();
    let mut fetch_errors: Vec<(&'static str, String)> = Vec::new();
    for (name, res) in &fetched {
        match res {
            Ok(candles) if !candles.is_empty() => {
                let map: BTreeMap<u64, Candle> = candles.iter().map(|c| (c.ts, *c)).collect();
                if !cfg.quiet {
                    eprintln!(
                        "{name}: {} candles ({}..{})",
                        map.len(),
                        map.keys().next().copied().unwrap_or(0),
                        map.keys().next_back().copied().unwrap_or(0)
                    );
                }
                by_exchange.insert(*name, map);
            }
            Ok(_) => fetch_errors.push((*name, "no candles returned".to_string())),
            Err(e) => fetch_errors.push((*name, e.clone())),
        }
    }

    let mut results = open_sink(Some(&cfg.results_path))?;
    let mut vwap_sink = open_sink(Some(&cfg.vwap_path))?;
    let mut errors = open_sink(Some(&cfg.errors_path))?;

    // Whole-fetch failures get a single row stamped at `from`, with each failing
    // exchange as a key. Matches write_named_errors in src/main.rs.
    if !fetch_errors.is_empty() {
        if let Some(file) = errors.as_mut() {
            let mut obj = serde_json::Map::new();
            obj.insert("ts".to_string(), from.into());
            for (name, msg) in &fetch_errors {
                obj.insert((*name).to_string(), serde_json::json!(msg));
            }
            write_line(file, &obj)?;
        }
    }

    // Walk the 15m grid, emitting one row per slot for both sinks. We hold the
    // last VWAP_WINDOW close/qvol pairs per exchange in a small ring so the
    // rolling 24h VWAP is computed in O(1) per slot.
    let mut ring: HashMap<&'static str, std::collections::VecDeque<(f64, f64)>> = HashMap::new();
    let mut ring_qvol_sum: HashMap<&'static str, f64> = HashMap::new();
    let mut ring_num_sum: HashMap<&'static str, f64> = HashMap::new();
    for ex in EXCHANGES {
        ring.insert(*ex, std::collections::VecDeque::with_capacity(VWAP_WINDOW));
        ring_qvol_sum.insert(*ex, 0.0);
        ring_num_sum.insert(*ex, 0.0);
    }

    let mut slots_written = 0u64;
    let mut ts = from;
    while ts < to {
        // For each exchange, push this slot's candle (if any) into the ring and
        // evict the oldest if we're at capacity.
        for ex in EXCHANGES {
            let map = match by_exchange.get(*ex) {
                Some(m) => m,
                None => continue,
            };
            let q = ring.get_mut(*ex).unwrap();
            let qs = ring_qvol_sum.get_mut(*ex).unwrap();
            let ns = ring_num_sum.get_mut(*ex).unwrap();
            if q.len() == VWAP_WINDOW {
                if let Some((close, qvol)) = q.pop_front() {
                    *qs -= qvol;
                    *ns -= close * qvol;
                }
            }
            if let Some(c) = map.get(&ts) {
                q.push_back((c.close, c.quote_volume));
                *qs += c.quote_volume;
                *ns += c.close * c.quote_volume;
            } else {
                // Push a zero-weight slot so the window still slides one step.
                q.push_back((f64::NAN, 0.0));
            }
        }

        // Emit the results row: every exchange that has a candle at this slot.
        let mut results_obj = serde_json::Map::new();
        results_obj.insert("ts".to_string(), ts.into());
        for ex in EXCHANGES {
            if let Some(c) = by_exchange.get(*ex).and_then(|m| m.get(&ts)) {
                results_obj.insert((*ex).to_string(), serde_json::json!(c.close));
            }
        }
        // Only write the row if at least one exchange has data for this slot.
        if results_obj.len() > 1 {
            if let Some(file) = results.as_mut() {
                write_line(file, &results_obj)?;
            }
            slots_written += 1;
        }

        // Emit the VWAP row when at least one exchange has a positive trailing
        // quote-volume sum — i.e. its window holds real data.
        let mut vwap_obj = serde_json::Map::new();
        vwap_obj.insert("ts".to_string(), ts.into());
        let mut any = false;
        for ex in EXCHANGES {
            let qs = *ring_qvol_sum.get(*ex).unwrap_or(&0.0);
            let ns = *ring_num_sum.get(*ex).unwrap_or(&0.0);
            if qs > 0.0 {
                vwap_obj.insert(
                    (*ex).to_string(),
                    serde_json::json!({ "vwap": ns / qs, "volume": qs }),
                );
                any = true;
            }
        }
        if any {
            if let Some(file) = vwap_sink.as_mut() {
                write_line(file, &vwap_obj)?;
            }
        }

        ts += STEP;
    }

    if !cfg.quiet {
        eprintln!("wrote {slots_written} aligned rows to {}", cfg.results_path);
    }
    Ok(())
}

/// Wrap a fetcher future so its `Err` is turned into a string and logged.
async fn wrap<F>(fut: F, name: &'static str, quiet: bool) -> FetchResult
where
    F: std::future::Future<Output = Result<Vec<Candle>>>,
{
    match fut.await {
        Ok(v) => Ok(v),
        Err(e) => {
            let msg = format!("{:#}", e);
            if !quiet {
                eprintln!("{name}: FAILED — {msg}");
            }
            Err(msg)
        }
    }
}
