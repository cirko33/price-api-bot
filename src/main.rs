//! Fetch the live DOT spot price from 8 centralized exchanges concurrently and
//! list each side by side. No aggregation is performed; USDT and USD quotes are
//! pooled together (USDT ≈ USD).
//!
//! On a slower cadence, also pull each exchange's 24h base/quote volume to
//! compute a per-exchange 24h VWAP (`quote_volume / base_volume`). The chart
//! generator uses those volumes to draw a single volume-weighted "real price"
//! line across all venues.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

use dot_price::{get_json, open_sink, parse_price, unix_secs, vwap_from, write_line, TIMEOUT, USER_AGENT};

const VWAP_DEFAULT_INTERVAL_MS: u64 = 3_600_000;

/// A single exchange: display name, the pair we query, a reliability note (from
/// the source table), and the async fetcher that returns the last price in USD.
struct Source {
    name: &'static str,
    pair: &'static str,
    reliability: &'static str,
}

/// One exchange's 24h volume-weighted figures.
#[derive(Clone, Copy)]
struct VwapSample {
    vwap: f64,
    /// 24h quote volume, in USD/USDT. Used as the cross-exchange weight.
    quote_volume: f64,
}

// ---------------------------------------------------------------------------
// Per-exchange response structs + fetchers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct Binance {
    price: String,
}

async fn fetch_binance(client: &reqwest::Client) -> Result<f64> {
    let r: Binance = get_json(
        client,
        "https://data-api.binance.vision/api/v3/ticker/price?symbol=DOTUSDT",
    )
    .await?;
    parse_price(&r.price)
}

#[derive(Deserialize)]
struct Binance24h {
    #[serde(rename = "weightedAvgPrice")]
    weighted_avg_price: String,
    #[serde(rename = "quoteVolume")]
    quote_volume: String,
}

async fn fetch_binance_vwap(client: &reqwest::Client) -> Result<VwapSample> {
    let r: Binance24h = get_json(
        client,
        "https://data-api.binance.vision/api/v3/ticker/24hr?symbol=DOTUSDT",
    )
    .await?;
    Ok(VwapSample {
        vwap: parse_price(&r.weighted_avg_price)?,
        quote_volume: parse_price(&r.quote_volume)?,
    })
}

#[derive(Deserialize)]
struct Okx {
    data: Vec<OkxTicker>,
}
#[derive(Deserialize)]
struct OkxTicker {
    last: String,
    vol24h: String,
    #[serde(rename = "volCcy24h")]
    vol_ccy24h: String,
}

async fn fetch_okx(client: &reqwest::Client) -> Result<f64> {
    let r: Okx = get_json(
        client,
        "https://www.okx.com/api/v5/market/ticker?instId=DOT-USDT",
    )
    .await?;
    let last = r.data.first().ok_or_else(|| anyhow!("empty data array"))?;
    parse_price(&last.last)
}

async fn fetch_okx_vwap(client: &reqwest::Client) -> Result<VwapSample> {
    let r: Okx = get_json(
        client,
        "https://www.okx.com/api/v5/market/ticker?instId=DOT-USDT",
    )
    .await?;
    let t = r.data.first().ok_or_else(|| anyhow!("empty data array"))?;
    let base = parse_price(&t.vol24h)?;
    let quote = parse_price(&t.vol_ccy24h)?;
    Ok(VwapSample {
        vwap: vwap_from(base, quote)?,
        quote_volume: quote,
    })
}

#[derive(Deserialize)]
struct Coinbase {
    data: CoinbaseData,
}
#[derive(Deserialize)]
struct CoinbaseData {
    amount: String,
}

async fn fetch_coinbase(client: &reqwest::Client) -> Result<f64> {
    let r: Coinbase = get_json(
        client,
        "https://api.coinbase.com/v2/prices/DOT-USD/spot",
    )
    .await?;
    parse_price(&r.data.amount)
}

#[derive(Deserialize)]
struct CoinbaseStats {
    last: String,
    volume: String,
}

async fn fetch_coinbase_vwap(client: &reqwest::Client) -> Result<VwapSample> {
    // /stats returns base `volume` and `last` only. No quote_volume is exposed,
    // so we approximate quote_volume ≈ volume × last and report `vwap = last`.
    // Rough but cheap: the resulting weight is correct in order of magnitude,
    // and Coinbase's "VWAP" line equals its last price.
    let r: CoinbaseStats = get_json(
        client,
        "https://api.exchange.coinbase.com/products/DOT-USD/stats",
    )
    .await?;
    let last = parse_price(&r.last)?;
    let base = parse_price(&r.volume)?;
    Ok(VwapSample {
        vwap: last,
        quote_volume: last * base,
    })
}

#[derive(Deserialize)]
struct Kraken {
    result: HashMap<String, KrakenPair>,
}
#[derive(Deserialize)]
struct KrakenPair {
    /// Last trade closed: [price, lot volume].
    c: Vec<String>,
    /// Volume: [today, last 24 hours].
    v: Vec<String>,
    /// VWAP: [today, last 24 hours].
    p: Vec<String>,
}

async fn fetch_kraken(client: &reqwest::Client) -> Result<f64> {
    let r: Kraken = get_json(
        client,
        "https://api.kraken.com/0/public/Ticker?pair=DOTUSD",
    )
    .await?;
    // Kraken keys the result by its own asset code (e.g. "DOTUSD"); take the
    // single entry rather than hardcoding the key.
    let pair = r
        .result
        .values()
        .next()
        .ok_or_else(|| anyhow!("empty result map"))?;
    let price = pair.c.first().ok_or_else(|| anyhow!("missing close price"))?;
    parse_price(price)
}

async fn fetch_kraken_vwap(client: &reqwest::Client) -> Result<VwapSample> {
    let r: Kraken = get_json(
        client,
        "https://api.kraken.com/0/public/Ticker?pair=DOTUSD",
    )
    .await?;
    let pair = r
        .result
        .values()
        .next()
        .ok_or_else(|| anyhow!("empty result map"))?;
    let vwap_str = pair.p.get(1).ok_or_else(|| anyhow!("missing 24h VWAP"))?;
    let vol_str = pair.v.get(1).ok_or_else(|| anyhow!("missing 24h volume"))?;
    let vwap = parse_price(vwap_str)?;
    let base = parse_price(vol_str)?;
    Ok(VwapSample {
        vwap,
        quote_volume: vwap * base,
    })
}

#[derive(Deserialize)]
struct Bybit {
    result: BybitResult,
}
#[derive(Deserialize)]
struct BybitResult {
    list: Vec<BybitTicker>,
}
#[derive(Deserialize)]
struct BybitTicker {
    #[serde(rename = "lastPrice")]
    last_price: String,
    volume24h: String,
    turnover24h: String,
}

async fn fetch_bybit(client: &reqwest::Client) -> Result<f64> {
    let r: Bybit = get_json(
        client,
        "https://api.bybit.com/v5/market/tickers?category=spot&symbol=DOTUSDT",
    )
    .await?;
    let t = r
        .result
        .list
        .first()
        .ok_or_else(|| anyhow!("empty list"))?;
    parse_price(&t.last_price)
}

async fn fetch_bybit_vwap(client: &reqwest::Client) -> Result<VwapSample> {
    let r: Bybit = get_json(
        client,
        "https://api.bybit.com/v5/market/tickers?category=spot&symbol=DOTUSDT",
    )
    .await?;
    let t = r
        .result
        .list
        .first()
        .ok_or_else(|| anyhow!("empty list"))?;
    let base = parse_price(&t.volume24h)?;
    let quote = parse_price(&t.turnover24h)?;
    Ok(VwapSample {
        vwap: vwap_from(base, quote)?,
        quote_volume: quote,
    })
}

#[derive(Deserialize)]
struct Kucoin {
    data: KucoinData,
}
#[derive(Deserialize)]
struct KucoinData {
    price: String,
}

async fn fetch_kucoin(client: &reqwest::Client) -> Result<f64> {
    let r: Kucoin = get_json(
        client,
        "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=DOT-USDT",
    )
    .await?;
    parse_price(&r.data.price)
}

#[derive(Deserialize)]
struct KucoinStats {
    data: KucoinStatsData,
}
#[derive(Deserialize)]
struct KucoinStatsData {
    vol: String,
    #[serde(rename = "volValue")]
    vol_value: String,
}

async fn fetch_kucoin_vwap(client: &reqwest::Client) -> Result<VwapSample> {
    let r: KucoinStats = get_json(
        client,
        "https://api.kucoin.com/api/v1/market/stats?symbol=DOT-USDT",
    )
    .await?;
    let base = parse_price(&r.data.vol)?;
    let quote = parse_price(&r.data.vol_value)?;
    Ok(VwapSample {
        vwap: vwap_from(base, quote)?,
        quote_volume: quote,
    })
}

#[derive(Deserialize)]
struct CryptoCom {
    result: CryptoComResult,
}
#[derive(Deserialize)]
struct CryptoComResult {
    data: Vec<CryptoComTicker>,
}
#[derive(Deserialize)]
struct CryptoComTicker {
    /// `a` = latest trade price.
    a: String,
    /// `v` = 24h traded volume (base).
    v: String,
    /// `vv` = 24h traded value (quote).
    vv: String,
}

async fn fetch_cryptocom(client: &reqwest::Client) -> Result<f64> {
    let r: CryptoCom = get_json(
        client,
        "https://api.crypto.com/exchange/v1/public/get-tickers?instrument_name=DOT_USD",
    )
    .await?;
    let t = r
        .result
        .data
        .first()
        .ok_or_else(|| anyhow!("empty data array"))?;
    parse_price(&t.a)
}

async fn fetch_cryptocom_vwap(client: &reqwest::Client) -> Result<VwapSample> {
    let r: CryptoCom = get_json(
        client,
        "https://api.crypto.com/exchange/v1/public/get-tickers?instrument_name=DOT_USD",
    )
    .await?;
    let t = r
        .result
        .data
        .first()
        .ok_or_else(|| anyhow!("empty data array"))?;
    let base = parse_price(&t.v)?;
    let quote = parse_price(&t.vv)?;
    Ok(VwapSample {
        vwap: vwap_from(base, quote)?,
        quote_volume: quote,
    })
}

#[derive(Deserialize)]
struct GateTicker {
    last: String,
    base_volume: String,
    quote_volume: String,
}

async fn fetch_gate(client: &reqwest::Client) -> Result<f64> {
    let r: Vec<GateTicker> = get_json(
        client,
        "https://api.gateio.ws/api/v4/spot/tickers?currency_pair=DOT_USDT",
    )
    .await?;
    let t = r.first().ok_or_else(|| anyhow!("empty ticker array"))?;
    parse_price(&t.last)
}

async fn fetch_gate_vwap(client: &reqwest::Client) -> Result<VwapSample> {
    let r: Vec<GateTicker> = get_json(
        client,
        "https://api.gateio.ws/api/v4/spot/tickers?currency_pair=DOT_USDT",
    )
    .await?;
    let t = r.first().ok_or_else(|| anyhow!("empty ticker array"))?;
    let base = parse_price(&t.base_volume)?;
    let quote = parse_price(&t.quote_volume)?;
    Ok(VwapSample {
        vwap: vwap_from(base, quote)?,
        quote_volume: quote,
    })
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

/// Command-line configuration.
struct Config {
    /// Poll interval in milliseconds; `None` = run once and exit.
    interval_ms: Option<u64>,
    /// VWAP poll interval in milliseconds. Defaults to 1h. Only used in
    /// continuous mode; single-shot runs always do exactly one VWAP poll.
    vwap_interval_ms: u64,
    /// Explicit results-log path; `None` = use the timestamped default.
    results_path: Option<String>,
    /// Explicit error-log path; `None` = use the timestamped default.
    errors_path: Option<String>,
    /// Explicit VWAP-log path; `None` = use the timestamped default.
    vwap_path: Option<String>,
    /// Disable all file logging (results, errors, and VWAP).
    no_log: bool,
    /// Suppress the stdout tables (file logs still written).
    quiet: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = parse_args()?;

    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(TIMEOUT)
        .build()
        .context("building HTTP client")?;

    // Resolve log paths. With --no-log, nothing is written. Otherwise an explicit
    // path wins, else fall back to {results,error,vwap}_<ts>.ndjson where <ts>
    // is the program's start time (shared by all files for one run).
    let start = unix_secs();
    let (results_path, errors_path, vwap_path) = if cfg.no_log {
        (None, None, None)
    } else {
        (
            Some(cfg.results_path.unwrap_or_else(|| format!("results_{start}.ndjson"))),
            Some(cfg.errors_path.unwrap_or_else(|| format!("error_{start}.ndjson"))),
            Some(cfg.vwap_path.unwrap_or_else(|| format!("vwap_{start}.ndjson"))),
        )
    };

    // Open each sink once, in append mode, so repeated rounds keep adding.
    let mut results = open_sink(results_path.as_deref())?;
    let mut errors = open_sink(errors_path.as_deref())?;
    let mut vwap_sink = open_sink(vwap_path.as_deref())?;
    if let Some(p) = &results_path {
        eprintln!("logging prices  -> {p}");
    }
    if let Some(p) = &errors_path {
        eprintln!("logging errors  -> {p}");
    }
    if let Some(p) = &vwap_path {
        eprintln!("logging vwap    -> {p}");
    }

    match cfg.interval_ms {
        // Single shot: one spot poll AND one VWAP poll, then exit. Non-zero
        // exit only if every spot source failed.
        None => {
            let round = poll_once(&client, cfg.quiet).await;
            write_results(&mut results, &round)?;
            write_errors(&mut errors, &round)?;
            let vwap_round = poll_vwap_once(&client, cfg.quiet).await;
            write_vwap_results(&mut vwap_sink, &vwap_round)?;
            write_vwap_errors(&mut errors, &vwap_round)?;
            if round.ok_count == 0 {
                std::process::exit(1);
            }
        }
        // Continuous: two independent timers, driven by a single task via
        // `select!`. Each timer ticks at its own fixed rate; ticks don't drift
        // with fetch latency.
        Some(spot_ms) => {
            let mut spot_timer = tokio::time::interval(Duration::from_millis(spot_ms));
            let mut vwap_timer =
                tokio::time::interval(Duration::from_millis(cfg.vwap_interval_ms));
            loop {
                tokio::select! {
                    _ = spot_timer.tick() => {
                        if !cfg.quiet {
                            println!("\n# unix {} (spot)", unix_secs());
                        }
                        let round = poll_once(&client, cfg.quiet).await;
                        write_results(&mut results, &round)?;
                        write_errors(&mut errors, &round)?;
                    }
                    _ = vwap_timer.tick() => {
                        if !cfg.quiet {
                            println!("\n# unix {} (vwap)", unix_secs());
                        }
                        let vwap_round = poll_vwap_once(&client, cfg.quiet).await;
                        write_vwap_results(&mut vwap_sink, &vwap_round)?;
                        write_vwap_errors(&mut errors, &vwap_round)?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Parse CLI flags.
fn parse_args() -> Result<Config> {
    let mut cfg = Config {
        interval_ms: None,
        vwap_interval_ms: VWAP_DEFAULT_INTERVAL_MS,
        results_path: None,
        errors_path: None,
        vwap_path: None,
        no_log: false,
        quiet: false,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--interval" | "-i" => {
                let v = args
                    .next()
                    .ok_or_else(|| anyhow!("--interval requires a value in milliseconds"))?;
                cfg.interval_ms = Some(parse_interval(&v)?);
            }
            other if other.starts_with("--interval=") => {
                cfg.interval_ms = Some(parse_interval(other.trim_start_matches("--interval="))?);
            }
            "--vwap-interval" => {
                let v = args
                    .next()
                    .ok_or_else(|| anyhow!("--vwap-interval requires a value in milliseconds"))?;
                cfg.vwap_interval_ms = parse_interval(&v)?;
            }
            other if other.starts_with("--vwap-interval=") => {
                cfg.vwap_interval_ms =
                    parse_interval(other.trim_start_matches("--vwap-interval="))?;
            }
            "--ndjson" | "--results" | "-o" => {
                cfg.results_path = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("--results requires a file path"))?,
                );
            }
            other if other.starts_with("--ndjson=") => {
                cfg.results_path = Some(other.trim_start_matches("--ndjson=").to_string());
            }
            other if other.starts_with("--results=") => {
                cfg.results_path = Some(other.trim_start_matches("--results=").to_string());
            }
            "--errors" | "-e" => {
                cfg.errors_path = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("--errors requires a file path"))?,
                );
            }
            other if other.starts_with("--errors=") => {
                cfg.errors_path = Some(other.trim_start_matches("--errors=").to_string());
            }
            "--vwap-results" => {
                cfg.vwap_path = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("--vwap-results requires a file path"))?,
                );
            }
            other if other.starts_with("--vwap-results=") => {
                cfg.vwap_path = Some(other.trim_start_matches("--vwap-results=").to_string());
            }
            "--no-log" => cfg.no_log = true,
            "--quiet" | "-q" => cfg.quiet = true,
            "-h" | "--help" => {
                println!(
                    "usage: dot-price [--interval <ms>] [--vwap-interval <ms>]\n\
                     \x20                [--results <file>] [--errors <file>] [--vwap-results <file>]\n\
                     \x20                [--no-log] [--quiet]\n\
                     \n\
                     Logs prices, errors, and VWAP samples as NDJSON. Defaults: results_<ts>.ndjson,\n\
                     error_<ts>.ndjson, vwap_<ts>.ndjson in the current directory. --no-log disables\n\
                     all file logging. --vwap-interval defaults to 3600000 (1 hour)."
                );
                std::process::exit(0);
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }
    if cfg.no_log
        && (cfg.results_path.is_some()
            || cfg.errors_path.is_some()
            || cfg.vwap_path.is_some())
    {
        return Err(anyhow!(
            "--no-log cannot be combined with --results/--errors/--vwap-results"
        ));
    }
    Ok(cfg)
}

fn parse_interval(v: &str) -> Result<u64> {
    let ms: u64 = v
        .parse()
        .with_context(|| format!("invalid interval {v:?}; expected milliseconds"))?;
    if ms == 0 {
        return Err(anyhow!("interval must be greater than 0"));
    }
    Ok(ms)
}

/// Result of one spot-price poll round.
struct Round {
    ts: u64,
    ok_count: usize,
    /// `(exchange name, price)` for sources that succeeded.
    prices: Vec<(&'static str, f64)>,
    /// `(exchange name, error message)` for sources that failed.
    errors: Vec<(&'static str, String)>,
}

/// Result of one VWAP poll round.
struct VwapRound {
    ts: u64,
    /// `(exchange name, VwapSample)` for sources that succeeded.
    samples: Vec<(&'static str, VwapSample)>,
    /// `(exchange name, error message)` for sources that failed.
    errors: Vec<(&'static str, String)>,
}

/// Append the prices as one JSON line: `{"ts": <unix>, "<name>": <price>, ...}`.
fn write_results(sink: &mut Option<std::fs::File>, round: &Round) -> Result<()> {
    if let Some(file) = sink {
        let mut obj = serde_json::Map::new();
        obj.insert("ts".to_string(), round.ts.into());
        for (name, price) in &round.prices {
            obj.insert((*name).to_string(), serde_json::json!(price));
        }
        write_line(file, &obj)?;
    }
    Ok(())
}

/// Append the failures as one JSON line: `{"ts": <unix>, "<name>": "<error>", ...}`.
/// Rounds with no failures write nothing, keeping the error log signal-only.
fn write_errors(sink: &mut Option<std::fs::File>, round: &Round) -> Result<()> {
    write_named_errors(sink, round.ts, &round.errors)
}

/// Append the VWAP samples as one JSON line:
/// `{"ts": <unix>, "<name>": {"vwap": <f>, "volume": <f>}, ...}`.
fn write_vwap_results(sink: &mut Option<std::fs::File>, round: &VwapRound) -> Result<()> {
    if let Some(file) = sink {
        let mut obj = serde_json::Map::new();
        obj.insert("ts".to_string(), round.ts.into());
        for (name, s) in &round.samples {
            obj.insert(
                (*name).to_string(),
                serde_json::json!({ "vwap": s.vwap, "volume": s.quote_volume }),
            );
        }
        write_line(file, &obj)?;
    }
    Ok(())
}

/// Append VWAP-poll failures to the shared error log. Prefixes each exchange
/// name with `"vwap:"` so the two failure kinds remain distinguishable in one
/// file.
fn write_vwap_errors(sink: &mut Option<std::fs::File>, round: &VwapRound) -> Result<()> {
    if round.errors.is_empty() {
        return Ok(());
    }
    let named: Vec<(String, String)> = round
        .errors
        .iter()
        .map(|(n, m)| (format!("vwap:{n}"), m.clone()))
        .collect();
    if let Some(file) = sink {
        let mut obj = serde_json::Map::new();
        obj.insert("ts".to_string(), round.ts.into());
        for (name, msg) in &named {
            obj.insert(name.clone(), serde_json::json!(msg));
        }
        write_line(file, &obj)?;
    }
    Ok(())
}

fn write_named_errors(
    sink: &mut Option<std::fs::File>,
    ts: u64,
    errors: &[(&'static str, String)],
) -> Result<()> {
    if let Some(file) = sink {
        if errors.is_empty() {
            return Ok(());
        }
        let mut obj = serde_json::Map::new();
        obj.insert("ts".to_string(), ts.into());
        for (name, msg) in errors {
            obj.insert((*name).to_string(), serde_json::json!(msg));
        }
        write_line(file, &obj)?;
    }
    Ok(())
}

/// Fetch all sources once, return the round data, and (unless `quiet`) print
/// the table + footer to stdout.
async fn poll_once(client: &reqwest::Client, quiet: bool) -> Round {
    // (metadata, fetcher) pairs. Reliability notes come from the source table.
    let sources: Vec<(Source, futures::future::BoxFuture<'_, Result<f64>>)> = vec![
        (
            Source { name: "Binance", pair: "DOT/USDT", reliability: "Very High" },
            Box::pin(fetch_binance(client)),
        ),
        (
            Source { name: "OKX", pair: "DOT/USDT", reliability: "High" },
            Box::pin(fetch_okx(client)),
        ),
        (
            Source { name: "Coinbase", pair: "DOT/USD", reliability: "Very High" },
            Box::pin(fetch_coinbase(client)),
        ),
        (
            Source { name: "Kraken", pair: "DOT/USD", reliability: "Very High" },
            Box::pin(fetch_kraken(client)),
        ),
        (
            Source { name: "Bybit", pair: "DOT/USDT", reliability: "High" },
            Box::pin(fetch_bybit(client)),
        ),
        (
            Source { name: "KuCoin", pair: "DOT/USDT", reliability: "Medium-High" },
            Box::pin(fetch_kucoin(client)),
        ),
        (
            Source { name: "Crypto.com", pair: "DOT/USD", reliability: "High" },
            Box::pin(fetch_cryptocom(client)),
        ),
        (
            Source { name: "Gate.io", pair: "DOT/USDT", reliability: "Medium-High" },
            Box::pin(fetch_gate(client)),
        ),
    ];

    let (metas, futs): (Vec<Source>, Vec<_>) = sources.into_iter().unzip();
    let results = futures::future::join_all(futs).await;

    // Header.
    if !quiet {
        println!(
            "{:<12} {:<10} {:>13}  {}",
            "Exchange", "Pair", "Price (USD)", "Reliability"
        );
        println!("{}", "-".repeat(56));
    }

    let mut out_prices = Vec::new();
    let mut out_errors = Vec::new();
    let mut prices = Vec::new();
    for (meta, res) in metas.iter().zip(results.into_iter()) {
        match res {
            Ok(price) => {
                prices.push(price);
                out_prices.push((meta.name, price));
                if !quiet {
                    println!(
                        "{:<12} {:<10} {:>13.4}  {}",
                        meta.name, meta.pair, price, meta.reliability
                    );
                }
            }
            Err(e) => {
                let cause = e.root_cause().to_string();
                if !quiet {
                    // Render the failing source as a dash row, root cause inline.
                    println!(
                        "{:<12} {:<10} {:>13}  {} (error: {})",
                        meta.name, meta.pair, "—", meta.reliability, cause
                    );
                }
                out_errors.push((meta.name, cause));
            }
        }
    }

    // Footer: min/max/spread across the sources that succeeded (still a listing,
    // not a single aggregated price).
    let min = prices.iter().copied().min_by(f64::total_cmp);
    let max = prices.iter().copied().max_by(f64::total_cmp);
    let spread_bps = match (min, max) {
        (Some(lo), Some(hi)) if lo > 0.0 => Some((hi - lo) / lo * 10_000.0),
        _ => None,
    };
    if let (Some(lo), Some(hi)) = (min, max) {
        if !quiet {
            println!("{}", "-".repeat(56));
            println!(
                "{} sources ok  |  min {:.4}  max {:.4}  spread {:.1} bps",
                prices.len(),
                lo,
                hi,
                spread_bps.unwrap_or(0.0)
            );
        }
    } else {
        eprintln!("All sources failed.");
    }

    Round {
        ts: unix_secs(),
        ok_count: prices.len(),
        prices: out_prices,
        errors: out_errors,
    }
}

/// Fetch 24h VWAP + quote volume from each source once, in parallel. Prints a
/// compact table when not quiet.
async fn poll_vwap_once(client: &reqwest::Client, quiet: bool) -> VwapRound {
    let sources: Vec<(&'static str, futures::future::BoxFuture<'_, Result<VwapSample>>)> = vec![
        ("Binance", Box::pin(fetch_binance_vwap(client))),
        ("OKX", Box::pin(fetch_okx_vwap(client))),
        ("Coinbase", Box::pin(fetch_coinbase_vwap(client))),
        ("Kraken", Box::pin(fetch_kraken_vwap(client))),
        ("Bybit", Box::pin(fetch_bybit_vwap(client))),
        ("KuCoin", Box::pin(fetch_kucoin_vwap(client))),
        ("Crypto.com", Box::pin(fetch_cryptocom_vwap(client))),
        ("Gate.io", Box::pin(fetch_gate_vwap(client))),
    ];

    let (names, futs): (Vec<&'static str>, Vec<_>) = sources.into_iter().unzip();
    let results = futures::future::join_all(futs).await;

    if !quiet {
        println!(
            "{:<12} {:>10}  {:>18}",
            "Exchange", "24h VWAP", "24h quote vol"
        );
        println!("{}", "-".repeat(44));
    }

    let mut samples = Vec::new();
    let mut errors = Vec::new();
    for (name, res) in names.iter().zip(results.into_iter()) {
        match res {
            Ok(s) => {
                if !quiet {
                    println!(
                        "{:<12} {:>10.4}  {:>18.0}",
                        name, s.vwap, s.quote_volume
                    );
                }
                samples.push((*name, s));
            }
            Err(e) => {
                let cause = e.root_cause().to_string();
                if !quiet {
                    println!("{:<12} {:>10}  {:>18}  (error: {})", name, "—", "—", cause);
                }
                errors.push((*name, cause));
            }
        }
    }

    if !quiet {
        let total: f64 = samples.iter().map(|(_, s)| s.quote_volume).sum();
        println!("{}", "-".repeat(44));
        println!(
            "{} sources ok  |  total 24h quote vol {:.0}",
            samples.len(),
            total
        );
    }

    VwapRound {
        ts: unix_secs(),
        samples,
        errors,
    }
}
