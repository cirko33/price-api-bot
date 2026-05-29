//! Fetch the live DOT spot price from 8 centralized exchanges concurrently and
//! list each side by side. No aggregation is performed; USDT and USD quotes are
//! pooled together (USDT ≈ USD).

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

const USER_AGENT: &str = "dot-price/0.1 (+https://github.com)";
const TIMEOUT: Duration = Duration::from_secs(8);

/// A single exchange: display name, the pair we query, a reliability note (from
/// the source table), and the async fetcher that returns the last price in USD.
struct Source {
    name: &'static str,
    pair: &'static str,
    reliability: &'static str,
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
struct Okx {
    data: Vec<OkxTicker>,
}
#[derive(Deserialize)]
struct OkxTicker {
    last: String,
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
struct Kraken {
    result: HashMap<String, KrakenPair>,
}
#[derive(Deserialize)]
struct KrakenPair {
    /// Last trade closed: [price, lot volume].
    c: Vec<String>,
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

#[derive(Deserialize)]
struct GateTicker {
    last: String,
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// GET a URL and deserialize the JSON body into `T`.
async fn get_json<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
) -> Result<T> {
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("request to {url} failed"))?
        .error_for_status()
        .with_context(|| format!("{url} returned an error status"))?;
    resp.json::<T>()
        .await
        .with_context(|| format!("decoding JSON from {url} failed"))
}

/// Parse an exchange's price string into an `f64`.
fn parse_price(s: &str) -> Result<f64> {
    s.trim()
        .parse::<f64>()
        .with_context(|| format!("could not parse price {s:?}"))
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

/// Command-line configuration.
struct Config {
    /// Poll interval in milliseconds; `None` = run once and exit.
    interval_ms: Option<u64>,
    /// Explicit results-log path; `None` = use the timestamped default.
    results_path: Option<String>,
    /// Explicit error-log path; `None` = use the timestamped default.
    errors_path: Option<String>,
    /// Disable all file logging (results and errors).
    no_log: bool,
    /// Suppress the stdout price table (file logs still written).
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
    // path wins, else fall back to results_<ts>.ndjson / error_<ts>.ndjson where
    // <ts> is the program's start time (shared by both files for one run).
    let start = unix_secs();
    let (results_path, errors_path) = if cfg.no_log {
        (None, None)
    } else {
        (
            Some(cfg.results_path.unwrap_or_else(|| format!("results_{start}.ndjson"))),
            Some(cfg.errors_path.unwrap_or_else(|| format!("error_{start}.ndjson"))),
        )
    };

    // Open each sink once, in append mode, so repeated rounds keep adding.
    let mut results = open_sink(results_path.as_deref())?;
    let mut errors = open_sink(errors_path.as_deref())?;
    if let Some(p) = &results_path {
        eprintln!("logging prices  -> {p}");
    }
    if let Some(p) = &errors_path {
        eprintln!("logging errors  -> {p}");
    }

    match cfg.interval_ms {
        // Single shot: exit non-zero if every source failed.
        None => {
            let round = poll_once(&client, cfg.quiet).await;
            write_results(&mut results, &round)?;
            write_errors(&mut errors, &round)?;
            if round.ok_count == 0 {
                std::process::exit(1);
            }
        }
        // Repeat forever on a fixed-rate timer (ticks don't drift with fetch latency).
        Some(ms) => {
            let mut timer = tokio::time::interval(Duration::from_millis(ms));
            loop {
                timer.tick().await;
                if !cfg.quiet {
                    println!("\n# unix {}", unix_secs());
                }
                let round = poll_once(&client, cfg.quiet).await;
                write_results(&mut results, &round)?;
                write_errors(&mut errors, &round)?;
            }
        }
    }

    Ok(())
}

/// Open a log file in create+append mode, or `None` if no path is given.
fn open_sink(path: Option<&str>) -> Result<Option<std::fs::File>> {
    match path {
        Some(p) => Ok(Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .with_context(|| format!("opening log file {p:?}"))?,
        )),
        None => Ok(None),
    }
}

/// Parse CLI flags.
fn parse_args() -> Result<Config> {
    let mut cfg = Config {
        interval_ms: None,
        results_path: None,
        errors_path: None,
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
            "--no-log" => cfg.no_log = true,
            "--quiet" | "-q" => cfg.quiet = true,
            "-h" | "--help" => {
                println!(
                    "usage: dot-price [--interval <ms>] [--results <file>] [--errors <file>] [--no-log] [--quiet]\n\
                     \n\
                     Logs prices and errors as NDJSON. Defaults: results_<ts>.ndjson and\n\
                     error_<ts>.ndjson in the current directory. --no-log disables file logging."
                );
                std::process::exit(0);
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }
    if cfg.no_log && (cfg.results_path.is_some() || cfg.errors_path.is_some()) {
        return Err(anyhow!("--no-log cannot be combined with --results/--errors"));
    }
    Ok(cfg)
}

fn parse_interval(v: &str) -> Result<u64> {
    let ms: u64 = v
        .parse()
        .with_context(|| format!("invalid interval {v:?}; expected milliseconds"))?;
    if ms == 0 {
        return Err(anyhow!("--interval must be greater than 0"));
    }
    Ok(ms)
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Result of one poll round.
struct Round {
    ts: u64,
    ok_count: usize,
    /// `(exchange name, price)` for sources that succeeded.
    prices: Vec<(&'static str, f64)>,
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
    if let Some(file) = sink {
        if round.errors.is_empty() {
            return Ok(());
        }
        let mut obj = serde_json::Map::new();
        obj.insert("ts".to_string(), round.ts.into());
        for (name, msg) in &round.errors {
            obj.insert((*name).to_string(), serde_json::json!(msg));
        }
        write_line(file, &obj)?;
    }
    Ok(())
}

/// Write one JSON object as a line, flushed immediately so a tailing reader
/// (or a kill mid-loop) never loses a complete record.
fn write_line(file: &mut std::fs::File, obj: &serde_json::Map<String, serde_json::Value>) -> Result<()> {
    let line = serde_json::to_string(obj).context("serializing NDJSON line")?;
    writeln!(file, "{line}").context("writing NDJSON line")?;
    file.flush().context("flushing NDJSON file")?;
    Ok(())
}

/// Fetch all sources once, return the round data, and (unless `quiet`) print
/// the table + footer to stdout.
async fn poll_once(client: &reqwest::Client, quiet: bool) -> Round {
    // (metadata, fetcher) pairs. Reliability notes come from the source table.
    let sources: Vec<(Source, futures::future::BoxFuture<'_, Result<f64>>)> = vec![
        (
            Source { name: "Binance", pair: "DOT/USDT", reliability: "Very High" },
            Box::pin(fetch_binance(&client)),
        ),
        (
            Source { name: "OKX", pair: "DOT/USDT", reliability: "High" },
            Box::pin(fetch_okx(&client)),
        ),
        (
            Source { name: "Coinbase", pair: "DOT/USD", reliability: "Very High" },
            Box::pin(fetch_coinbase(&client)),
        ),
        (
            Source { name: "Kraken", pair: "DOT/USD", reliability: "Very High" },
            Box::pin(fetch_kraken(&client)),
        ),
        (
            Source { name: "Bybit", pair: "DOT/USDT", reliability: "High" },
            Box::pin(fetch_bybit(&client)),
        ),
        (
            Source { name: "KuCoin", pair: "DOT/USDT", reliability: "Medium-High" },
            Box::pin(fetch_kucoin(&client)),
        ),
        (
            Source { name: "Crypto.com", pair: "DOT/USD", reliability: "High" },
            Box::pin(fetch_cryptocom(&client)),
        ),
        (
            Source { name: "Gate.io", pair: "DOT/USDT", reliability: "Medium-High" },
            Box::pin(fetch_gate(&client)),
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
