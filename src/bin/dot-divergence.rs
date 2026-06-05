//! Backfill 1-minute DOT spot closes for an arbitrary window across six
//! exchanges (the `dot-history` set minus Kraken, whose OHLC API only keeps
//! the most recent ~720 candles — 12 hours at 1m), then analyse the
//! cross-venue divergence per minute.
//!
//! Divergence at a slot is the max deviation from the cross-venue mean:
//! `d = max_i |price_i − mean| / mean × 100`. Each analysable slot is binned
//! into `<0.5%`, `0.5–1%`, `1–5%`, `≥5%`; the summary reports the share and
//! duration of each band, and consecutive slots in the same `≥0.5%` band are
//! merged into "episodes" written out for manual inspection.
//!
//! Raw aligned closes are written in the same NDJSON schema as
//! `results.ndjson`, so `chart/generate.ts` renders them unchanged and
//! `--input` can re-analyse a previous run without re-fetching.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, IsTerminal};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

use dot_price::{get_json, open_sink, parse_price, unix_secs, write_line, TIMEOUT, USER_AGENT};

/// 1m in seconds.
const STEP: u64 = 60;
/// Per-call pacing for each exchange's pagination loop.
const PER_CALL_DELAY_MS: u64 = 500;
/// Attempts before giving up on a single paginated request.
const RETRY_ATTEMPTS: u32 = 4;

/// Band boundaries (upper bounds, percent) and their labels. The last band is
/// open-ended.
const BANDS: &[(f64, &str)] = &[
    (0.5, "<0.5%"),
    (1.0, "0.5-1%"),
    (5.0, "1-5%"),
    (f64::INFINITY, ">=5%"),
];

/// One aligned candle from one exchange.
#[derive(Clone, Copy, Debug)]
struct Candle {
    ts: u64,
    close: f64,
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Config {
    from: Option<u64>,
    to: Option<u64>,
    results_path: String,
    occurrences_path: String,
    errors_path: String,
    input: Option<String>,
    quiet: bool,
}

fn parse_args() -> Result<Config> {
    let mut cfg = Config {
        from: None,
        to: None,
        results_path: "divergence_1m.ndjson".to_string(),
        occurrences_path: "divergence_occurrences.ndjson".to_string(),
        errors_path: "errors_divergence.ndjson".to_string(),
        input: None,
        quiet: false,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--from" => cfg.from = Some(take_u64(&mut args, "--from")?),
            other if other.starts_with("--from=") => {
                cfg.from = Some(other.trim_start_matches("--from=").parse()?)
            }
            "--to" => cfg.to = Some(take_u64(&mut args, "--to")?),
            other if other.starts_with("--to=") => {
                cfg.to = Some(other.trim_start_matches("--to=").parse()?)
            }
            "--results" | "-o" => cfg.results_path = take_str(&mut args, "--results")?,
            other if other.starts_with("--results=") => {
                cfg.results_path = other.trim_start_matches("--results=").to_string()
            }
            "--occurrences" => cfg.occurrences_path = take_str(&mut args, "--occurrences")?,
            other if other.starts_with("--occurrences=") => {
                cfg.occurrences_path = other.trim_start_matches("--occurrences=").to_string()
            }
            "--errors" | "-e" => cfg.errors_path = take_str(&mut args, "--errors")?,
            other if other.starts_with("--errors=") => {
                cfg.errors_path = other.trim_start_matches("--errors=").to_string()
            }
            "--input" | "-i" => cfg.input = Some(take_str(&mut args, "--input")?),
            other if other.starts_with("--input=") => {
                cfg.input = Some(other.trim_start_matches("--input=").to_string())
            }
            "--quiet" | "-q" => cfg.quiet = true,
            "-h" | "--help" => {
                println!(
                    "usage: dot-divergence [--from <unix>] [--to <unix>]\n\
                     \x20                    [--results <file>] [--occurrences <file>] [--errors <file>]\n\
                     \x20                    [--input <file>] [--quiet]\n\
                     \n\
                     Backfills 1m DOT spot closes across 6 exchanges (no Gate.io, no\n\
                     Kraken) for the window [from, to) (defaults: now-7d .. now), then\n\
                     reports how long the cross-venue divergence sat in each band\n\
                     (<0.5%, 0.5-1%, 1-5%, >=5%) and lists contiguous episodes >=0.5%.\n\
                     \n\
                     --input skips fetching and re-analyses an existing results NDJSON;\n\
                     the window then defaults to the file's own time span."
                );
                std::process::exit(0);
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }
    if let (Some(f), Some(t)) = (cfg.from, cfg.to) {
        if t <= f {
            return Err(anyhow!("--to must be greater than --from"));
        }
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
// Per-exchange fetchers (1m variants of src/bin/dot-history.rs)
// ---------------------------------------------------------------------------

/// Snap a unix timestamp to the start of its 1m grid slot.
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

// ---------------------------------------------------------------------------
// Download progress
// ---------------------------------------------------------------------------

/// Shared per-venue progress, updated by the fetcher after every page. Each
/// fetcher walks the window monotonically (forward or reverse), so "fraction
/// of the window the cursor has covered" is an honest progress measure.
#[derive(Clone)]
struct Prog(Arc<ProgInner>);

struct ProgInner {
    /// 0..=1000; the fetcher's covered fraction of [from, to).
    permille: AtomicU64,
    candles: AtomicU64,
    failed: AtomicBool,
}

impl Prog {
    fn new() -> Self {
        Prog(Arc::new(ProgInner {
            permille: AtomicU64::new(0),
            candles: AtomicU64::new(0),
            failed: AtomicBool::new(false),
        }))
    }
    /// Record covered fraction of the window (cursor-based, clamped).
    fn set_frac(&self, f: f64) {
        let pm = (f.clamp(0.0, 1.0) * 1000.0) as u64;
        self.0.permille.store(pm, Ordering::Relaxed);
    }
    fn add_candles(&self, n: u64) {
        self.0.candles.fetch_add(n, Ordering::Relaxed);
    }
    fn finish(&self) {
        self.0.permille.store(1000, Ordering::Relaxed);
    }
    fn fail(&self) {
        self.0.failed.store(true, Ordering::Relaxed);
    }
}

fn progress_bar(permille: u64) -> String {
    const W: usize = 30;
    let filled = (permille.min(1000) as usize * W) / 1000;
    format!("[{}{}]", "#".repeat(filled), "-".repeat(W - filled))
}

/// Redraw one line per venue in place (ANSI cursor-up), used on a TTY.
fn draw_progress_tty(rows: &[(&'static str, Prog)], first: &mut bool) {
    use std::io::Write;
    let mut out = String::new();
    if !*first {
        out.push_str(&format!("\x1b[{}A", rows.len()));
    }
    *first = false;
    for (name, p) in rows {
        let pm = p.0.permille.load(Ordering::Relaxed);
        let candles = p.0.candles.load(Ordering::Relaxed);
        let status = if p.0.failed.load(Ordering::Relaxed) {
            "FAILED".to_string()
        } else {
            format!("{:5.1}%", pm.min(1000) as f64 / 10.0)
        };
        out.push_str(&format!(
            "{name:<11} {} {status:>7}  {candles} candles\x1b[K\n",
            progress_bar(pm)
        ));
    }
    eprint!("{out}");
    let _ = std::io::stderr().flush();
}

/// One compact line per tick, used when stderr is piped to a log.
fn draw_progress_plain(rows: &[(&'static str, Prog)]) {
    let parts: Vec<String> = rows
        .iter()
        .map(|(name, p)| {
            if p.0.failed.load(Ordering::Relaxed) {
                format!("{name} FAILED")
            } else {
                format!(
                    "{name} {:.0}%",
                    p.0.permille.load(Ordering::Relaxed).min(1000) as f64 / 10.0
                )
            }
        })
        .collect();
    eprintln!("progress: {}", parts.join(" | "));
}

/// Periodically render progress until `stop` is set (then render once more).
async fn render_progress(rows: Vec<(&'static str, Prog)>, stop: Arc<AtomicBool>, tty: bool) {
    let tick = Duration::from_millis(if tty { 500 } else { 30_000 });
    let mut first = true;
    loop {
        let stopping = stop.load(Ordering::Relaxed);
        if tty {
            draw_progress_tty(&rows, &mut first);
        } else {
            draw_progress_plain(&rows);
        }
        if stopping {
            break;
        }
        tokio::time::sleep(tick).await;
    }
}

// --- Binance: forward-paginate by startTime ---------------------------------

async fn fetch_binance(
    client: &reqwest::Client,
    from: u64,
    to: u64,
    prog: &Prog,
) -> Result<Vec<Candle>> {
    // Kline rows: [openTime, open, high, low, close, volume, closeTime, ...]
    let mut out = Vec::new();
    let mut start_ms = from * 1000;
    let end_ms = to * 1000;
    loop {
        let url = format!(
            "https://data-api.binance.vision/api/v3/klines?symbol=DOTUSDT&interval=1m&startTime={start_ms}&endTime={end_ms}&limit=1000"
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
            newest_open_ms = newest_open_ms.max(open_ms);
            out.push(Candle {
                ts: snap(open_ms / 1000),
                close,
            });
        }
        prog.add_candles(rows.len() as u64);
        if rows.len() < 1000 {
            break;
        }
        start_ms = newest_open_ms + STEP * 1000;
        prog.set_frac((start_ms - from * 1000) as f64 / (end_ms - from * 1000) as f64);
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

async fn fetch_okx(
    client: &reqwest::Client,
    from: u64,
    to: u64,
    prog: &Prog,
) -> Result<Vec<Candle>> {
    // Each candle: [ts(ms), open, high, low, close, ...]. history-candles
    // returns up to 100 newest-first; pass `after` = older bound (exclusive)
    // to step further back in time.
    let mut out = Vec::new();
    let mut after_ms = to * 1000;
    let from_ms = from * 1000;
    loop {
        let url = format!(
            "https://www.okx.com/api/v5/market/history-candles?instId=DOT-USDT&bar=1m&after={after_ms}&limit=100"
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
            oldest_ms = oldest_ms.min(open_ms);
            out.push(Candle {
                ts: snap(open_ms / 1000),
                close,
            });
        }
        prog.add_candles(resp.data.len() as u64);
        // Stop once we've crossed the lower bound or the page wasn't full.
        if oldest_ms <= from_ms || resp.data.len() < 100 {
            break;
        }
        after_ms = oldest_ms;
        prog.set_frac((to * 1000 - after_ms) as f64 / (to * 1000 - from_ms) as f64);
        tokio::time::sleep(Duration::from_millis(PER_CALL_DELAY_MS)).await;
    }
    Ok(out)
}

// --- Coinbase: forward by `start`/`end` (epoch seconds accepted) ------------

async fn fetch_coinbase(
    client: &reqwest::Client,
    from: u64,
    to: u64,
    prog: &Prog,
) -> Result<Vec<Candle>> {
    // Each candle: [time, low, high, open, close, volume].
    let mut out = Vec::new();
    let mut start = from;
    let chunk = STEP * 300; // 300 candles per call max
    while start < to {
        let end = (start + chunk).min(to);
        let url = format!(
            "https://api.exchange.coinbase.com/products/DOT-USD/candles?start={start}&end={end}&granularity=60"
        );
        let rows: Vec<Vec<f64>> = with_retry(|| get_json(client, &url)).await?;
        prog.add_candles(rows.len() as u64);
        for r in &rows {
            if r.len() < 6 {
                continue;
            }
            let t = r[0] as u64;
            if t < from || t >= to {
                continue;
            }
            out.push(Candle {
                ts: snap(t),
                close: r[4],
            });
        }
        start = end;
        prog.set_frac((start - from) as f64 / (to - from) as f64);
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

async fn fetch_bybit(
    client: &reqwest::Client,
    from: u64,
    to: u64,
    prog: &Prog,
) -> Result<Vec<Candle>> {
    // Each candle (newest-first): [startTime(ms), open, high, low, close, ...]
    let mut out = Vec::new();
    let mut end_ms = to * 1000;
    let from_ms = from * 1000;
    loop {
        let url = format!(
            "https://api.bybit.com/v5/market/kline?category=spot&symbol=DOTUSDT&interval=1&end={end_ms}&limit=1000"
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
            oldest_ms = oldest_ms.min(open_ms);
            out.push(Candle {
                ts: snap(open_ms / 1000),
                close,
            });
        }
        prog.add_candles(resp.result.list.len() as u64);
        if oldest_ms <= from_ms || resp.result.list.len() < 1000 {
            break;
        }
        end_ms = oldest_ms - 1;
        prog.set_frac((to * 1000 - end_ms) as f64 / (to * 1000 - from_ms) as f64);
        tokio::time::sleep(Duration::from_millis(PER_CALL_DELAY_MS)).await;
    }
    Ok(out)
}

// --- KuCoin: reverse by `endAt` ---------------------------------------------

#[derive(Deserialize)]
struct KucoinResp {
    data: Vec<Vec<String>>,
}

async fn fetch_kucoin(
    client: &reqwest::Client,
    from: u64,
    to: u64,
    prog: &Prog,
) -> Result<Vec<Candle>> {
    // Each candle: [time(s), open, close, high, low, volume, turnover]
    let mut out = Vec::new();
    let mut end_s = to;
    loop {
        let start_window = end_s.saturating_sub(STEP * 1500).max(from);
        let url = format!(
            "https://api.kucoin.com/api/v1/market/candles?symbol=DOT-USDT&type=1min&startAt={start_window}&endAt={end_s}"
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
            oldest = oldest.min(t);
            out.push(Candle {
                ts: snap(t),
                close,
            });
        }
        prog.add_candles(resp.data.len() as u64);
        if oldest <= from || start_window == from {
            break;
        }
        end_s = oldest.saturating_sub(1);
        prog.set_frac((to - end_s) as f64 / (to - from) as f64);
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
}

async fn fetch_cryptocom(
    client: &reqwest::Client,
    from: u64,
    to: u64,
    prog: &Prog,
) -> Result<Vec<Candle>> {
    // Each candle has fields t(ms), o, h, l, c, v.
    let mut out = Vec::new();
    let mut start_ms = from * 1000;
    let end_ms = to * 1000;
    let chunk_ms = STEP * 1000 * 300;
    while start_ms < end_ms {
        let win_end = (start_ms + chunk_ms).min(end_ms);
        let url = format!(
            "https://api.crypto.com/exchange/v1/public/get-candlestick?instrument_name=DOT_USD&timeframe=1m&start_ts={start_ms}&end_ts={win_end}&count=300"
        );
        let resp: CryptoComResp = with_retry(|| get_json(client, &url)).await?;
        if resp.result.data.is_empty() {
            start_ms = win_end;
            prog.set_frac((start_ms - from * 1000) as f64 / (end_ms - from * 1000) as f64);
            tokio::time::sleep(Duration::from_millis(PER_CALL_DELAY_MS)).await;
            continue;
        }
        prog.add_candles(resp.result.data.len() as u64);
        for c in &resp.result.data {
            if c.t < from * 1000 || c.t >= end_ms {
                continue;
            }
            let close = parse_price(&c.c)?;
            out.push(Candle {
                ts: snap(c.t / 1000),
                close,
            });
        }
        start_ms = win_end;
        prog.set_frac((start_ms - from * 1000) as f64 / (end_ms - from * 1000) as f64);
        tokio::time::sleep(Duration::from_millis(PER_CALL_DELAY_MS)).await;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Alignment + analysis
// ---------------------------------------------------------------------------

/// Same JSON keys as `dot-price` / `dot-history`, minus Gate.io (never fetched
/// historically) and Kraken (no deep 1m history).
const EXCHANGES: &[&str] = &[
    "Binance",
    "OKX",
    "Coinbase",
    "Bybit",
    "KuCoin",
    "Crypto.com",
];

/// Result of fetching one exchange's full history.
type FetchResult = std::result::Result<Vec<Candle>, String>;

/// One contiguous run of 1m slots whose divergence sat in the same band.
struct Episode {
    band: usize,
    start_ts: u64,
    /// Exclusive.
    end_ts: u64,
    peak_pct: f64,
    peak_ts: u64,
    peak_venue: &'static str,
    peak_price: f64,
    mean_at_peak: f64,
    venues_at_peak: usize,
}

struct Analysis {
    total_slots: u64,
    no_data_slots: u64,
    band_slots: [u64; 4],
    coverage: Vec<(&'static str, u64)>,
    episodes: Vec<Episode>,
}

fn analyze(by_exchange: &HashMap<&'static str, BTreeMap<u64, f64>>, from: u64, to: u64) -> Analysis {
    let mut band_slots = [0u64; 4];
    let mut no_data_slots = 0u64;
    let mut coverage: HashMap<&'static str, u64> = HashMap::new();
    let mut episodes: Vec<Episode> = Vec::new();
    let mut current: Option<Episode> = None;

    let mut ts = from;
    while ts < to {
        let mut prices: Vec<(&'static str, f64)> = Vec::with_capacity(EXCHANGES.len());
        for ex in EXCHANGES {
            if let Some(p) = by_exchange.get(*ex).and_then(|m| m.get(&ts)) {
                prices.push((*ex, *p));
                *coverage.entry(*ex).or_insert(0) += 1;
            }
        }
        // Divergence needs at least two venues; a sparse slot also breaks any
        // open episode so runs stay genuinely contiguous.
        if prices.len() < 2 {
            no_data_slots += 1;
            if let Some(ep) = current.take() {
                episodes.push(ep);
            }
            ts += STEP;
            continue;
        }

        let mean = prices.iter().map(|(_, p)| p).sum::<f64>() / prices.len() as f64;
        let (venue, price) = prices
            .iter()
            .copied()
            .max_by(|a, b| {
                (a.1 - mean)
                    .abs()
                    .partial_cmp(&(b.1 - mean).abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap();
        let d_pct = (price - mean).abs() / mean * 100.0;
        let band = BANDS.iter().position(|(hi, _)| d_pct < *hi).unwrap_or(3);
        band_slots[band] += 1;

        if band == 0 {
            if let Some(ep) = current.take() {
                episodes.push(ep);
            }
        } else {
            match current {
                Some(ref mut ep) if ep.band == band => {
                    ep.end_ts = ts + STEP;
                    if d_pct > ep.peak_pct {
                        ep.peak_pct = d_pct;
                        ep.peak_ts = ts;
                        ep.peak_venue = venue;
                        ep.peak_price = price;
                        ep.mean_at_peak = mean;
                        ep.venues_at_peak = prices.len();
                    }
                }
                _ => {
                    if let Some(ep) = current.take() {
                        episodes.push(ep);
                    }
                    current = Some(Episode {
                        band,
                        start_ts: ts,
                        end_ts: ts + STEP,
                        peak_pct: d_pct,
                        peak_ts: ts,
                        peak_venue: venue,
                        peak_price: price,
                        mean_at_peak: mean,
                        venues_at_peak: prices.len(),
                    });
                }
            }
        }
        ts += STEP;
    }
    if let Some(ep) = current.take() {
        episodes.push(ep);
    }

    Analysis {
        total_slots: (to - from) / STEP,
        no_data_slots,
        band_slots,
        coverage: EXCHANGES
            .iter()
            .map(|ex| (*ex, coverage.get(ex).copied().unwrap_or(0)))
            .collect(),
        episodes,
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

/// Unix seconds → ISO-8601 UTC, via Howard Hinnant's civil_from_days.
fn iso8601(ts: u64) -> String {
    let z = (ts / 86_400) as i64 + 719_468;
    let secs = ts % 86_400;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe as i64 + era * 400 + if m <= 2 { 1 } else { 0 };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        secs / 3_600,
        (secs % 3_600) / 60,
        secs % 60
    )
}

/// Minutes → "Xd Yh Zm".
fn fmt_duration(minutes: u64) -> String {
    let d = minutes / 1_440;
    let h = (minutes % 1_440) / 60;
    let m = minutes % 60;
    match (d, h) {
        (0, 0) => format!("{m}m"),
        (0, _) => format!("{h}h {m}m"),
        _ => format!("{d}d {h}h {m}m"),
    }
}

// ---------------------------------------------------------------------------
// Input mode: reload a previous run's results NDJSON
// ---------------------------------------------------------------------------

fn load_input(path: &str) -> Result<HashMap<&'static str, BTreeMap<u64, f64>>> {
    let file = std::fs::File::open(path).with_context(|| format!("opening input {path:?}"))?;
    let mut by_exchange: HashMap<&'static str, BTreeMap<u64, f64>> = HashMap::new();
    for (i, line) in std::io::BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("reading {path}:{}", i + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&line)
            .with_context(|| format!("parsing {path}:{}", i + 1))?;
        let ts = obj
            .get("ts")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow!("{path}:{}: missing ts", i + 1))?;
        for ex in EXCHANGES {
            if let Some(p) = obj.get(*ex).and_then(|v| v.as_f64()) {
                by_exchange.entry(*ex).or_default().insert(snap(ts), p);
            }
        }
    }
    if by_exchange.is_empty() {
        return Err(anyhow!("{path}: no rows for any known exchange"));
    }
    Ok(by_exchange)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cfg = parse_args()?;

    let by_exchange;
    let (from, to);
    if let Some(input) = &cfg.input {
        by_exchange = load_input(input)?;
        // Window defaults to the file's own span when not given explicitly.
        let first = by_exchange.values().filter_map(|m| m.keys().next()).min();
        let last = by_exchange.values().filter_map(|m| m.keys().next_back()).max();
        from = snap(cfg.from.unwrap_or(*first.unwrap()));
        to = snap(cfg.to.unwrap_or(*last.unwrap() + STEP));
        if !cfg.quiet {
            eprintln!("analysing {input} ({} venues)", by_exchange.len());
        }
    } else {
        let now = unix_secs();
        from = snap(cfg.from.unwrap_or(now.saturating_sub(7 * 86_400)));
        to = snap(cfg.to.unwrap_or(now));
        by_exchange = fetch_all(&cfg, from, to).await?;
    }
    if to <= from {
        return Err(anyhow!("empty window: to ({to}) <= from ({from})"));
    }

    let analysis = analyze(&by_exchange, from, to);
    write_occurrences(&cfg.occurrences_path, &analysis.episodes)?;
    print_summary(&cfg, from, to, &analysis);
    Ok(())
}

/// Fetch all six venues concurrently and write the aligned 1m rows to the
/// results sink (same schema as `results.ndjson`).
async fn fetch_all(
    cfg: &Config,
    from: u64,
    to: u64,
) -> Result<HashMap<&'static str, BTreeMap<u64, f64>>> {
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(TIMEOUT)
        .build()
        .context("building HTTP client")?;

    if !cfg.quiet {
        eprintln!(
            "backfilling 1m candles from {from} to {to} ({} slots)",
            (to - from) / STEP
        );
        eprintln!("results     -> {}", cfg.results_path);
        eprintln!("occurrences -> {}", cfg.occurrences_path);
        eprintln!("errors      -> {}", cfg.errors_path);
    }

    // One progress handle per venue (indexes match EXCHANGES), plus a
    // background renderer: in-place bars on a TTY, a compact line every 30s
    // when stderr is piped to a log. Fetchers don't print mid-run, so the
    // redraw never gets interleaved.
    let progs: Vec<Prog> = EXCHANGES.iter().map(|_| Prog::new()).collect();
    let stop = Arc::new(AtomicBool::new(false));
    let renderer = if cfg.quiet {
        None
    } else {
        let rows: Vec<(&'static str, Prog)> = EXCHANGES
            .iter()
            .copied()
            .zip(progs.iter().cloned())
            .collect();
        let tty = std::io::stderr().is_terminal();
        Some(tokio::spawn(render_progress(rows, stop.clone(), tty)))
    };

    let (binance, okx, coinbase, bybit, kucoin, cryptocom) = tokio::join!(
        wrap(fetch_binance(&client, from, to, &progs[0]), &progs[0]),
        wrap(fetch_okx(&client, from, to, &progs[1]), &progs[1]),
        wrap(fetch_coinbase(&client, from, to, &progs[2]), &progs[2]),
        wrap(fetch_bybit(&client, from, to, &progs[3]), &progs[3]),
        wrap(fetch_kucoin(&client, from, to, &progs[4]), &progs[4]),
        wrap(fetch_cryptocom(&client, from, to, &progs[5]), &progs[5]),
    );

    stop.store(true, Ordering::Relaxed);
    if let Some(handle) = renderer {
        let _ = handle.await;
    }

    let fetched: Vec<(&'static str, FetchResult)> = vec![
        ("Binance", binance),
        ("OKX", okx),
        ("Coinbase", coinbase),
        ("Bybit", bybit),
        ("KuCoin", kucoin),
        ("Crypto.com", cryptocom),
    ];

    let mut by_exchange: HashMap<&'static str, BTreeMap<u64, f64>> = HashMap::new();
    let mut fetch_errors: Vec<(&'static str, String)> = Vec::new();
    for (name, res) in &fetched {
        match res {
            Ok(candles) if !candles.is_empty() => {
                let map: BTreeMap<u64, f64> = candles.iter().map(|c| (c.ts, c.close)).collect();
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
    // Failures are printed here, after the progress display has shut down.
    if !cfg.quiet {
        for (name, msg) in &fetch_errors {
            eprintln!("{name}: FAILED — {msg}");
        }
    }

    // Whole-fetch failures get a single row stamped at `from`, with each
    // failing exchange as a key. Matches dot-history.
    if !fetch_errors.is_empty() {
        if let Some(file) = open_sink(Some(&cfg.errors_path))?.as_mut() {
            let mut obj = serde_json::Map::new();
            obj.insert("ts".to_string(), from.into());
            for (name, msg) in &fetch_errors {
                obj.insert((*name).to_string(), serde_json::json!(msg));
            }
            write_line(file, &obj)?;
        }
    }

    // Persist the aligned grid so chart/generate.ts and --input can reuse it.
    let mut results = open_sink(Some(&cfg.results_path))?;
    let mut slots_written = 0u64;
    let mut ts = from;
    while ts < to {
        let mut obj = serde_json::Map::new();
        obj.insert("ts".to_string(), ts.into());
        for ex in EXCHANGES {
            if let Some(p) = by_exchange.get(*ex).and_then(|m| m.get(&ts)) {
                obj.insert((*ex).to_string(), serde_json::json!(p));
            }
        }
        if obj.len() > 1 {
            if let Some(file) = results.as_mut() {
                write_line(file, &obj)?;
            }
            slots_written += 1;
        }
        ts += STEP;
    }
    if !cfg.quiet {
        eprintln!("wrote {slots_written} aligned rows to {}", cfg.results_path);
    }
    Ok(by_exchange)
}

fn write_occurrences(path: &str, episodes: &[Episode]) -> Result<()> {
    let mut sink = open_sink(Some(path))?;
    if let Some(file) = sink.as_mut() {
        for ep in episodes {
            let mut obj = serde_json::Map::new();
            obj.insert("band".to_string(), serde_json::json!(BANDS[ep.band].1));
            obj.insert("start_ts".to_string(), ep.start_ts.into());
            obj.insert("end_ts".to_string(), ep.end_ts.into());
            obj.insert("start_utc".to_string(), serde_json::json!(iso8601(ep.start_ts)));
            obj.insert("end_utc".to_string(), serde_json::json!(iso8601(ep.end_ts)));
            obj.insert(
                "duration_min".to_string(),
                ((ep.end_ts - ep.start_ts) / STEP).into(),
            );
            obj.insert("peak_pct".to_string(), serde_json::json!(ep.peak_pct));
            obj.insert("peak_ts".to_string(), ep.peak_ts.into());
            obj.insert("peak_utc".to_string(), serde_json::json!(iso8601(ep.peak_ts)));
            obj.insert("peak_venue".to_string(), serde_json::json!(ep.peak_venue));
            obj.insert("peak_price".to_string(), serde_json::json!(ep.peak_price));
            obj.insert("mean_at_peak".to_string(), serde_json::json!(ep.mean_at_peak));
            obj.insert("venues_at_peak".to_string(), ep.venues_at_peak.into());
            write_line(file, &obj)?;
        }
    }
    Ok(())
}

fn print_summary(cfg: &Config, from: u64, to: u64, a: &Analysis) {
    let analyzable = a.band_slots.iter().sum::<u64>();
    println!();
    println!(
        "window     : {} .. {} ({} slots of 1m)",
        iso8601(from),
        iso8601(to),
        a.total_slots
    );
    println!(
        "analyzable : {analyzable} slots ({} with <2 venues, excluded from percentages)",
        a.no_data_slots
    );
    println!();
    println!("venue coverage:");
    for (ex, n) in &a.coverage {
        println!(
            "  {ex:<11} {:>6.2}%  ({n}/{} slots)",
            *n as f64 / a.total_slots.max(1) as f64 * 100.0,
            a.total_slots
        );
    }
    println!();
    println!("divergence d = max |price - mean| / mean across venues, per 1m slot:");
    for (i, (_, label)) in BANDS.iter().enumerate() {
        let n = a.band_slots[i];
        let pct = n as f64 / analyzable.max(1) as f64 * 100.0;
        let episodes = a.episodes.iter().filter(|e| e.band == i).count();
        let ep_note = if i == 0 {
            String::new()
        } else {
            format!("  ({episodes} episodes)")
        };
        println!(
            "  {label:<8} {n:>8} slots  {pct:>6.2}%  {:<12}{ep_note}",
            fmt_duration(n)
        );
    }
    println!();

    // Episode tables: every episode for the 1-5% and >=5% bands; the 0.5-1%
    // band can be huge, so print the top 50 by peak and point at the file.
    for band in (1..BANDS.len()).rev() {
        let mut eps: Vec<&Episode> = a.episodes.iter().filter(|e| e.band == band).collect();
        if eps.is_empty() {
            continue;
        }
        let label = BANDS[band].1;
        let cap = if band == 1 { 50 } else { usize::MAX };
        let total = eps.len();
        if total > cap {
            eps.sort_by(|a, b| b.peak_pct.partial_cmp(&a.peak_pct).unwrap());
            eps.truncate(cap);
            eps.sort_by_key(|e| e.start_ts);
            println!(
                "episodes {label} (top {cap} of {total} by peak; full list in {}):",
                cfg.occurrences_path
            );
        } else {
            println!("episodes {label}:");
        }
        println!(
            "  {:<20} {:>9} {:>7} {:<11} {:>9} {:>9} {:>7}",
            "start (UTC)", "duration", "peak%", "venue", "price", "mean", "venues"
        );
        for ep in eps {
            println!(
                "  {:<20} {:>9} {:>7.3} {:<11} {:>9.4} {:>9.4} {:>7}",
                iso8601(ep.start_ts),
                fmt_duration((ep.end_ts - ep.start_ts) / STEP),
                ep.peak_pct,
                ep.peak_venue,
                ep.peak_price,
                ep.mean_at_peak,
                ep.venues_at_peak
            );
        }
        println!();
    }
}

/// Wrap a fetcher future so its `Err` is turned into a string, and mark the
/// venue's progress bar finished or failed. Failures are printed after the
/// progress display stops, so nothing interleaves with the redraw.
async fn wrap<F>(fut: F, prog: &Prog) -> FetchResult
where
    F: std::future::Future<Output = Result<Vec<Candle>>>,
{
    match fut.await {
        Ok(v) => {
            prog.finish();
            Ok(v)
        }
        Err(e) => {
            prog.fail();
            Err(format!("{:#}", e))
        }
    }
}
