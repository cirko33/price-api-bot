//! Shared helpers used by both the live poller (`dot-price`) and the historical
//! backfill (`dot-history`). Anything that touches the network, parses prices,
//! or writes NDJSON lives here so the two binaries stay byte-compatible.

use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};

pub const USER_AGENT: &str = "dot-price/0.1 (+https://github.com)";
pub const TIMEOUT: Duration = Duration::from_secs(8);

/// GET a URL and deserialize the JSON body into `T`.
pub async fn get_json<T: serde::de::DeserializeOwned>(
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
pub fn parse_price(s: &str) -> Result<f64> {
    s.trim()
        .parse::<f64>()
        .with_context(|| format!("could not parse number {s:?}"))
}

/// Compute VWAP as `quote / base`, guarding against zero base volume.
pub fn vwap_from(base: f64, quote: f64) -> Result<f64> {
    if base <= 0.0 {
        return Err(anyhow!("zero base volume; cannot compute VWAP"));
    }
    Ok(quote / base)
}

pub fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Open a log file in create+append mode, or `None` if no path is given.
pub fn open_sink(path: Option<&str>) -> Result<Option<std::fs::File>> {
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

/// Write one JSON object as a line, flushed immediately so a tailing reader
/// (or a kill mid-loop) never loses a complete record.
pub fn write_line(
    file: &mut std::fs::File,
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<()> {
    let line = serde_json::to_string(obj).context("serializing NDJSON line")?;
    writeln!(file, "{line}").context("writing NDJSON line")?;
    file.flush().context("flushing NDJSON file")?;
    Ok(())
}
