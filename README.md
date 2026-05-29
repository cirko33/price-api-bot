# dot-price

Fetch the live [Polkadot (DOT)](https://polkadot.network) spot price from 8 centralized
exchanges concurrently, log each round as NDJSON, and render the result as an
interactive chart. No aggregation is performed — every exchange is listed side by side
so you can see the cross-exchange spread.

USDT and USD quotes are pooled together (USDT ≈ USD).

## Sources

| Exchange   | Pair       |
|------------|------------|
| Binance    | DOT/USDT   |
| OKX        | DOT/USDT   |
| Coinbase   | DOT/USD    |
| Kraken     | DOT/USD    |
| Bybit      | DOT/USDT   |
| KuCoin     | DOT/USDT   |
| Crypto.com | DOT/USD    |
| Gate.io    | DOT/USDT   |

## Build

```bash
cargo build --release
```

The binary is produced at `target/release/dot-price`.

## Usage

Run once and print a table to stdout:

```bash
cargo run --release
```

```
Exchange     Pair             Price (USD)  Reliability
--------------------------------------------------------
Binance      DOT/USDT              1.2590  Very High
OKX          DOT/USDT              1.2590  High
...
--------------------------------------------------------
8 sources ok  |  min 1.2570  max 1.2600  spread 23.9 bps
```

Poll continuously every 6 seconds:

```bash
cargo run --release -- --interval 6000
```

### Options

| Flag | Description |
|------|-------------|
| `-i`, `--interval <ms>` | Poll repeatedly every `<ms>` milliseconds. Omit to run once and exit. |
| `-o`, `--results <file>` (alias `--ndjson`) | Path for the prices log. Default: `results_<ts>.ndjson`. |
| `-e`, `--errors <file>` | Path for the errors log. Default: `error_<ts>.ndjson`. |
| `--no-log` | Disable all file logging (cannot be combined with `--results`/`--errors`). |
| `-q`, `--quiet` | Suppress the stdout table; file logs are still written. |
| `-h`, `--help` | Print usage and exit. |

When run once, the process exits non-zero if every source failed.

## Output format

Both logs are [NDJSON](https://ndjson.org) (one JSON object per line, flushed immediately).

`results.ndjson` — one line per round, with a price for each exchange that succeeded:

```json
{"ts":1779874404,"Binance":1.259,"OKX":1.259,"Coinbase":1.257,"Kraken":1.2573,"Bybit":1.259,"KuCoin":1.2595,"Crypto.com":1.2574,"Gate.io":1.26}
```

`errors.ndjson` — only written when a round has failures, keeping it signal-only:

```json
{"ts":1779890527,"OKX":"HTTP status client error (409 Conflict) for url (https://www.okx.com/api/v5/market/ticker?instId=DOT-USDT)"}
```

## Chart

`chart/generate.ts` reads `results.ndjson` (and `errors.ndjson` if present) and writes a
self-contained `chart/chart.html` with two interactive Chart.js line charts:

1. Raw price per exchange over time.
2. Per-exchange deviation from the cross-exchange mean (in basis points), which makes the
   arbitrage spread between exchanges visible.

Failed fetches are overlaid as triangle markers with the error message in the tooltip.
Charts support scroll-to-zoom, drag-to-pan, and double-click to reset.

```bash
node chart/generate.ts   # requires Node >= 22 (runs the TS directly)
open chart/chart.html
```

`sync-chart.sh` pulls the latest logs from a remote poller and regenerates the chart in
one step:

```bash
./sync-chart.sh
```

> Note: the `cargo-remote` host in `sync-chart.sh` is environment-specific; adjust the
> rsync targets to match your setup.

## Running as a service

`dot-price.service` is a systemd user unit that runs the poller continuously and restarts
on failure. Install it on the host doing the polling:

```bash
# build, then copy the binary + unit into place
cp target/release/dot-price ~/dot-price/
mkdir -p ~/.config/systemd/user
cp dot-price.service ~/.config/systemd/user/

systemctl --user daemon-reload
systemctl --user enable --now dot-price.service
```

It polls every 6 seconds in `--quiet` mode, logging to `~/dot-price/results.ndjson` and
`~/dot-price/errors.ndjson`.

## License

MIT
