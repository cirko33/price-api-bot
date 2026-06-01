/**
 * Reads ../results.ndjson (spot prices per exchange) and ../vwap.ndjson (24h
 * VWAP + quote volume per exchange) and writes a single self-contained
 * diff.html chart of:
 *
 *     simple_mean(spot_i)  −  Σ(spot_i × vol_i) / Σ(vol_i)
 *
 * Plotted in both USD and basis points of the VWAP-weighted price. A positive
 * value means the unweighted mean sits above the liquidity-weighted price.
 *
 * Usage:  node chart/diff.ts
 * Output: chart/diff.html
 */
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const resultsPath = join(here, "..", "results.ndjson");
const vwapPath = join(here, "..", "vwap.ndjson");
const outputPath = join(here, "diff.html");

type Row = { ts: number } & Record<string, number>;
type VwapSample = { vwap: number; volume: number };
type VwapRow = { ts: number } & Record<string, VwapSample>;

const rows: Row[] = readFileSync(resultsPath, "utf8")
  .split("\n")
  .filter((l) => l.trim().length > 0)
  .map((l) => JSON.parse(l) as Row);
if (rows.length === 0) throw new Error("no data in results.ndjson");

if (!existsSync(vwapPath)) throw new Error("vwap.ndjson missing");
const vwapRows: VwapRow[] = readFileSync(vwapPath, "utf8")
  .split("\n")
  .filter((l) => l.trim().length > 0)
  .map((l) => JSON.parse(l) as VwapRow);
if (vwapRows.length === 0) throw new Error("no data in vwap.ndjson");

const exchanges = Object.keys(rows[0]).filter((k) => k !== "ts");

// Latest vwap row at or before ts; -1 if none.
const vwapTs = vwapRows.map((r) => r.ts);
const latestVwapIdx = (ts: number) => {
  if (vwapTs[0] > ts) return -1;
  let lo = 0, hi = vwapTs.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (vwapTs[mid] <= ts) lo = mid;
    else hi = mid - 1;
  }
  return lo;
};

const labels: string[] = [];
const diffUsd: (number | null)[] = [];
const diffBps: (number | null)[] = [];

let sumAbsBps = 0, countBps = 0, maxAbsBps = 0;

for (const r of rows) {
  labels.push(new Date(r.ts * 1000).toLocaleTimeString("en-GB", { hour12: false, timeZone: "UTC" }));

  const spots = exchanges
    .map((e) => r[e])
    .filter((v): v is number => typeof v === "number");
  const mean = spots.length > 0 ? spots.reduce((s, v) => s + v, 0) / spots.length : null;

  const vi = latestVwapIdx(r.ts);
  if (mean === null || vi < 0) { diffUsd.push(null); diffBps.push(null); continue; }
  const vr = vwapRows[vi];
  let num = 0, den = 0;
  for (const e of exchanges) {
    const s = vr[e];
    if (typeof r[e] !== "number") continue;
    if (!s || typeof s.volume !== "number" || s.volume <= 0) continue;
    num += r[e] * s.volume;
    den += s.volume;
  }
  if (den === 0) { diffUsd.push(null); diffBps.push(null); continue; }
  const vwapPrice = num / den;
  const d = mean - vwapPrice;
  diffUsd.push(d);
  const bps = (d / vwapPrice) * 10_000;
  diffBps.push(bps);
  sumAbsBps += Math.abs(bps);
  if (Math.abs(bps) > maxAbsBps) maxAbsBps = Math.abs(bps);
  countBps++;
}

const meanAbsBps = countBps > 0 ? sumAbsBps / countBps : 0;
const fmt = (n: number) => n.toFixed(3);

const usdDataset = {
  label: "mean − vwap (USD)",
  data: diffUsd,
  borderColor: "#4363d8",
  backgroundColor: "#4363d8",
  borderWidth: 1.5,
  pointRadius: 0,
  tension: 0.1,
  yAxisID: "yUsd",
  spanGaps: false,
};
const bpsDataset = {
  label: "mean − vwap (bps)",
  data: diffBps,
  borderColor: "#e6194b",
  backgroundColor: "#e6194b",
  borderWidth: 1.5,
  pointRadius: 0,
  tension: 0.1,
  yAxisID: "yBps",
  spanGaps: false,
};

const dateLabel = new Date(rows[0].ts * 1000).toLocaleDateString("en-CA", { timeZone: "UTC" });
const dateLabelEnd = new Date(rows[rows.length - 1].ts * 1000).toLocaleDateString("en-CA", { timeZone: "UTC" });
const range = dateLabel === dateLabelEnd ? dateLabel : `${dateLabel} → ${dateLabelEnd}`;

const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Simple mean − VWAP-weighted price</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script src="https://cdn.jsdelivr.net/npm/hammerjs@2"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2"></script>
<style>
  body { font-family: system-ui, sans-serif; margin: 2rem; background: #fafafa; color: #222; }
  h1 { font-size: 1.3rem; }
  .meta { color: #666; font-size: 0.85rem; }
  .hint { color: #888; font-size: 0.8rem; margin: 0 0 0.5rem; }
  .chart-head { display: flex; align-items: baseline; gap: 0.75rem; }
  .chart-head button { font: inherit; font-size: 0.8rem; padding: 0.2rem 0.6rem;
    border: 1px solid #ccc; background: #fff; border-radius: 4px; cursor: pointer; }
  .chart-head button:hover { background: #f0f0f0; }
  .chart-wrap { background: #fff; border: 1px solid #e0e0e0; border-radius: 8px; padding: 1rem; height: 520px; }
  code { background: #eee; padding: 0 0.3rem; border-radius: 3px; }
</style>
</head>
<body>
  <h1>Simple mean − VWAP-weighted price</h1>
  <p class="meta">${rows.length} samples · ${exchanges.length} exchanges · ${range} · mean |diff| = ${fmt(meanAbsBps)} bps · max |diff| = ${fmt(maxAbsBps)} bps</p>
  <p class="hint"><code>diff = mean(spot_i) − Σ(spot_i × vol_i) / Σ(vol_i)</code> · positive ⇒ unweighted mean above liquidity-weighted price · scroll = zoom · drag = pan · double-click = reset</p>
  <div class="chart-head">
    <button onclick="diffChart.resetZoom()">Reset zoom</button>
  </div>
  <div class="chart-wrap"><canvas id="diff"></canvas></div>
<script>
const labels = ${JSON.stringify(labels)};
const usdDataset = ${JSON.stringify(usdDataset)};
const bpsDataset = ${JSON.stringify(bpsDataset)};

const diffChart = new Chart(document.getElementById("diff"), {
  type: "line",
  data: { labels, datasets: [usdDataset, bpsDataset] },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "nearest", intersect: false },
    scales: {
      x: { type: "category", ticks: { maxTicksLimit: 12, autoSkip: true } },
      yUsd: { position: "left", title: { display: true, text: "USD" } },
      yBps: { position: "right", title: { display: true, text: "bps" }, grid: { drawOnChartArea: false } },
    },
    plugins: {
      legend: { position: "top" },
      zoom: {
        pan: { enabled: true, mode: "x" },
        zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: "x" },
        limits: { x: { minRange: 5 } },
      },
    },
  },
});
document.getElementById("diff").addEventListener("dblclick", () => diffChart.resetZoom());
</script>
</body>
</html>`;

writeFileSync(outputPath, html);
console.log(
  `Wrote ${outputPath} (${rows.length} samples, mean |diff| = ${fmt(meanAbsBps)} bps, max |diff| = ${fmt(maxAbsBps)} bps)`,
);
