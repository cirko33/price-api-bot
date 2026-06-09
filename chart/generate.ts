/**
 * Reads ../results.ndjson (DOT spot price per exchange, sampled every few
 * seconds) and ../vwap.ndjson (24h VWAP + quote volume per exchange, sampled
 * less frequently) and writes a self-contained chart.html with three Chart.js
 * line charts:
 *
 *   1. Raw price per exchange over time.
 *   2. Per-exchange deviation from the cross-exchange mean (in basis points),
 *      which makes the arbitrage spread between exchanges visible.
 *   3. Volume-weighted "real price" — sum(spot_i × volume_i) / sum(volume_i),
 *      using each exchange's latest 24h quote volume — overlaid with each
 *      exchange's 24h VWAP. Exchanges with more liquidity dominate the line.
 *
 * The third chart is omitted if vwap.ndjson is missing or empty.
 *
 * Usage:  node chart/generate.ts        (Node >= 22, runs TS directly)
 * Output: chart/chart.html  — open it in a browser.
 */
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const inputPath = join(here, "..", "results.ndjson");
const errorsPath = join(here, "..", "errors.ndjson");
const vwapPath = join(here, "..", "vwap.ndjson");
const outputPath = join(here, "chart.html");

type Row = { ts: number } & Record<string, number>;
type VwapSample = { vwap: number; volume: number };
type VwapRow = { ts: number } & Record<string, VwapSample>;

const rows: Row[] = readFileSync(inputPath, "utf8")
  .split("\n")
  .filter((line) => line.trim().length > 0)
  .map((line) => JSON.parse(line) as Row)
  // Sort by timestamp: the chart and the binary searches below assume the
  // rows are in chronological order, but the input is not guaranteed to be.
  .sort((a, b) => a.ts - b.ts);

if (rows.length === 0) throw new Error("no data in results.ndjson");

// Every key except `ts` is an exchange.
const exchanges = Object.keys(rows[0]).filter((k) => k !== "ts");

// X axis is a real time scale: labels are unix-ms timestamps, and Chart.js's
// time scale renders date+time ticks at the right granularity (and keeps the
// spacing proportional to the actual time gaps between samples).
const fmtTime = (ts: number) =>
  new Date(ts * 1000).toLocaleTimeString("en-GB", { hour12: false });
const fmtDate = (ts: number) =>
  new Date(ts * 1000).toLocaleDateString("en-CA"); // YYYY-MM-DD
const labels = rows.map((r) => r.ts * 1000);
const dateLabel =
  fmtDate(rows[0].ts) === fmtDate(rows[rows.length - 1].ts)
    ? fmtDate(rows[0].ts)
    : `${fmtDate(rows[0].ts)} → ${fmtDate(rows[rows.length - 1].ts)}`;

// Distinct colors per exchange.
const palette = [
  "#e6194b", "#3cb44b", "#4363d8", "#f58231",
  "#911eb4", "#42d4f4", "#f032e6", "#9a6324",
];
const color = (i: number) => palette[i % palette.length];

const priceDatasets = exchanges.map((ex, i) => ({
  label: ex,
  data: rows.map((r) => r[ex]),
  borderColor: color(i),
  backgroundColor: color(i),
  borderWidth: 1.5,
  pointRadius: 0,
  tension: 0.1,
}));

// Per-row deviation (bps) for the deviation chart and for error-marker y-values.
const devByRow = rows.map((r) => {
  const mean = exchanges.reduce((sum, e) => sum + r[e], 0) / exchanges.length;
  return Object.fromEntries(
    exchanges.map((e) => [e, ((r[e] - mean) / mean) * 10_000]),
  ) as Record<string, number>;
});

const devDatasets = exchanges.map((ex, i) => ({
  label: ex,
  data: devByRow.map((d) => d[ex]),
  borderColor: color(i),
  backgroundColor: color(i),
  borderWidth: 1.5,
  pointRadius: 0,
  tension: 0.1,
}));

// --- Errors: ndjson rows like {ts, <Exchange>: "<message>", ...}. -----------
type ErrorMarker = { x: number; y: number; msg: string; ts: number };

// Find the index of the closest results.ndjson row by timestamp.
const tsArr = rows.map((r) => r.ts);
const nearestIdx = (ts: number) => {
  let lo = 0, hi = tsArr.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (tsArr[mid] < ts) lo = mid + 1;
    else hi = mid;
  }
  if (lo > 0 && Math.abs(tsArr[lo - 1] - ts) < Math.abs(tsArr[lo] - ts)) lo--;
  return lo;
};

const errorsByExchange: Record<string, ErrorMarker[]> = Object.fromEntries(
  exchanges.map((e) => [e, []]),
);
let errorCount = 0;

if (existsSync(errorsPath)) {
  for (const line of readFileSync(errorsPath, "utf8").split("\n")) {
    if (!line.trim()) continue;
    const row = JSON.parse(line) as { ts: number } & Record<string, string>;
    const idx = nearestIdx(row.ts);
    for (const [k, v] of Object.entries(row)) {
      if (k === "ts" || !errorsByExchange[k]) continue;
      errorsByExchange[k].push({
        x: labels[idx],
        y: rows[idx][k],
        msg: String(v),
        ts: row.ts,
      });
      errorCount++;
    }
  }
}

const buildErrorDatasets = (yField: "price" | "dev") =>
  exchanges
    .map((ex, i) => ({
      label: `${ex} error`,
      data: errorsByExchange[ex].map((m) => ({
        x: m.x,
        y: yField === "price" ? m.y : devByRow[nearestIdx(m.ts)][ex],
        msg: m.msg,
        ts: m.ts,
      })),
      borderColor: color(i),
      backgroundColor: color(i),
      showLine: false,
      pointStyle: "triangle",
      pointRadius: 7,
      pointHoverRadius: 9,
      pointBorderColor: "#000",
      pointBorderWidth: 1,
      isError: true,
      // Hide from legend; the line dataset already covers each exchange.
    }))
    .filter((d) => d.data.length > 0);

const priceErrorDatasets = buildErrorDatasets("price");
const devErrorDatasets = buildErrorDatasets("dev");

// --- VWAP & volume-weighted real price ---------------------------------------
//
// Each spot row picks the most recent VWAP row at or before its timestamp (we
// never use future volumes). Real price per row = Σ(spot_i × vol_i) / Σ(vol_i)
// over exchanges that have BOTH a spot price this round and a volume in the
// chosen VWAP row.
const vwapRows: VwapRow[] = existsSync(vwapPath)
  ? readFileSync(vwapPath, "utf8")
      .split("\n")
      .filter((l) => l.trim().length > 0)
      .map((l) => JSON.parse(l) as VwapRow)
      // latestVwapIdx() below binary-searches these, so they must be sorted.
      .sort((a, b) => a.ts - b.ts)
  : [];

const haveVwap = vwapRows.length > 0;

// For each spot row, find the index of the latest vwap row with ts <= spot.ts.
// Returns -1 if no vwap row precedes this spot row.
const vwapTs = vwapRows.map((r) => r.ts);
const latestVwapIdx = (ts: number) => {
  if (vwapTs.length === 0 || vwapTs[0] > ts) return -1;
  let lo = 0, hi = vwapTs.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (vwapTs[mid] <= ts) lo = mid;
    else hi = mid - 1;
  }
  return lo;
};

// Real price per spot row, and per-exchange VWAP value snapshot per spot row.
// Also compute the unweighted arithmetic mean of spot prices per row, so the
// third chart can show both means side-by-side: the bold black volume-weighted
// "real price" and the orange unweighted mean. The gap between them shows how
// much liquidity weighting actually shifts the cross-exchange consensus.
const realPriceByRow: (number | null)[] = [];
const meanPriceByRow: (number | null)[] = [];
const vwapPerExchangeByRow: Record<string, (number | null)[]> = Object.fromEntries(
  exchanges.map((e) => [e, []]),
);

for (const r of rows) {
  const spots = exchanges
    .map((e) => r[e])
    .filter((v): v is number => typeof v === "number");
  meanPriceByRow.push(spots.length > 0 ? spots.reduce((s, v) => s + v, 0) / spots.length : null);

  const vi = latestVwapIdx(r.ts);
  if (vi < 0) {
    realPriceByRow.push(null);
    for (const e of exchanges) vwapPerExchangeByRow[e].push(null);
    continue;
  }
  const vr = vwapRows[vi];
  let num = 0, den = 0;
  for (const e of exchanges) {
    const s = vr[e];
    if (typeof r[e] !== "number") continue;
    if (!s || typeof s.volume !== "number" || s.volume <= 0) continue;
    num += r[e] * s.volume;
    den += s.volume;
  }
  realPriceByRow.push(den > 0 ? num / den : null);
  for (const e of exchanges) {
    const s = vr[e];
    vwapPerExchangeByRow[e].push(s && typeof s.vwap === "number" ? s.vwap : null);
  }
}

const realDataset = {
  label: "Real price (vol-weighted)",
  data: realPriceByRow,
  borderColor: "#111",
  backgroundColor: "#111",
  borderWidth: 2.5,
  pointRadius: 0,
  tension: 0.1,
  spanGaps: false,
};

const meanDataset = {
  label: "Simple mean (unweighted)",
  data: meanPriceByRow,
  borderColor: "#ff7700",
  backgroundColor: "#ff7700",
  borderWidth: 2,
  pointRadius: 0,
  tension: 0.1,
  spanGaps: false,
};

const vwapDatasets = exchanges.map((ex, i) => ({
  label: `${ex} VWAP`,
  data: vwapPerExchangeByRow[ex],
  borderColor: color(i),
  backgroundColor: color(i),
  borderWidth: 1,
  borderDash: [4, 4],
  pointRadius: 0,
  tension: 0,
  spanGaps: false,
}));

const realChartSection = haveVwap
  ? `
  <div class="chart-head">
    <h2>Volume-weighted real price (USD)</h2>
    <button onclick="realChart.resetZoom()">Reset zoom</button>
  </div>
  <p class="hint">bold black = Σ(spot × vol) / Σ(vol) using latest 24h quote volumes · orange = unweighted arithmetic mean · dashed = per-exchange 24h VWAP · scroll = zoom · drag = pan · double-click = reset</p>
  <div class="chart-wrap"><canvas id="real"></canvas></div>`
  : "";

const realChartScript = haveVwap
  ? `
const realDataset = ${JSON.stringify(realDataset)};
const meanDataset = ${JSON.stringify(meanDataset)};
const vwapDatasets = ${JSON.stringify(vwapDatasets)};
const realChart = new Chart(document.getElementById("real"), {
  type: "line",
  data: { labels, datasets: [realDataset, meanDataset, ...vwapDatasets] },
  options: { ...common, scales: { ...common.scales, y: { title: { display: true, text: "USD" } } } },
});
document.getElementById("real").addEventListener("dblclick", () => realChart.resetZoom());`
  : "";

const vwapMeta = haveVwap ? ` · ${vwapRows.length} VWAP sample${vwapRows.length === 1 ? "" : "s"}` : "";

const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>DOT price across exchanges</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/hammerjs@2"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2"></script>
<style>
  body { font-family: system-ui, sans-serif; margin: 2rem; background: #fafafa; color: #222; }
  h1 { font-size: 1.3rem; } h2 { font-size: 1.05rem; margin-top: 2.5rem; margin-bottom: 0.3rem; }
  .meta { color: #666; font-size: 0.85rem; }
  .hint { color: #888; font-size: 0.8rem; margin: 0 0 0.5rem; }
  .chart-head { display: flex; align-items: baseline; gap: 0.75rem; }
  .chart-head button { font: inherit; font-size: 0.8rem; padding: 0.2rem 0.6rem;
    border: 1px solid #ccc; background: #fff; border-radius: 4px; cursor: pointer; }
  .chart-head button:hover { background: #f0f0f0; }
  .chart-wrap { background: #fff; border: 1px solid #e0e0e0; border-radius: 8px; padding: 1rem; height: 460px; }
</style>
</head>
<body>
  <h1>DOT price across exchanges</h1>
  <p class="meta">${rows.length} samples · ${exchanges.length} exchanges · ${dateLabel} · ${fmtTime(rows[0].ts)}–${fmtTime(rows[rows.length - 1].ts)} · ${errorCount} error${errorCount === 1 ? "" : "s"}${vwapMeta}</p>

  <div class="chart-head">
    <h2>Price (USD)</h2>
    <button onclick="priceChart.resetZoom()">Reset zoom</button>
  </div>
  <p class="hint">scroll = zoom · drag = pan · double-click = reset</p>
  <div class="chart-wrap"><canvas id="price"></canvas></div>

  <div class="chart-head">
    <h2>Deviation from cross-exchange mean (basis points)</h2>
    <button onclick="devChart.resetZoom()">Reset zoom</button>
  </div>
  <p class="hint">scroll = zoom · drag = pan · double-click = reset</p>
  <div class="chart-wrap"><canvas id="dev"></canvas></div>
${realChartSection}
<script>
const labels = ${JSON.stringify(labels)};
const priceDatasets = ${JSON.stringify(priceDatasets)};
const devDatasets = ${JSON.stringify(devDatasets)};
const priceErrorDatasets = ${JSON.stringify(priceErrorDatasets)};
const devErrorDatasets = ${JSON.stringify(devErrorDatasets)};

const tooltipCallbacks = {
  // Title shows the full date + time of the hovered point (x is a unix-ms
  // timestamp on the time scale).
  title: (items) =>
    items.length
      ? new Date(items[0].parsed.x).toLocaleString("en-GB", { hour12: false })
      : "",
  // Custom label so error points show "<Exchange> error @ HH:MM:SS — <msg>",
  // while normal line points keep Chart.js's default formatting.
  label: (ctx) => {
    if (ctx.dataset.isError) {
      const t = new Date(ctx.raw.ts * 1000).toLocaleTimeString("en-GB", { hour12: false });
      return ctx.dataset.label + " @ " + t + " — " + ctx.raw.msg;
    }
    return ctx.dataset.label + ": " + ctx.formattedValue;
  },
};

const common = {
  responsive: true,
  maintainAspectRatio: false,
  interaction: { mode: "nearest", intersect: false },
  scales: {
    x: {
      type: "time",
      time: {
        // Tooltip already overridden via callbacks; these control the axis ticks.
        displayFormats: {
          second: "HH:mm:ss",
          minute: "HH:mm",
          hour: "MMM d, HH:mm",
          day: "MMM d",
          week: "MMM d",
          month: "MMM yyyy",
        },
      },
      ticks: { maxTicksLimit: 12, autoSkip: true, maxRotation: 0 },
    },
  },
  plugins: {
    legend: {
      position: "top",
      // Hide the per-exchange "error" entries; legend stays clean.
      labels: { filter: (item, data) => !data.datasets[item.datasetIndex].isError },
    },
    tooltip: { callbacks: tooltipCallbacks },
    zoom: {
      pan: { enabled: true, mode: "x" },
      zoom: {
        wheel: { enabled: true },
        pinch: { enabled: true },
        mode: "x",
      },
      limits: { x: { minRange: 1000 * 30 } },  // can't zoom past a ~30s window
    },
  },
};

const priceChart = new Chart(document.getElementById("price"), {
  type: "line",
  data: { labels, datasets: [...priceDatasets, ...priceErrorDatasets] },
  options: { ...common, scales: { ...common.scales, y: { title: { display: true, text: "USD" } } } },
});

const devChart = new Chart(document.getElementById("dev"), {
  type: "line",
  data: { labels, datasets: [...devDatasets, ...devErrorDatasets] },
  options: { ...common, scales: { ...common.scales, y: { title: { display: true, text: "bps vs mean" } } } },
});

// Double-click anywhere on a chart to reset its zoom.
document.getElementById("price").addEventListener("dblclick", () => priceChart.resetZoom());
document.getElementById("dev").addEventListener("dblclick", () => devChart.resetZoom());
${realChartScript}
</script>
</body>
</html>`;

writeFileSync(outputPath, html);
console.log(
  `Wrote ${outputPath} (${rows.length} samples, ${exchanges.length} exchanges, ${vwapRows.length} VWAP samples)`,
);
