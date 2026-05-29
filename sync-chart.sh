rsync -avz cargo-remote:~/dot-price/results.ndjson $(pwd)
rsync -avz cargo-remote:~/dot-price/errors.ndjson $(pwd)
rsync -avz cargo-remote:~/dot-price/vwap.ndjson $(pwd)

node chart/generate.ts
open chart/chart.html