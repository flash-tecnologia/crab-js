# PDF Benchmark

This package compares PDF generation paths with the same generated table dataset:

- `Node + pdf-crab`: local native `pdf-crab-js` generation.
- `Node + Gotenberg`: Node.js multipart upload to Gotenberg's Chromium HTML-to-PDF endpoint.

## Setup

Start Gotenberg locally when running the Gotenberg scenario:

```bash
docker run --rm -p 3000:3000 gotenberg/gotenberg:8
```

## Run

```bash
pnpm --filter pdf-benchmark benchmark
```

The benchmark defaults to a 10-page PDF with 10 table rows per page. Useful knobs:

- `PDF_BENCHMARK_PAGES=5120` changes the page count.
- `PDF_BENCHMARK_RUNS=10` changes measured runs per scenario.
- `PDF_BENCHMARK_WARMUP=3` changes warmup runs per scenario.
- `PDF_BENCHMARK_ONLY=pdf-crab` or `PDF_BENCHMARK_ONLY=gotenberg-node` selects scenarios.
- `PDF_BENCHMARK_GOTENBERG_URL=http://localhost:3000` changes the Gotenberg base URL.
- `PDF_BENCHMARK_WRITE=1` writes generated PDFs to `benchmarks/pdf/output/`.
- `PDF_BENCHMARK_COLORS=0` disables terminal colors.

For a large run similar to the captured benchmark table style:

```bash
PDF_BENCHMARK_PAGES=5120 PDF_BENCHMARK_RUNS=1 PDF_BENCHMARK_WARMUP=0 pnpm --filter pdf-benchmark benchmark
```
