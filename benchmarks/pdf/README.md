# PDF Benchmark

This package compares PDF generation paths with the same generated table dataset:

- `Node + pdf-crab`: local native `pdf-crab-js` generation.
- `Node + html-to-pdf-crab`: local native `html-to-pdf-crab-js` HTML-to-PDF generation.
- `Node + Gotenberg`: Node.js multipart upload to Gotenberg's Chromium HTML-to-PDF endpoint.

## 10-page Snapshot

Local 10-page benchmark, fastest to slowest by execution time:

| Order | Language                | Mode       | Execution time |       Throughput |
| ----- | ----------------------- | ---------- | -------------: | ---------------: |
| 1     | Node + pdf-crab         | local      |       4.116 ms | 2429.253 pages/s |
| 2     | Node + pdf-crab         | builder    |       4.232 ms | 2362.863 pages/s |
| 3     | Node + html-to-pdf-crab | local-html |      62.327 ms |  160.443 pages/s |
| 4     | Node + Gotenberg        | gotenberg  |     128.304 ms |   77.940 pages/s |

Interpretation:

- `pdf-crab-js` is the fastest path when the document can be represented as structured PDF pages
  and elements.
- `html-to-pdf-crab-js` is the easy HTML/CSS path. It avoids Chromium/Gotenberg while still doing
  HTML layout work.
- Gotenberg is useful when Chromium compatibility is required, but it adds a service boundary and
  browser conversion overhead.

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
- `PDF_BENCHMARK_ONLY=pdf-crab`, `PDF_BENCHMARK_ONLY=html-to-pdf-crab-js`, or `PDF_BENCHMARK_ONLY=gotenberg-node` selects scenarios.
- `PDF_BENCHMARK_GOTENBERG_URL=http://localhost:3000` changes the Gotenberg base URL.
- `PDF_BENCHMARK_WRITE=1` writes generated PDFs to `benchmarks/pdf/output/`.
- `PDF_BENCHMARK_COLORS=0` disables terminal colors.

For a large run similar to the captured benchmark table style:

```bash
PDF_BENCHMARK_PAGES=5120 PDF_BENCHMARK_RUNS=1 PDF_BENCHMARK_WARMUP=0 pnpm --filter pdf-benchmark benchmark
```
