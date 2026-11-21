# AGENTS.md — Shopping Sessions (< 0.25s Target)

## Target
Beat **0.25s** end-to-end wall time on 20M events (container start to exit, 8 pinned vCPUs) while keeping **100% exact compliance** with `rules.md` and staying **< 18 GB peak RSS** and **< 60s** on 60M events (hard container limit: 24 GB, no swap, 180s final cap).

## Boundaries
- **Read-only (never edit):** `rules.md`, `fixtures/`, `Makefile`, `scripts/verify.sh`, `./local/`
- **Editable:** `src/`, `Cargo.toml`, `Dockerfile`, `results.tsv`

## Constraints
1. **Zero stdout:** `stdout` must be 0 bytes. Emit microsecond stage timings to `stderr`.
2. **Container & ISA safety:** Respect `DATA_DIR`, `OUTPUT_DIR`, `SCRATCH_DIR`, `NCPU`, `MEM_GB`. Root FS is read-only; `/tmp` is 64 MB RAM; runs as uid 1000 with no network. Never compile with `-C target-cpu=native`—use runtime dispatch (`is_x86_feature_detected!`) for AVX2/AVX-512 so `linux/amd64` never triggers `SIGILL`.
3. **Zero dataset hardcoding:** Inspect `../local-data/generator/` to optimize fast paths for realistic shapes, but every fast path must have an exact general fallback covering all `rules.md` edge cases (literal `\r`, strict quoting, untrimmed case-sensitive byte-wise IDs, ASCII-only digits, F1/F2/F3 including Feb 29 2028, dedup-before-validation even when later copies change `user_id`, 400-hop DSU naming, exact boundary inequalities, header-only CSVs, arbitrary window lengths).

## Commands
- `bash scripts/verify.sh correct` — 36 fixtures + 1M dev diff + `rules.md` invariants + 0-byte stdout
- `bash scripts/verify.sh bench20` — 3 runs on 20M (median ms, per-stage `stderr` breakdown, `perf stat` IPC/cache-misses/page-faults if available)
- `bash scripts/verify.sh stress60` — 60M wall time + peak RSS (must stay < 18 GB)
- `bash scripts/verify.sh docker` — exact `linux/amd64` container mirror + container startup overhead check

## Engineering Loop
- Drive every optimization from real measurements (`bench20` stage breakdown + hardware counters), never guesswork.
- Always run `verify.sh correct` after any code change before benchmarking.
- Keep and commit changes that achieve a genuine speedup or clean architectural win while preserving correctness and 60M RAM headroom; revert regressions (`git checkout -- src/ Cargo.toml Dockerfile`).
- Append one line per experiment to `results.tsv`:
  `commit | status(KEEP/REVERT/FAIL) | bench20_ms | stress60_rss_mb | bottleneck_stage | change`
- Never run `git push`.