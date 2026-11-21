# Prompts
Lets run these in order (the prompts are the ones inside the ```)

# Step 1
```txt
Inspect this repository and `../local-data/` via shell commands (read `AGENTS.md`, `rules.md`, starter `Makefile`, and test scripts).

Build the harness:
1. `scripts/verify.sh` with 4 subcommands (fail fast with non-zero exit on any error):
   - `correct`: builds `--release`, runs all 36 fixtures, runs the 1M dev diff against `../local-data/dev_1m` and `../local-data/dev-answer`, verifies `stdout` is 0 bytes, and checks the `rules.md` table invariants (matching total `sessions` and `revenue_cents`, and `assisted_sessions >= non-organic sessions`).
   - `bench20`: runs 3 iterations against `../local-data/bench_20m` (clearing output/scratch between runs), prints each run's wall time in ms, the median, and the median run's `stderr` stage timings. If `perf` is available and permitted, include a 1-run `perf stat` summary (cycles, instructions, LLC-load-misses, page-faults, context-switches).
   - `stress60`: runs once against `../local-data/stress_60m` measuring wall time and peak RSS; fails if peak RSS > 18 GB or wall time > 90s.
   - `docker`: builds `linux/amd64` and runs the exact grader container mirror (`--cpus=8 --memory=24g --memory-swap=24g --read-only --tmpfs /tmp:size=64m --network none --user 1000:1000` with `DATA_DIR`, `OUTPUT_DIR`, `SCRATCH_DIR`, `HOME`, `NCPU`, `MEM_GB`) against `dev_1m` and reports container wall time.
2. `.gitignore`, `.dockerignore` (exclude `target/`, `.git/`, `fixtures/`, `scripts/`, `results.tsv`), and `results.tsv` with header row.
3. Check if `../local-data/{dev_1m,bench_20m,stress_60m}` exist; if missing and `../local-data/generator` is present, compile the generator and create them using the exact `--clicks` ratio from the starter `Makefile`.

Make `scripts/verify.sh` executable, validate its syntax, and confirm readiness. Do not push.
```

# Step 2
```txt
Read `AGENTS.md`, `rules.md`, and `../local-data/generator/` from disk.

Design and build a standalone, ultra-low-latency Rust solution (or static C++ if you judge it strictly faster for our custom SIMD + Parquet pipeline—pick one and justify briefly) targeting < 0.25s on 20M events after tuning. Skip naive prototypes and build the right foundation on v1:
- Static `linux/amd64` binary in a `FROM scratch` (or minimal) Dockerfile; call `_exit(0)` after flushing outputs.
- Zero per-row heap allocations: byte-chunked parallel I/O across all `NCPU` threads, custom byte-level CSV & SWAR F1/F2/F3 timestamp parsers (fast paths with exact general fallbacks), radix-partitioned L2/L3-resident `event_id` dedup, unlinked-user hashing without global interning contention, flat contiguous per-person slices, whale-first scheduling, and linear $O(C + S)$ sliding-window attribution/assists.
- Minimal hand-written uncompressed Parquet writer (each worker writes its own `user_daily` shard lock-free; single merged `campaign_daily` file).
- Microsecond per-stage timing breakdown printed to `stderr`; zero bytes to `stdout`.

Run `bash scripts/verify.sh correct`, `bash scripts/verify.sh bench20`, `bash scripts/verify.sh stress60`, and `bash scripts/verify.sh docker`. Iterate until all four pass, log the baseline in `results.tsv`, and commit locally. Do not push.
```

# Step 3
```txt
Gather full state via commands:
- `git log -n 10 --oneline`
- `cat results.tsv`
- `bash scripts/verify.sh bench20`

You have full architectural autonomy to drive `bench20` well below 290 ms (target < 200–250 ms) without breaking `rules.md` or exceeding 18 GB peak RSS on 60M. Inspect `src/`, `Dockerfile`, and `../local-data/generator/` as needed.

Run a focused optimization sprint (up to 4 keep/revert experiments):
- Use the real `bench20` stage timings and hardware counters (`perf` / flamegraph / custom instrumentation if helpful) to attack the largest remaining latency contributors—whether that is I/O page faults (`pread` vs `mmap` + `MADV_HUGEPAGE`/`WILLNEED`), runtime-dispatched AVX2/AVX-512 parsing, radix partition sizing/prefetching, hash table layout, false sharing, sorting, Parquet serialization, or container/binary startup overhead.
- After every edit: run `bash scripts/verify.sh correct` before benchmarking. Run `bash scripts/verify.sh stress60` whenever memory layout changes.
- Commit real wins and cleanly revert anything that doesn't improve speed or simplify code. Log every attempt in `results.tsv`.

Do not modify read-only files and do not run `git push`. Report the final `bench20` median, per-stage breakdown, and the next biggest bottleneck.
```