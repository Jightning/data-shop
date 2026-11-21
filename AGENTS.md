# Shopping Sessions Speed Pipeline

## Objective
Minimize end-to-end container wall time (target: < 0.3s on 20M events) while maintaining 100% exact compliance with `rules.md` and staying safely under 18 GB peak RAM and < 120s on 60M events.

## Editable Scope
- `src/`, `Cargo.toml`, `Dockerfile`
- `results.tsv` (append-only experiment log)

## Invariants
1. **Zero stdout:** Print NOTHING to `stdout`. All timings/logs go strictly to `stderr`.
2. **Container safety:** Respect `DATA_DIR`, `OUTPUT_DIR`, `SCRATCH_DIR`, `NCPU`, `MEM_GB`. Never write to `/` or `/tmp` (64 MB cap). Never use `-C target-cpu=native` (use runtime CPU detection for AVX2/SIMD so `linux/amd64` never crashes with `SIGILL`).
3. **No dataset hardcoding:** Never hardcode window dates, counts of files/products/campaigns, or seed assumptions. Every optimization must preserve every edge case in `rules.md` (literal `\r`, strict quotes, untrimmed case-sensitive IDs, ASCII digits only, F1/F2/F3 timestamps including leap day 2028, dedup-before-validation, 400-hop DSU naming, exact boundary inequalities, header-only CSVs).
4. **60M survival:** Peak RSS must stay < 18 GB on 60M events (container hard cap is 24 GB, no swap).

## Verification Commands
Run these verbatim; never claim success without fresh command output:
- Correctness gate (fast): `bash scripts/verify.sh correct` (36 fixtures + 1M dev check + self-checks + stdout == 0 bytes)
- Speed benchmark (20M): `bash scripts/verify.sh bench20` (median of 3 runs + stderr stage timings)
- Memory gate (60M): `bash scripts/verify.sh stress60` (wall time + peak RSS)
- Container gate: `bash scripts/verify.sh docker` (read-only root, 64 MB `/tmp`, no network, uid 1000, 8 vCPUs, 24 GB RAM)

## Required Workflow (Follow Every Turn)

1. **Inspect:** Read `results.tsv` and the latest per-stage `stderr` timings. Pick the single slowest stage (Amdahl's law).
2. **Hypothesize:** State ONE concrete change, the stage it targets, and why it preserves `rules.md` and < 18 GB RAM at 60M.
3. **Edit:** Make the minimal focused change.
4. **Verify Correctness FIRST:** Run `bash scripts/verify.sh correct`. If ANY check fails, fix root cause or revert immediately—never benchmark broken code.
5. **Benchmark:** Run `bash scripts/verify.sh bench20`.
6. **Promote or Revert:**
   - **KEEP** if 20M median improves by >= 3% (or simplifies code at equal speed) AND `stress60` stays < 18 GB RAM (run `stress60` whenever memory layout or data structures change). Run `git commit` with the measured delta.
   - **REVERT** otherwise: run `git checkout -- src/ Cargo.toml Dockerfile` to restore the last clean commit.
7. **Log:** Append one tab-separated row to `results.tsv`:
   `commit | status(KEEP/REVERT/FAIL) | bench20_ms | stress60_rss_mb | slowest_stage | hypothesis`
8. **Fail fast:** If the same failure repeats twice, stop and diagnose the root cause instead of blindly retrying.

## Output Format
Be terse. Report: (1) hypothesis, (2) files changed, (3) exact command results (`correct`, `bench20`, stage timings), and (4) `KEEP` or `REVERT`.