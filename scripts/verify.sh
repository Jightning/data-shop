#!/usr/bin/env bash
# Verification harness for the Shopping Sessions solution.
#
#   bash scripts/verify.sh correct    release build + 36 fixtures + 1M dev diff + rules.md invariants + 0-byte stdout
#   bash scripts/verify.sh bench20    3 timed runs on bench_20m: per-run ms, median, median run's stderr, perf stat
#   bash scripts/verify.sh stress60   1 run on stress_60m: wall time + peak RSS (fails > 18 GB or > 90 s)
#   bash scripts/verify.sh docker     linux/amd64 image, run with the grader's container flags on dev_1m
#   bash scripts/verify.sh data       generate missing dev_1m / bench_20m / stress_60m with ../local-data/generator
#
# Every subcommand fails fast with a non-zero exit on the first error.
# Overridable env: LOCAL_DATA, FIXTURES, BIN, PYTHON, NCPU, MEM_GB, IMAGE, PLATFORM, VERIFY_WORK,
#                  BENCH_RUNS, STRESS_MAX_RSS_BYTES, STRESS_MAX_S
set -euo pipefail
export MSYS_NO_PATHCONV=1   # keep Git Bash from mangling /data-style paths in docker args

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO"

# ---------------------------------------------------------------- locations
if [[ -z "${LOCAL_DATA:-}" ]]; then
  if [[ -d "$REPO/../local-data" ]]; then LOCAL_DATA="$REPO/../local-data"; else LOCAL_DATA="$REPO/local"; fi
fi
LOCAL_DATA=$(cd "$LOCAL_DATA" && pwd)
DEV_DIR="$LOCAL_DATA/dev_1m"
DEV_ANSWER="$LOCAL_DATA/dev-answer"
BENCH_DIR="$LOCAL_DATA/bench_20m"
STRESS_DIR="$LOCAL_DATA/stress_60m"
GENERATOR_DIR="$LOCAL_DATA/generator"

# Fixtures: repo-root fixtures/ if present, else the copy shipped with the starters.
if [[ -z "${FIXTURES:-}" ]]; then
  for candidate in "$REPO/fixtures" "$REPO/local/starter-duckdb/fixtures" "$LOCAL_DATA/starter-duckdb/fixtures"; do
    if [[ -d "$candidate" ]]; then FIXTURES="$candidate"; break; fi
  done
fi
# Starter test scripts (run_fixtures.py / compare.py, need `polars`).
STARTER_SCRIPTS=""
for candidate in "$REPO/local/starter-duckdb/scripts" "$LOCAL_DATA/starter-duckdb/scripts"; do
  if [[ -f "$candidate/run_fixtures.py" ]]; then STARTER_SCRIPTS="$candidate"; break; fi
done

PYTHON="${PYTHON:-python3}"
export NCPU="${NCPU:-8}" MEM_GB="${MEM_GB:-24}"
WORK="${VERIFY_WORK:-$REPO/target/verify}"
IMAGE="${IMAGE:-shopping-sessions:latest}"
PLATFORM="${PLATFORM:-linux/amd64}"
EXPECTED_FIXTURES=36

# Generator ratio taken from the starter Makefile: --events 1000000 --clicks 250000 (clicks = events / 4).
DEV_SEED=1 BENCH_SEED=2 STRESS_SEED=3

# ---------------------------------------------------------------- helpers
die()  { echo "FAIL: $*" >&2; exit 1; }
info() { echo "==> $*" >&2; }
now_ns() { date +%s%N; }
need() { command -v "$1" >/dev/null 2>&1 || die "'$1' not found on PATH${2:+ ($2)}"; }

need_dir() { [[ -d "$1" ]] || die "missing $1${2:+ — $2}"; }

need_python_polars() {
  need "$PYTHON"
  "$PYTHON" -c 'import polars' 2>/dev/null || die "$PYTHON cannot import polars (pip install polars)"
  [[ -n "$STARTER_SCRIPTS" ]] || die "cannot find starter scripts/run_fixtures.py"
}

# Release build; sets BIN to the built executable unless BIN was given.
build_release() {
  need cargo
  [[ -f "$REPO/Cargo.toml" ]] || die "no Cargo.toml in $REPO (solution not written yet?)"
  info "cargo build --release"
  local json
  json=$(cargo build --release --message-format=json-render-diagnostics) || die "cargo build --release failed"
  if [[ -z "${BIN:-}" ]]; then
    BIN=$(printf '%s\n' "$json" | "$PYTHON" -c '
import json, sys
exe = None
for line in sys.stdin:
    try: m = json.loads(line)
    except ValueError: continue
    if m.get("reason") == "compiler-artifact" and m.get("executable") and "bin" in m["target"]["kind"]:
        exe = m["executable"]
print(exe or "")')
  fi
  [[ -n "${BIN:-}" && -x "$BIN" ]] || die "could not locate the release binary (set BIN=...)"
  info "binary: $BIN"
}

fresh_dirs() {  # fresh_dirs OUT SCRATCH
  rm -rf "$1" "$2"
  mkdir -p "$1" "$2"
}

# run_bin DATA OUT SCRATCH STDOUT_FILE STDERR_FILE -> runs the binary with the container env.
run_bin() {
  DATA_DIR="$1" OUTPUT_DIR="$2" SCRATCH_DIR="$3" HOME="$3" TMPDIR="$3" "$BIN" >"$4" 2>"$5"
}

check_stdout_empty() {  # check_stdout_empty FILE LABEL
  local bytes
  bytes=$(wc -c <"$1" | tr -d ' ')
  [[ "$bytes" == 0 ]] || { head -c 500 "$1" >&2; die "$2: stdout was $bytes bytes, must be 0"; }
}

# rules.md self-check over an output folder holding user_daily/ and campaign_daily/.
check_invariants() {
  "$PYTHON" - "$1" <<'PY'
import sys
from pathlib import Path
import polars as pl

def load(folder):
    files = [p for p in sorted(Path(folder).glob("*.parquet"))
             if not p.name.startswith((".", "_")) and p.is_file() and not p.is_symlink()]
    if not files:
        sys.exit(f"invariants: no parquet files in {folder}")
    return pl.concat([pl.read_parquet(p) for p in files], how="vertical_relaxed")

out = Path(sys.argv[1])
u, c = load(out / "user_daily"), load(out / "campaign_daily")
for name, df, col in (("user_daily", u, "day"), ("campaign_daily", c, "day")):
    if df.schema[col] != pl.Date:
        sys.exit(f"invariants: {name}.day is {df.schema[col]}, must be DATE")
us, cs = int(u["sessions"].sum() or 0), int(c["sessions"].sum() or 0)
ur, cr = int(u["revenue_cents"].sum() or 0), int(c["revenue_cents"].sum() or 0)
assisted = int(c["assisted_sessions"].sum() or 0)
non_organic = int(c.filter(pl.col("campaign_id") != "organic")["sessions"].sum() or 0)
organic_assists = int(c.filter(pl.col("campaign_id") == "organic")["assisted_sessions"].sum() or 0)
print(f"invariants: sessions {us} vs {cs}; revenue_cents {ur} vs {cr}; "
      f"assisted {assisted} >= non-organic sessions {non_organic}")
errors = []
if us != cs: errors.append("summed sessions differ between tables")
if ur != cr: errors.append("summed revenue_cents differ between tables")
if assisted < non_organic: errors.append("assisted_sessions < credited non-organic sessions")
if organic_assists: errors.append("organic has non-zero assisted_sessions")
if errors:
    sys.exit("invariants FAILED: " + "; ".join(errors))
PY
}

median() { printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}'; }

# ---------------------------------------------------------------- data generation
generate_one() {  # generate_one DIR SEED EVENTS
  local dir="$1" seed="$2" events="$3" clicks=$(( $3 / 4 ))
  if [[ -f "$dir/manifest.json" ]]; then info "$dir exists"; return; fi
  [[ -x "$GENERATE" ]] || die "generator binary missing"
  info "generating $dir (seed $seed, events $events, clicks $clicks)"
  rm -rf "$dir.partial"
  "$GENERATE" --seed "$seed" --events "$events" --clicks "$clicks" --output "$dir.partial"
  mv "$dir.partial" "$dir"
}

cmd_data() {
  need_dir "$GENERATOR_DIR" "cannot generate datasets"
  need cargo
  info "building generator"
  (cd "$GENERATOR_DIR" && cargo build --release --quiet) || die "generator build failed"
  GENERATE="$GENERATOR_DIR/target/release/generate"
  [[ -x "$GENERATE" ]] || GENERATE="$GENERATE.exe"
  generate_one "$DEV_DIR"    "$DEV_SEED"    1000000
  generate_one "$BENCH_DIR"  "$BENCH_SEED"  20000000
  generate_one "$STRESS_DIR" "$STRESS_SEED" 60000000
}

# ---------------------------------------------------------------- correct
cmd_correct() {
  need_python_polars
  [[ -n "${FIXTURES:-}" ]] && need_dir "$FIXTURES" || die "fixtures directory not found (set FIXTURES=...)"
  need_dir "$DEV_DIR" "run: bash scripts/verify.sh data"
  need_dir "$DEV_ANSWER"
  build_release

  local count
  count=$(find "$FIXTURES" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')
  [[ "$count" == "$EXPECTED_FIXTURES" ]] || die "expected $EXPECTED_FIXTURES fixtures in $FIXTURES, found $count"

  # Wrapper: gives the binary a scratch dir and fails the fixture if anything reaches stdout.
  mkdir -p "$WORK"
  local wrapper="$WORK/fixture_wrapper.sh"
  cat >"$wrapper" <<EOF
#!/usr/bin/env bash
set -euo pipefail
scratch=\$(mktemp -d)
trap 'rm -rf "\$scratch"' EXIT
stdout_file="\$scratch/.stdout"
SCRATCH_DIR="\$scratch" HOME="\$scratch" TMPDIR="\$scratch" "$BIN" >"\$stdout_file"
bytes=\$(wc -c <"\$stdout_file" | tr -d ' ')
if [[ "\$bytes" != 0 ]]; then echo "stdout was \$bytes bytes, must be 0" >&2; exit 97; fi
EOF
  chmod +x "$wrapper"

  info "fixtures: $FIXTURES"
  "$PYTHON" "$STARTER_SCRIPTS/run_fixtures.py" "$FIXTURES" -- bash "$wrapper" || die "fixtures failed"

  info "1M dev run: $DEV_DIR"
  local out="$WORK/dev_out" scratch="$WORK/dev_scratch"
  fresh_dirs "$out" "$scratch"
  local t0 t1
  t0=$(now_ns)
  run_bin "$DEV_DIR" "$out" "$scratch" "$WORK/dev.stdout" "$WORK/dev.stderr" \
    || { tail -n 40 "$WORK/dev.stderr" >&2; die "binary exited non-zero on dev_1m"; }
  t1=$(now_ns)
  check_stdout_empty "$WORK/dev.stdout" "dev_1m"
  info "dev_1m wall: $(( (t1 - t0) / 1000000 )) ms"

  info "diff against $DEV_ANSWER"
  (cd "$STARTER_SCRIPTS" && "$PYTHON" compare.py "$out" "$DEV_ANSWER") || die "dev_1m output differs from dev-answer"

  check_invariants "$out" || die "rules.md invariants failed"
  echo "correct: PASS"
}

# ---------------------------------------------------------------- bench20
cmd_bench20() {
  need_dir "$BENCH_DIR" "run: bash scripts/verify.sh data"
  build_release
  local runs="${BENCH_RUNS:-3}" out="$WORK/bench_out" scratch="$WORK/bench_scratch"
  local -a times=()
  local i t0 t1 ms
  for (( i = 1; i <= runs; i++ )); do
    fresh_dirs "$out" "$scratch"
    t0=$(now_ns)
    run_bin "$BENCH_DIR" "$out" "$scratch" "$WORK/bench.$i.stdout" "$WORK/bench.$i.stderr" \
      || { tail -n 40 "$WORK/bench.$i.stderr" >&2; die "bench run $i exited non-zero"; }
    t1=$(now_ns)
    check_stdout_empty "$WORK/bench.$i.stdout" "bench run $i"
    ms=$(( (t1 - t0) / 1000000 ))
    times+=("$ms")
    echo "run $i: $ms ms"
  done
  local med median_run=1
  med=$(median "${times[@]}")
  for (( i = 1; i <= runs; i++ )); do
    if [[ "${times[i-1]}" == "$med" ]]; then median_run=$i; break; fi
  done
  echo "median: $med ms (run $median_run)"
  echo "--- stderr of median run ($median_run) ---"
  cat "$WORK/bench.$median_run.stderr"

  if command -v perf >/dev/null 2>&1 && perf stat -e cycles -- true >/dev/null 2>&1; then
    echo "--- perf stat (1 run) ---"
    fresh_dirs "$out" "$scratch"
    DATA_DIR="$BENCH_DIR" OUTPUT_DIR="$out" SCRATCH_DIR="$scratch" HOME="$scratch" TMPDIR="$scratch" \
      perf stat -e cycles,instructions,LLC-load-misses,page-faults,context-switches \
      -o "$WORK/perf.txt" -- "$BIN" >/dev/null 2>"$WORK/perf.run.stderr" \
      || die "perf run exited non-zero"
    grep -E 'cycles|instructions|LLC-load-misses|page-faults|context-switches|elapsed' "$WORK/perf.txt" || true
  else
    echo "perf: not available or not permitted, skipped"
  fi
}

# ---------------------------------------------------------------- stress60
cmd_stress60() {
  need_dir "$STRESS_DIR" "run: bash scripts/verify.sh data"
  [[ -x /usr/bin/time ]] || die "/usr/bin/time (GNU time) needed for peak RSS — run on Linux/WSL"
  build_release
  local max_rss_bytes="${STRESS_MAX_RSS_BYTES:-18000000000}" max_s="${STRESS_MAX_S:-90}"
  local out="$WORK/stress_out" scratch="$WORK/stress_scratch"
  fresh_dirs "$out" "$scratch"
  local t0 t1 status=0
  t0=$(now_ns)
  DATA_DIR="$STRESS_DIR" OUTPUT_DIR="$out" SCRATCH_DIR="$scratch" HOME="$scratch" TMPDIR="$scratch" \
    /usr/bin/time -v -o "$WORK/stress.time" "$BIN" >"$WORK/stress.stdout" 2>"$WORK/stress.stderr" || status=$?
  t1=$(now_ns)
  [[ "$status" == 0 ]] || { tail -n 40 "$WORK/stress.stderr" >&2; die "stress run exited $status"; }
  check_stdout_empty "$WORK/stress.stdout" "stress_60m"
  local ms rss_kb rss_bytes
  ms=$(( (t1 - t0) / 1000000 ))
  rss_kb=$(awk -F': *' '/Maximum resident set size/ {print $2}' "$WORK/stress.time")
  [[ -n "$rss_kb" ]] || die "could not read peak RSS from /usr/bin/time"
  rss_bytes=$(( rss_kb * 1024 ))
  echo "stress60 wall: $ms ms"
  echo "stress60 peak RSS: $(( rss_kb / 1024 )) MB"
  echo "--- stderr ---"
  cat "$WORK/stress.stderr"
  (( rss_bytes <= max_rss_bytes )) || die "peak RSS $rss_bytes B > limit $max_rss_bytes B"
  (( ms <= max_s * 1000 )) || die "wall $ms ms > limit ${max_s}s"
  echo "stress60: PASS"
}

# ---------------------------------------------------------------- docker
cmd_docker() {
  need docker
  need_dir "$DEV_DIR" "run: bash scripts/verify.sh data"
  [[ -f "$REPO/Dockerfile" ]] || die "no Dockerfile in $REPO"
  info "docker buildx build --platform $PLATFORM -t $IMAGE"
  docker buildx build --platform "$PLATFORM" --load -t "$IMAGE" "$REPO" || die "docker build failed"

  local out="$WORK/docker_out" scratch="$WORK/docker_scratch"
  fresh_dirs "$out" "$scratch"
  chmod 777 "$out" "$scratch"
  local data
  data=$(cd "$DEV_DIR" && pwd)
  local t0 t1 status=0
  t0=$(now_ns)
  docker run --rm --platform "$PLATFORM" \
    --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
    --cpus=8 --memory=24g --memory-swap=24g \
    --user 1000:1000 \
    -v "$data":/data:ro,z -v "$out":/output:z -v "$scratch":/scratch:z \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
    -e NCPU=8 -e MEM_GB=24 \
    "$IMAGE" >"$WORK/docker.stdout" 2>"$WORK/docker.stderr" || status=$?
  t1=$(now_ns)
  [[ "$status" == 0 ]] || { tail -n 40 "$WORK/docker.stderr" >&2; die "container exited $status"; }
  check_stdout_empty "$WORK/docker.stdout" "docker dev_1m"
  echo "container wall (dev_1m, incl. startup): $(( (t1 - t0) / 1000000 )) ms"
  echo "--- container stderr ---"
  cat "$WORK/docker.stderr"
  if [[ -d "$DEV_ANSWER" ]] && "$PYTHON" -c 'import polars' 2>/dev/null && [[ -n "$STARTER_SCRIPTS" ]]; then
    (cd "$STARTER_SCRIPTS" && "$PYTHON" compare.py "$out" "$DEV_ANSWER") || die "container output differs from dev-answer"
  else
    echo "dev-answer diff skipped (needs polars + dev-answer)"
  fi
  echo "docker: PASS"
}

# ---------------------------------------------------------------- main
case "${1:-}" in
  correct)  cmd_correct ;;
  bench20)  cmd_bench20 ;;
  stress60) cmd_stress60 ;;
  docker)   cmd_docker ;;
  data)     cmd_data ;;
  *) echo "usage: bash scripts/verify.sh {correct|bench20|stress60|docker|data}" >&2; exit 2 ;;
esac
