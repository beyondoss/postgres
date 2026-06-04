#!/usr/bin/env bash
# GlideFS × Postgres substrate-tuning benchmark harness.
#
# Stands up a real GlideFS block device (against an in-memory / local-file /
# MinIO object store — never real S3), puts Postgres' data dir on it, runs a
# fixed pgbench workload, and scrapes GlideFS's own per-export metrics before
# and after so each tuning knob can be scored on S3 write cost / coalescing.
#
# This is the measure-first rig from plans/wal-recycle-off-*.md. Every knob in
# that plan is validated here before any default changes.
#
#   sudo is used for: glidefs (needs CAP_SYS_ADMIN to create /dev/nbdN via
#   netlink), mkfs.ext4, mount/umount. Postgres itself runs as $USER.
#
# Usage:
#   bench/glidefs-pg/run.sh [--conf FILE] [--label NAME] [--backend memory|file]
#                           [--scale N] [--duration SECS] [--clients N]
#                           [--cooldown N] [--transport nbd|ublk] [--keep]
#
# Output: a timestamped dir under bench/glidefs-pg/out/ with the full before/
# after Prometheus snapshots, per-export JSON, pgbench log, and a delta table.
set -euo pipefail

# --------------------------------------------------------------------------
# Config (env-overridable; flags below take precedence)
# --------------------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF="${CONF:-$HERE/conf/baseline.conf}"
LABEL="${LABEL:-baseline}"
BACKEND="${BACKEND:-file}"          # file:// (default, no OOM) | memory:// | minio
SCALE="${SCALE:-25}"                # pgbench -i -s  (~375 MB at 25)
DURATION="${DURATION:-120}"         # pgbench -T seconds (time-based; default)
TXNS="${TXNS:-}"                     # if set: fixed TOTAL transactions (work-normalized) — beats -T for A/B
CLIENTS="${CLIENTS:-8}"
JOBS="${JOBS:-4}"
COOLDOWN="${COOLDOWN:-0}"           # GlideFS compaction_cooldown (G experiment)
TRANSPORT="${TRANSPORT:-nbd}"       # nbd (default, no extra cfg) | ublk
EXPORT_NAME="${EXPORT_NAME:-pgdata}"
SIZE_GB="${SIZE_GB:-10}"
BLOCK_SIZE="${BLOCK_SIZE:-131072}"
# Distinct from the homelab's production glidefs (api :9113). Stay off common ports.
API="${API:-127.0.0.1:18080}"
NBD_ADDR="${NBD_ADDR:-127.0.0.1:10899}"
KEEP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --conf) CONF="$2"; shift 2;;
    --label) LABEL="$2"; shift 2;;
    --backend) BACKEND="$2"; shift 2;;
    --scale) SCALE="$2"; shift 2;;
    --duration) DURATION="$2"; shift 2;;
    --txns) TXNS="$2"; shift 2;;
    --clients) CLIENTS="$2"; shift 2;;
    --cooldown) COOLDOWN="$2"; shift 2;;
    --transport) TRANSPORT="$2"; shift 2;;
    --keep) KEEP=1; shift;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

# --------------------------------------------------------------------------
# Paths / tools
# --------------------------------------------------------------------------
GLIDEFS="${GLIDEFS:-$(command -v glidefs || echo /usr/local/bin/glidefs)}"
PG_BINDIR="$(dirname "$(command -v initdb)")"
TS="$(date +%Y%m%d-%H%M%S)"
RUN="$HERE/out/${TS}-${LABEL}"
WORK="$(mktemp -d "${WORKROOT:-/tmp}/gfpg.XXXXXX")"
CACHE_DIR="$WORK/cache"
OBJ_DIR="$WORK/objstore"
MNT="$WORK/mnt"
PGDATA="$MNT/pgdata"
SOCK="$WORK/gf.nbd.sock"
TOML="$WORK/glidefs.toml"
GF_LOG="$RUN/glidefs.log"   # in the persistent run dir so it survives cleanup
mkdir -p "$RUN" "$CACHE_DIR" "$OBJ_DIR" "$MNT"

GF_PID=""
DEVICE=""
PG_UP=0
MOUNTED=0

log() { printf '\033[1;36m[harness]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[harness] FATAL:\033[0m %s\n' "$*" >&2; exit 1; }

# --------------------------------------------------------------------------
# Cleanup (reverse order; each step guarded)
# --------------------------------------------------------------------------
cleanup() {
  set +e
  [[ "$PG_UP" == 1 ]] && "$PG_BINDIR/pg_ctl" -D "$PGDATA" -m immediate stop >/dev/null 2>&1
  [[ "$MOUNTED" == 1 ]] && sudo umount "$MNT" >/dev/null 2>&1
  # Release the export -> unregisters the kernel device.
  curl -fsS -X DELETE "http://$API/api/exports/$EXPORT_NAME?purge=true" >/dev/null 2>&1
  if [[ -n "$GF_PID" ]]; then sudo kill "$GF_PID" >/dev/null 2>&1; wait "$GF_PID" 2>/dev/null; fi
  if [[ "$KEEP" == 1 ]]; then
    log "kept work dir: $WORK"
  else
    sudo rm -rf "$WORK" >/dev/null 2>&1
  fi
}
trap cleanup EXIT INT TERM

# --------------------------------------------------------------------------
# 1. Render glidefs.toml for the chosen backend
# --------------------------------------------------------------------------
case "$BACKEND" in
  memory) STORE_URL="memory:///" ;;
  file)   STORE_URL="file://$OBJ_DIR" ;;
  minio)  STORE_URL="${MINIO_URL:?set MINIO_URL=s3://bucket/prefix and AWS_* + AWS_ENDPOINT_URL}" ;;
  *) die "unknown backend: $BACKEND" ;;
esac

cat > "$TOML" <<EOF
[cache]
dir = "$CACHE_DIR"
disk_size_gb = 20.0
memory_size_gb = 2.0

[storage]
url = "$STORE_URL"

[servers.nbd]
addresses = ["$NBD_ADDR"]
unix_socket = "$SOCK"
api_address = "$API"
block_size = $BLOCK_SIZE
flush_threshold = 500

[[servers.nbd.exports]]
name = "$EXPORT_NAME"
size_gb = $SIZE_GB
transport = "$TRANSPORT"
compaction_cooldown = $COOLDOWN
EOF
if [[ "$TRANSPORT" == "ublk" ]]; then
  cat >> "$TOML" <<EOF

[servers.ublk]
nr_queues = 4
EOF
fi
cp "$TOML" "$RUN/glidefs.toml"

# --------------------------------------------------------------------------
# 2. Start glidefs (root; background)
# --------------------------------------------------------------------------
# Safety: never attach to a pre-existing server (e.g. the homelab's production
# glidefs). The API port must be free before we start our own instance.
if curl -fsS "http://$API/health" >/dev/null 2>&1; then
  die "something is already serving $API — refusing to run (set API=host:port to a free port)"
fi
log "starting glidefs (backend=$BACKEND transport=$TRANSPORT cooldown=$COOLDOWN)"
sudo -b sh -c "exec env NO_COLOR=1 '$GLIDEFS' run -c '$TOML' >'$GF_LOG' 2>&1"
# find the glidefs pid (sudo -b backgrounds a subshell; locate the real process)
for _ in $(seq 1 50); do
  GF_PID="$(pgrep -f "glidefs run -c $TOML" | head -1 || true)"
  [[ -n "$GF_PID" ]] && break
  sleep 0.1
done
[[ -n "$GF_PID" ]] || { cat "$GF_LOG" >&2; die "glidefs failed to start"; }

# wait for API
for _ in $(seq 1 100); do
  curl -fsS "http://$API/health" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -fsS "http://$API/health" >/dev/null 2>&1 || { cat "$GF_LOG" >&2; die "glidefs API not ready"; }

# --------------------------------------------------------------------------
# 3. Attach device (export is defined in the config, registered at startup;
#    PUT is a harmless idempotent confirm that also covers older builds)
# --------------------------------------------------------------------------
log "ensuring export $EXPORT_NAME (${SIZE_GB}GB)"
curl -fsS -X PUT "http://$API/api/exports/$EXPORT_NAME" \
  -H 'Content-Type: application/json' \
  -d "{\"size_gb\":$SIZE_GB,\"transport\":\"$TRANSPORT\",\"block_size\":$BLOCK_SIZE,\"compaction_cooldown\":$COOLDOWN}" \
  >/dev/null 2>&1 || true

for _ in $(seq 1 100); do
  DEVICE="$(curl -fsS "http://$API/api/exports/$EXPORT_NAME" | jq -r '.device // empty')"
  [[ -n "$DEVICE" && -b "$DEVICE" ]] && break
  sleep 0.1
done
[[ -n "$DEVICE" && -b "$DEVICE" ]] || { cat "$GF_LOG" >&2; die "no block device for export"; }
log "device: $DEVICE"

# --------------------------------------------------------------------------
# 4. ext4 + mount + hand to $USER
# --------------------------------------------------------------------------
log "mkfs.ext4 + mount"
sudo mkfs.ext4 -q -F "$DEVICE"
sudo mount "$DEVICE" "$MNT"; MOUNTED=1
sudo chown -R "$USER:$(id -gn)" "$MNT"

# --------------------------------------------------------------------------
# 5. initdb + apply baseline + candidate conf + start
# --------------------------------------------------------------------------
log "initdb"
"$PG_BINDIR/initdb" -D "$PGDATA" -A trust -U postgres --no-sync >/dev/null 2>&1 \
  || die "initdb failed"
# Layer the harness baseline + the candidate conf under test.
{ echo "include_if_exists = 'harness.conf'"; } >> "$PGDATA/postgresql.conf"
cat "$HERE/conf/baseline.conf" > "$PGDATA/harness.conf"
if [[ "$CONF" != "$HERE/conf/baseline.conf" ]]; then
  echo "# ---- candidate overlay: $(basename "$CONF") ----" >> "$PGDATA/harness.conf"
  cat "$CONF" >> "$PGDATA/harness.conf"
fi
cp "$PGDATA/harness.conf" "$RUN/harness.conf"

"$PG_BINDIR/pg_ctl" -D "$PGDATA" -l "$RUN/postgres.log" \
  -o "-p 5440 -k $MNT" -w start >/dev/null 2>&1 || { cat "$RUN/postgres.log" >&2; die "pg start failed"; }
PG_UP=1
PSQL=("$PG_BINDIR/psql" -h "$MNT" -p 5440 -U postgres)
PGBENCH=("$PG_BINDIR/pgbench" -h "$MNT" -p 5440 -U postgres)

# --------------------------------------------------------------------------
# 6. Workload + flush accounting
#
# GlideFS's Prometheus s3_* counters are NOT wired in this build (they read 0),
# but the flush log is authoritative: each "flush complete" line carries
# packs_uploaded / bytes_uploaded / blocks_claimed / blocks_cross_deduped. We
# force a drain (synchronous flush of all dirty blocks) so the whole run's
# writes are accounted, then sum the flush events in the run window.
# guest_bytes_written_total / guest_write_ops_total ARE wired → write-amp + coalescing.
# --------------------------------------------------------------------------
getm() { curl -fsS "http://$API/metrics" 2>/dev/null | sed -n "s/^glidefs_$1{[^}]*} //p" | head -1; }
drain() { curl -fsS -X POST "http://$API/api/exports/$EXPORT_NAME/drain" >/dev/null 2>&1 || true; }

log "pgbench init (scale=$SCALE)"
"${PGBENCH[@]}" -i -q -s "$SCALE" postgres >/dev/null 2>&1 || die "pgbench init failed"
"${PSQL[@]}" -c "CHECKPOINT;" >/dev/null 2>&1; drain; sleep 2

# baseline markers (after the init flush is drained)
GB0=$(getm guest_bytes_written_total); GO0=$(getm guest_write_ops_total)
L0=$(wc -l < "$GF_LOG")

# Fixed-transaction (work-normalized) beats time-based for A/B: every config does
# the SAME work, so S3-byte deltas reflect efficiency, not throughput.
if [[ -n "$TXNS" ]]; then WL=(-t "$(( TXNS / CLIENTS ))"); WLDESC="t=$TXNS"; else WL=(-T "$DURATION"); WLDESC="T=${DURATION}s"; fi
log "pgbench run ($WLDESC c=$CLIENTS j=$JOBS)"
"${PGBENCH[@]}" "${WL[@]}" -c "$CLIENTS" -j "$JOBS" -P 15 postgres \
  > "$RUN/pgbench.log" 2>&1 || { cat "$RUN/pgbench.log" >&2; die "pgbench run failed"; }
"${PSQL[@]}" -c "CHECKPOINT;" >/dev/null 2>&1; drain; sleep 3

GB1=$(getm guest_bytes_written_total); GO1=$(getm guest_write_ops_total)
curl -fsS "http://$API/metrics" 2>/dev/null | grep -F "$EXPORT_NAME" > "$RUN/metrics.prom" || true

# --------------------------------------------------------------------------
# 7. Scoreboard — sum the run-window flush events from the glidefs log
# --------------------------------------------------------------------------
log "computing scoreboard"
tps="$(grep -Eo 'tps = [0-9.]+' "$RUN/pgbench.log" | tail -1 | grep -Eo '[0-9.]+$' || echo 0)"
tail -n +$((L0+1)) "$GF_LOG" | sed 's/\x1b\[[0-9;]*m//g' | awk \
  -v gb="$(( ${GB1:-0} - ${GB0:-0} ))" -v go="$(( ${GO1:-0} - ${GO0:-0} ))" -v tps="$tps" \
  -v lbl="$LABEL" -v conf="$(basename "$CONF")" -v scale="$SCALE" -v dur="$DURATION" \
  -v cd="$COOLDOWN" -v be="$BACKEND" '
  /flush complete/ {
    for (i=1;i<=NF;i++) { n=split($i,kv,"=");
      if (n==2) {
        if (kv[1]=="packs_uploaded") packs+=kv[2];
        else if (kv[1]=="bytes_uploaded") bytes+=kv[2];
        else if (kv[1]=="blocks_claimed") blocks+=kv[2];
        else if (kv[1]=="blocks_cross_deduped") xd+=kv[2];
      } }
    flushes++;
  }
  END {
    wamp = (gb>0)? bytes/gb : 0;
    coal = (packs>0)? go/packs : 0;
    bpp  = (packs>0)? blocks/packs : 0;
    printf "================ %s ================\n", lbl;
    printf "  conf=%s backend=%s scale=%s dur=%ss cooldown=%s\n", conf, be, scale, dur, cd;
    printf "  %-26s %15d\n", "S3 write bytes",       bytes;
    printf "  %-26s %15d\n", "S3 packs (PUTs)",      packs;
    printf "  %-26s %15d\n", "flush cycles",         flushes;
    printf "  %-26s %15d\n", "blocks flushed",       blocks;
    printf "  %-26s %15d\n", "blocks cross-deduped", xd;
    printf "  %-26s %15d\n", "guest bytes written",  gb;
    printf "  %-26s %15d\n", "guest write ops",      go;
    printf "  %-26s %15.3f  (S3 bytes / guest bytes; LOWER better)\n", "write-amp", wamp;
    printf "  %-26s %15.1f  (blocks / pack)\n", "pack fill", bpp;
    printf "  %-26s %15s\n", "pgbench tps", tps;
  }' | tee "$RUN/score.txt"
log "results: $RUN/score.txt"
