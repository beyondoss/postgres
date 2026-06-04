#!/usr/bin/env bash
# Drive every remaining substrate experiment with HARD GUARDRAILS:
#   - per-run timeout (a bad config dies in minutes, never hangs for hours)
#   - forced guest cleanup BEFORE every run (a wedged prior run can't poison the next)
#   - each result pulled to host disk immediately (incident-proof)
# Requires qemu-bringup.sh to have printed READY.
set -uo pipefail
VMDIR="${VMDIR:-/var/tmp/pgtune-qemu}"
OUT="${OUT:-/home/jared/pgtune-results}"; mkdir -p "$OUT"
SSH="ssh -i $VMDIR/id -p ${SSHP:-2222} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 bench@127.0.0.1"
GENV='export PATH=/usr/lib/postgresql/18/bin:$PATH WORKROOT=/mnt/scratch; cd /home/bench/glidefs-pg'

guest_clean(){
  timeout 60 $SSH 'sudo pkill -9 -f "glidefs run" 2>/dev/null; sudo pkill -9 -f "run.sh|exp-e-fork|exp-f-pgb|exp-d2|minio" 2>/dev/null; sudo pkill -9 pgbench 2>/dev/null; sudo pkill -9 postgres 2>/dev/null; sleep 2; for e in pgdata eA eB dA dB; do curl -fsS -X DELETE "http://127.0.0.1:18080/api/exports/$e?purge=true" >/dev/null 2>&1; done; sudo umount /mnt/scratch/gf*/m* 2>/dev/null; sudo umount /mnt/scratch/gf*/mnt 2>/dev/null; sudo rm -rf /mnt/scratch/gf* 2>/dev/null; true' >/dev/null 2>&1
}

# drive <label> <timeout_s> <remote-cmd...>
drive(){ local label="$1" tmo="$2"; shift 2
  echo "### $label (timeout ${tmo}s) ###"
  guest_clean
  # double timeout: remote `timeout` + outer ssh `timeout` so a hung ssh still returns
  timeout $((tmo+45)) $SSH "$GENV && timeout ${tmo} $*" 2>&1 | tail -4
  $SSH "cat \$(ls -dt /home/bench/glidefs-pg/out/*/ 2>/dev/null | head -1)score.txt" > "$OUT/${label}.txt" 2>/dev/null
  if [ -s "$OUT/${label}.txt" ]; then echo "  ✓ saved $OUT/${label}.txt"; sed 's/^/    /' "$OUT/${label}.txt"; else echo "  ✗ NO RESULT (timed out or failed)"; fi
}

echo "===== C1 backfill: cp30s (PG floor) ====="
drive "c1-cp30s" 480 "./run.sh --label c1-cp30s --backend memory --scale 50 --txns 300000 --conf conf/c1-cp30s.conf"

echo "===== C3 bgwriter (FIXED: scale20 fits in 1GB sb, 200k txns) ====="
for v in aggressive gentle off; do
  drive "c3-$v" 480 "./run.sh --label c3-$v --backend memory --scale 20 --txns 200000 --conf conf/c3-$v.conf"
done

echo "===== G cooldown churn (scale10 hot-overwrite, 400k txns) ====="
drive "g-cool0" 600 "./run.sh --label g-cool0 --backend memory --scale 10 --txns 400000 --cooldown 0"
drive "g-cool8" 600 "./run.sh --label g-cool8 --backend memory --scale 10 --txns 400000 --cooldown 8"

echo "===== E autovacuum-on-CoW divergence (snapshot->fork->idle) ====="
drive "e-aggressive" 480 "./exp-e-fork.sh aggressive"
drive "e-throttled"  480 "./exp-e-fork.sh throttled"

echo "===== F pgbouncer multi-worker (so_reuseport 1 vs N) ====="
drive "f-pgbouncer" 300 "./exp-f-pgbouncer.sh"

echo "===== D2 cold-from-MinIO VACUUM (maintenance_io_concurrency) ====="
drive "d2-coldfork" 1500 "./exp-d2-coldfork.sh"

guest_clean
echo "ALL_EXPERIMENTS_DONE — results in $OUT"
