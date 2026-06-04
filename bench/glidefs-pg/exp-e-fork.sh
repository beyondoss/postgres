#!/usr/bin/env bash
# E: autovacuum-on-CoW divergence.
#
# Populate parent export A with dead tuples, snapshot it, FORK child B (shares all
# blocks, 0 divergence at t=0), then let B idle under a given autovacuum profile.
# A's Postgres is STOPPED during B's idle window, so A is quiescent → every flush
# in that window is B's divergence. Measure B's uploaded bytes = CoW divergence cost.
#
# Usage: exp-e-fork.sh <aggressive|throttled>
set -euo pipefail
PROFILE="${1:?aggressive|throttled}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GLIDEFS="${GLIDEFS:-$(command -v glidefs)}"; PG="$(dirname "$(command -v initdb)")"
API=127.0.0.1:18080; NBD=127.0.0.1:10899
W="$(mktemp -d "${WORKROOT:-/tmp}/gfe.XXXXXX")"
RUN="$HERE/out/$(date +%Y%m%d-%H%M%S)-e-$PROFILE"; mkdir -p "$RUN"
GF_LOG="$RUN/glidefs.log"; GF_PID=""; MA=""; MB=""
log(){ printf '\033[1;35m[E]\033[0m %s\n' "$*" >&2; }
cleanup(){ set +e
  [ -n "$MB" ] && { "$PG/pg_ctl" -D "$W/pgB" -m immediate stop >/dev/null 2>&1; sudo umount "$W/mb" 2>/dev/null; }
  [ -n "$MA" ] && { sudo umount "$W/ma" 2>/dev/null; }
  curl -fsS -X DELETE "http://$API/api/exports/eA?purge=true" >/dev/null 2>&1
  curl -fsS -X DELETE "http://$API/api/exports/eB?purge=true" >/dev/null 2>&1
  [ -n "$GF_PID" ] && { sudo kill "$GF_PID" 2>/dev/null; wait "$GF_PID" 2>/dev/null; }
  sudo rm -rf "$W" 2>/dev/null; }
trap cleanup EXIT INT TERM

cat > "$W/gf.toml" <<EOF
[cache]
dir = "$W/cache"
disk_size_gb = 20.0
memory_size_gb = 2.0
[storage]
url = "memory:///"
[servers.nbd]
addresses = ["$NBD"]
unix_socket = "$W/gf.sock"
api_address = "$API"
block_size = 131072
flush_threshold = 500
[[servers.nbd.exports]]
name = "eA"
size_gb = 10
transport = "nbd"
EOF
mkdir -p "$W/cache" "$W/ma" "$W/mb"
curl -fsS "http://$API/health" >/dev/null 2>&1 && { echo "API $API busy"; exit 1; }
log "start glidefs"
sudo -b sh -c "exec env NO_COLOR=1 '$GLIDEFS' run -c '$W/gf.toml' >'$GF_LOG' 2>&1"
for _ in $(seq 1 50); do GF_PID="$(pgrep -f "glidefs run -c $W/gf.toml"|head -1)"; [ -n "$GF_PID" ] && break; sleep 0.1; done
for _ in $(seq 1 100); do curl -fsS "http://$API/health" >/dev/null 2>&1 && break; sleep 0.1; done

DA="$(curl -fsS "http://$API/api/exports/eA"|jq -r .device)"
log "parent eA on $DA"
sudo mkfs.ext4 -q -F "$DA"; sudo mount "$DA" "$W/ma"; sudo chown -R "$USER:$(id -gn)" "$W/ma"
"$PG/initdb" -D "$W/pgA" -A trust -U postgres --no-sync >/dev/null 2>&1
echo "shared_buffers=512MB" >> "$W/pgA/postgresql.conf"
echo "autovacuum=off" >> "$W/pgA/postgresql.conf"   # no autovac on parent; we want dead tuples to survive into the fork
"$PG/pg_ctl" -D "$W/pgA" -l "$RUN/pgA.log" -o "-p 5440 -k $W/ma" -w start >/dev/null
PSQLA=("$PG/psql" -h "$W/ma" -p 5440 -U postgres)
log "load + churn (create dead tuples)"
"$PG/pgbench" -h "$W/ma" -p 5440 -U postgres -i -q -s 50 postgres >/dev/null 2>&1
# heavy UPDATE churn => many dead tuples for autovacuum to chase on the fork
"$PG/pgbench" -h "$W/ma" -p 5440 -U postgres -t 20000 -c 8 -j 4 postgres >/dev/null 2>&1
"${PSQLA[@]}" -c "CHECKPOINT;" >/dev/null
"$PG/pg_ctl" -D "$W/pgA" -m fast stop >/dev/null    # A quiescent from here on
curl -fsS -X POST "http://$API/api/exports/eA/drain" >/dev/null; sleep 2
SEQ="$(curl -fsS -X POST "http://$API/api/exports/eA/snapshot"|jq -r .sequence)"
log "snapshot eA seq=$SEQ; forking eB"
curl -fsS -X PUT "http://$API/api/exports/eB" -H 'Content-Type: application/json' \
  -d "{\"size_gb\":10,\"transport\":\"nbd\",\"manifest_name\":\"eA\",\"snapshot_sequence\":$SEQ}" >/dev/null
for _ in $(seq 1 100); do DB="$(curl -fsS "http://$API/api/exports/eB"|jq -r '.device // empty')"; [ -n "$DB" ] && [ -b "$DB" ] && break; sleep 0.1; done
log "fork eB on $DB"
sudo mount "$DB" "$W/mb"; sudo chown -R "$USER:$(id -gn)" "$W/mb"
# autovacuum profile under test
if [ "$PROFILE" = aggressive ]; then
  cat >> "$W/mb/pgdata/postgresql.conf" <<'P'
autovacuum = on
autovacuum_vacuum_scale_factor = 0.01
autovacuum_naptime = 10s
autovacuum_vacuum_cost_delay = 0
P
else
  cat >> "$W/mb/pgdata/postgresql.conf" <<'P'
autovacuum = on
autovacuum_vacuum_scale_factor = 0.4
autovacuum_naptime = 5min
autovacuum_vacuum_cost_delay = 20ms
autovacuum_vacuum_cost_limit = 200
P
fi
"$PG/pg_ctl" -D "$W/mb/pgdata" -l "$RUN/pgB.log" -o "-p 5441 -k $W/mb" -w start >/dev/null
MB=1
# mark window; B idles (no client workload) — only autovacuum writes
curl -fsS -X POST "http://$API/api/exports/eB/drain" >/dev/null; sleep 1
L0=$(wc -l < "$GF_LOG")
log "B idling 120s under '$PROFILE' autovacuum..."
sleep 120
"$PG/psql" -h "$W/mb" -p 5441 -U postgres -c "CHECKPOINT;" >/dev/null 2>&1
curl -fsS -X POST "http://$API/api/exports/eB/drain" >/dev/null; sleep 3
# divergence = bytes B flushed during the idle window (A is quiescent)
DIV=$(tail -n +$((L0+1)) "$GF_LOG" | sed 's/\x1b\[[0-9;]*m//g' | awk '/flush complete/{for(i=1;i<=NF;i++){n=split($i,kv,"=");if(n==2&&kv[1]=="bytes_uploaded")b+=kv[2]}}END{print b+0}')
PK=$(tail -n +$((L0+1)) "$GF_LOG" | sed 's/\x1b\[[0-9;]*m//g' | awk '/flush complete/{for(i=1;i<=NF;i++){n=split($i,kv,"=");if(n==2&&kv[1]=="packs_uploaded")p+=kv[2]}}END{print p+0}')
printf "E[%s]: fork divergence = %d bytes (%.1f MB) over 120s idle, packs=%d\n" "$PROFILE" "$DIV" "$(echo "$DIV/1048576"|bc -l)" "$PK" | tee "$RUN/score.txt"
