#!/usr/bin/env bash
# E: autovacuum write cost on GlideFS = the CoW divergence an idle fork would incur.
#
# Every page autovacuum dirties on a fork is a new block GlideFS must upload (lost
# parent sharing). We measure that directly without the fork API: load + churn to
# create dead tuples with autovacuum OFF, drain, then turn autovacuum ON under a
# given profile and idle (no client load). The S3 bytes flushed during the idle =
# what autovacuum alone wrote = the divergence cost.
#
# Usage: exp-e.sh <aggressive|throttled>
set -euo pipefail
PROFILE="${1:?aggressive|throttled}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GLIDEFS="${GLIDEFS:-$(command -v glidefs)}"; PG="$(dirname "$(command -v initdb)")"
API=127.0.0.1:18080; NBD=127.0.0.1:10899
W="$(mktemp -d "${WORKROOT:-/tmp}/gfe.XXXXXX")"
RUN="$HERE/out/$(date +%Y%m%d-%H%M%S)-e-$PROFILE"; mkdir -p "$RUN"
GF_LOG="$RUN/glidefs.log"; GF_PID=""; MNT=0
log(){ printf '\033[1;35m[E]\033[0m %s\n' "$*" >&2; }
cleanup(){ set +e; [ "$MNT" = 1 ] && { "$PG/pg_ctl" -D "$W/m/pgdata" -m immediate stop >/dev/null 2>&1; sudo umount "$W/m" 2>/dev/null; }
  curl -fsS -X DELETE "http://$API/api/exports/eA?purge=true" >/dev/null 2>&1
  [ -n "$GF_PID" ] && { sudo kill "$GF_PID" 2>/dev/null; wait "$GF_PID" 2>/dev/null; }; sudo rm -rf "$W"; }
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
unix_socket = "$W/s.sock"
api_address = "$API"
block_size = 131072
flush_threshold = 500
[[servers.nbd.exports]]
name = "eA"
size_gb = 10
transport = "nbd"
EOF
mkdir -p "$W/cache" "$W/m"
curl -fsS "http://$API/health" >/dev/null 2>&1 && { echo "API busy"; exit 1; }
log "start glidefs"
sudo -b sh -c "exec env NO_COLOR=1 '$GLIDEFS' run -c '$W/gf.toml' >'$GF_LOG' 2>&1"
for _ in $(seq 1 50); do GF_PID="$(pgrep -f "glidefs run -c $W/gf.toml"|head -1)"; [ -n "$GF_PID" ] && break; sleep 0.1; done
for _ in $(seq 1 100); do curl -fsS "http://$API/health" >/dev/null 2>&1 && break; sleep 0.1; done
DA="$(curl -fsS "http://$API/api/exports/eA"|jq -r .device)"
sudo mkfs.ext4 -q -F "$DA"; sudo mount "$DA" "$W/m"; sudo chown -R "$USER:$(id -gn)" "$W/m"
"$PG/initdb" -D "$W/m/pgdata" -A trust -U postgres --no-sync >/dev/null 2>&1
cat >> "$W/m/pgdata/postgresql.conf" <<'P'
listen_addresses = ''
shared_buffers = 512MB
autovacuum = off
P
"$PG/pg_ctl" -D "$W/m/pgdata" -l "$RUN/pg.log" -o "-p 5440 -k $W/m" -w start >/dev/null; MNT=1
PSQL=("$PG/psql" -h "$W/m" -p 5440 -U postgres)
log "load + churn (build dead tuples, autovacuum OFF)"
"$PG/pgbench" -h "$W/m" -p 5440 -U postgres -i -q -s 50 postgres >/dev/null 2>&1
"$PG/pgbench" -h "$W/m" -p 5440 -U postgres -t 12000 -c 8 -j 4 postgres >/dev/null 2>&1
"${PSQL[@]}" -c "CHECKPOINT;" >/dev/null
DEAD=$("${PSQL[@]}" -At -c "SELECT sum(n_dead_tup) FROM pg_stat_user_tables;")
curl -fsS -X POST "http://$API/api/exports/eA/drain" >/dev/null; sleep 2
# now turn autovacuum ON under the profile (append to conf; last value wins), reload
if [ "$PROFILE" = aggressive ]; then
  cat >> "$W/m/pgdata/postgresql.conf" <<'P'
autovacuum = on
autovacuum_vacuum_scale_factor = 0.01
autovacuum_naptime = 10s
autovacuum_vacuum_cost_delay = 0
P
else
  cat >> "$W/m/pgdata/postgresql.conf" <<'P'
autovacuum = on
autovacuum_vacuum_scale_factor = 0.4
autovacuum_naptime = 5min
autovacuum_vacuum_cost_delay = 20ms
autovacuum_vacuum_cost_limit = 200
P
fi
"${PSQL[@]}" -c "SELECT pg_reload_conf();" >/dev/null
L0=$(wc -l < "$GF_LOG")
log "idle 90s under '$PROFILE' autovacuum (dead_tuples=$DEAD)"
sleep 90
"${PSQL[@]}" -c "CHECKPOINT;" >/dev/null
curl -fsS -X POST "http://$API/api/exports/eA/drain" >/dev/null; sleep 3
DIV=$(tail -n +$((L0+1)) "$GF_LOG" | sed 's/\x1b\[[0-9;]*m//g' | awk '/flush complete/{for(i=1;i<=NF;i++){n=split($i,kv,"=");if(n==2&&kv[1]=="bytes_uploaded")b+=kv[2]}}END{print b+0}')
PK=$(tail -n +$((L0+1)) "$GF_LOG" | sed 's/\x1b\[[0-9;]*m//g' | awk '/flush complete/{for(i=1;i<=NF;i++){n=split($i,kv,"=");if(n==2&&kv[1]=="packs_uploaded")p+=kv[2]}}END{print p+0}')
REMAIN=$("${PSQL[@]}" -At -c "SELECT sum(n_dead_tup) FROM pg_stat_user_tables;")
printf "E[%s]: autovacuum wrote %d bytes (%.1f MB) in 120s idle; packs=%d; dead_tuples %s -> %s\n" \
  "$PROFILE" "$DIV" "$(echo "$DIV/1048576"|bc -l)" "$PK" "$DEAD" "$REMAIN" | tee "$RUN/score.txt"
