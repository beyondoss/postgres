#!/usr/bin/env bash
# D2: maintenance_io_concurrency on COLD-from-S3 reads.
#
# GlideFS fans cold block reads into parallel S3 GETs; PG's maintenance_io_concurrency
# drives VACUUM prefetch depth. We populate a parent against MinIO (real HTTP latency),
# snapshot it, then fork into a FRESH glidefs cache (so every read misses → MinIO) and
# time a cold VACUUM at m_io_c=10 vs 200. Each run gets its own fresh cache = truly cold.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GLIDEFS="${GLIDEFS:-$(command -v glidefs)}"; PG="$(dirname "$(command -v initdb)")"
API=127.0.0.1:18080; NBD=127.0.0.1:10899
W="$(mktemp -d "${WORKROOT:-/tmp}/gfd2.XXXXXX")"
RUN="$HERE/out/$(date +%Y%m%d-%H%M%S)-d2"; mkdir -p "$RUN"
SCALE="${SCALE:-100}"; MINIO_PID=""; GF_PID=""
log(){ printf '\033[1;32m[D2]\033[0m %s\n' "$*" >&2; }
gf_stop(){ [ -n "$GF_PID" ] && { sudo kill "$GF_PID" 2>/dev/null; wait "$GF_PID" 2>/dev/null; GF_PID=""; }; }
cleanup(){ set +e; "$PG/pg_ctl" -D "$W"/pg* -m immediate stop >/dev/null 2>&1
  sudo umount "$W"/m* 2>/dev/null; gf_stop
  [ -n "$MINIO_PID" ] && kill "$MINIO_PID" 2>/dev/null; sudo rm -rf "$W"; }
trap cleanup EXIT INT TERM

# --- MinIO ---
cd "$W"
[ -x ./minio ] || curl -fsSL -o minio https://dl.min.io/server/minio/release/linux-amd64/minio && chmod +x minio
[ -x ./mc ]   || curl -fsSL -o mc   https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x mc
mkdir -p "$W/minio-data"
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio server "$W/minio-data" --address 127.0.0.1:9000 >"$RUN/minio.log" 2>&1 &
MINIO_PID=$!
for _ in $(seq 1 50); do curl -fsS http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1 && break; sleep 0.2; done
./mc alias set loc http://127.0.0.1:9000 minioadmin minioadmin >/dev/null 2>&1
./mc mb loc/bench >/dev/null 2>&1 || true

gf_toml(){ # $1=cache_dir $2=export-stanza
cat <<EOF
[cache]
dir = "$1"
disk_size_gb = 30.0
memory_size_gb = 2.0
[storage]
url = "s3://bench/pg"
[aws]
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
endpoint = "http://127.0.0.1:9000"
allow_http = "true"
region = "us-east-1"
[servers.nbd]
addresses = ["$NBD"]
unix_socket = "$W/gf.sock"
api_address = "$API"
block_size = 131072
flush_threshold = 500
$2
EOF
}
gf_start(){ # $1=toml
  curl -fsS "http://$API/health" >/dev/null 2>&1 && { echo "API busy"; exit 1; }
  sudo -b sh -c "exec env NO_COLOR=1 '$GLIDEFS' run -c '$1' >>'$RUN/glidefs.log' 2>&1"
  for _ in $(seq 1 50); do GF_PID="$(pgrep -f "glidefs run -c $1"|head -1)"; [ -n "$GF_PID" ] && break; sleep 0.1; done
  for _ in $(seq 1 100); do curl -fsS "http://$API/health" >/dev/null 2>&1 && break; sleep 0.1; done; }
dev_of(){ for _ in $(seq 1 100); do d="$(curl -fsS "http://$API/api/exports/$1"|jq -r '.device // empty')"; [ -n "$d" ] && [ -b "$d" ] && { echo "$d"; return; }; sleep 0.1; done; }

# --- Parent: populate against MinIO, snapshot ---
gf_toml "$W/c1" $'[[servers.nbd.exports]]\nname = "dA"\nsize_gb = 20\ntransport = "nbd"' > "$W/t1.toml"
gf_start "$W/t1.toml"
DA="$(dev_of dA)"; log "parent dA $DA, populate scale=$SCALE"
sudo mkfs.ext4 -q -F "$DA"; sudo mkdir -p "$W/ma"; sudo mount "$DA" "$W/ma"; sudo chown -R "$USER:$(id -gn)" "$W/ma"
"$PG/initdb" -D "$W/pgA" -A trust -U postgres --no-sync >/dev/null 2>&1
echo "shared_buffers=1GB" >> "$W/pgA/postgresql.conf"
"$PG/pg_ctl" -D "$W/pgA" -l "$RUN/pgA.log" -o "-p 5440 -k $W/ma" -w start >/dev/null
"$PG/pgbench" -h "$W/ma" -p 5440 -U postgres -i -q -s "$SCALE" postgres >/dev/null 2>&1
"$PG/psql" -h "$W/ma" -p 5440 -U postgres -c "CREATE INDEX ON pgbench_accounts(abalance); CHECKPOINT;" >/dev/null
"$PG/pg_ctl" -D "$W/pgA" -m fast stop >/dev/null
curl -fsS -X POST "http://$API/api/exports/dA/drain" >/dev/null; sleep 2
SEQ="$(curl -fsS -X POST "http://$API/api/exports/dA/snapshot"|jq -r .sequence)"
log "snapshot seq=$SEQ (data now in MinIO)"; gf_stop

cold_vacuum(){ # $1=mioc
  local mioc="$1" t
  rm -rf "$W/c2"; mkdir -p "$W/c2"   # FRESH cache => cold reads hit MinIO
  gf_toml "$W/c2" "" > "$W/t2.toml"; gf_start "$W/t2.toml"
  curl -fsS -X PUT "http://$API/api/exports/dB" -H 'Content-Type: application/json' \
    -d "{\"size_gb\":20,\"transport\":\"nbd\",\"manifest_name\":\"dA\",\"snapshot_sequence\":$SEQ}" >/dev/null
  local DB; DB="$(dev_of dB)"
  sudo mkdir -p "$W/mb"; sudo mount "$DB" "$W/mb"; sudo chown -R "$USER:$(id -gn)" "$W/mb"
  cat >> "$W/mb/pgdata/postgresql.conf" <<P
shared_buffers = 256MB
maintenance_io_concurrency = $mioc
P
  "$PG/pg_ctl" -D "$W/mb/pgdata" -l "$RUN/pgB-$mioc.log" -o "-p 5441 -k $W/mb" -w start >/dev/null
  # cold maintenance: full VACUUM-scan of the big table + its index (reads from MinIO)
  t=$("$PG/psql" -h "$W/mb" -p 5441 -U postgres -At -c "\timing on" \
        -c "VACUUM (DISABLE_PAGE_SKIPPING) pgbench_accounts;" 2>&1 | grep -oE 'Time: [0-9.]+' | tail -1 | grep -oE '[0-9.]+')
  printf "  maintenance_io_concurrency=%-4s  cold VACUUM = %s ms\n" "$mioc" "$t" | tee -a "$RUN/score.txt"
  "$PG/pg_ctl" -D "$W/mb/pgdata" -m immediate stop >/dev/null 2>&1
  sudo umount "$W/mb" 2>/dev/null; gf_stop
}
echo "D2: cold-from-MinIO VACUUM, maintenance_io_concurrency sweep (scale=$SCALE)" | tee "$RUN/score.txt"
cold_vacuum 10
cold_vacuum 200
cold_vacuum 10
cold_vacuum 200