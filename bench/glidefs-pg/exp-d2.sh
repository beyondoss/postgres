#!/usr/bin/env bash
# D2: maintenance_io_concurrency on COLD-from-S3 reads (no fork API needed).
#
# Populate an export against MinIO (real HTTP latency), drain, then RESTART glidefs
# with a FRESH cache — the data persists in MinIO, the empty cache forces every read
# to miss → MinIO. Time a cold VACUUM at m_io_c=10 vs 200. GlideFS fans cold reads
# into parallel S3 GETs; high m_io_c drives PG's prefetch depth, hiding latency.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GLIDEFS="${GLIDEFS:-$(command -v glidefs)}"; PG="$(dirname "$(command -v initdb)")"
API=127.0.0.1:18080; NBD=127.0.0.1:10899
W="$(mktemp -d "${WORKROOT:-/tmp}/gfd2.XXXXXX")"
RUN="$HERE/out/$(date +%Y%m%d-%H%M%S)-d2"; mkdir -p "$RUN"
SCALE="${SCALE:-20}"; MINIO_PID=""; GF_PID=""
log(){ printf '\033[1;32m[D2]\033[0m %s\n' "$*" >&2; }
# NB: glidefs is started via `sudo -b` so it's NOT a child of this shell — `wait`
# on it returns 127 and trips set -e. Poll for death instead.
gf_stop(){ [ -n "$GF_PID" ] || return 0; sudo kill "$GF_PID" 2>/dev/null || true
  for _ in $(seq 1 50); do kill -0 "$GF_PID" 2>/dev/null || break; sleep 0.1; done; GF_PID=""; }
cleanup(){ set +e; "$PG/pg_ctl" -D "$W/m/pgdata" -m immediate stop >/dev/null 2>&1
  sudo umount "$W/m" 2>/dev/null; gf_stop; [ -n "$MINIO_PID" ] && kill "$MINIO_PID" 2>/dev/null; sudo rm -rf "$W"; }
trap cleanup EXIT INT TERM

cd "$W"
log "start MinIO (cached binaries)"
[ -x /tmp/minio ] || { curl -fsSL -o /tmp/minio https://dl.min.io/server/minio/release/linux-amd64/minio && chmod +x /tmp/minio; }
[ -x /tmp/mc ]    || { curl -fsSL -o /tmp/mc https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x /tmp/mc; }
ln -sf /tmp/minio minio; ln -sf /tmp/mc mc
mkdir -p "$W/minio-data" "$W/m"
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio server "$W/minio-data" --address 127.0.0.1:9000 >"$RUN/minio.log" 2>&1 &
MINIO_PID=$!
for _ in $(seq 1 60); do curl -fsS http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1 && break; sleep 0.2; done
./mc alias set loc http://127.0.0.1:9000 minioadmin minioadmin >/dev/null 2>&1; ./mc mb loc/bench >/dev/null 2>&1 || true

cat > "$W/gf.toml" <<EOF
[cache]
dir = "$W/cache"
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
unix_socket = "$W/s.sock"
api_address = "$API"
block_size = 131072
flush_threshold = 500
[[servers.nbd.exports]]
name = "dA"
size_gb = 20
transport = "nbd"
EOF
gf_start(){ curl -fsS "http://$API/health" >/dev/null 2>&1 && { echo "API busy"; exit 1; }
  sudo -b sh -c "exec env NO_COLOR=1 '$GLIDEFS' run -c '$W/gf.toml' >>'$RUN/glidefs.log' 2>&1"
  for _ in $(seq 1 50); do GF_PID="$(pgrep -f "glidefs run -c $W/gf.toml"|head -1)"; [ -n "$GF_PID" ] && break; sleep 0.1; done
  for _ in $(seq 1 150); do curl -fsS "http://$API/health" >/dev/null 2>&1 && break; sleep 0.1; done; }
dev_of(){ for _ in $(seq 1 150); do d="$(curl -fsS "http://$API/api/exports/dA"|jq -r '.device // empty' 2>/dev/null)"; [ -n "$d" ] && [ -b "$d" ] && { echo "$d"; return; }; sleep 0.1; done; }

log "populate dA scale=$SCALE against MinIO"
gf_start; DA="$(dev_of)"
sudo mkfs.ext4 -q -F "$DA"; sudo mount "$DA" "$W/m"; sudo chown -R "$USER:$(id -gn)" "$W/m"
"$PG/initdb" -D "$W/m/pgdata" -A trust -U postgres --no-sync >/dev/null 2>&1
printf "listen_addresses = ''\nshared_buffers = 1GB\n" >> "$W/m/pgdata/postgresql.conf"
"$PG/pg_ctl" -D "$W/m/pgdata" -l "$RUN/pgload.log" -o "-p 5440 -k $W/m" -w start >/dev/null
"$PG/pgbench" -h "$W/m" -p 5440 -U postgres -i -q -s "$SCALE" postgres >/dev/null 2>&1
"$PG/psql" -h "$W/m" -p 5440 -U postgres -c "CREATE INDEX ON pgbench_accounts(abalance); CHECKPOINT;" >/dev/null
"$PG/pg_ctl" -D "$W/m/pgdata" -m fast stop >/dev/null
sudo umount "$W/m"
curl -fsS -X POST "http://$API/api/exports/dA/drain" >/dev/null; sleep 2; gf_stop
log "data persisted to MinIO; restarting cold per m_io_c"

cold_vacuum(){ local mioc="$1" t
  sudo rm -rf "$W/cache"; mkdir -p "$W/cache"   # FRESH cache => cold reads hit MinIO (root-owned)
  gf_start; local DB; DB="$(dev_of)"
  sudo mount "$DB" "$W/m"; sudo chown -R "$USER:$(id -gn)" "$W/m"
  # set the knob (sed replaces any prior line)
  sed -i '/maintenance_io_concurrency/d;/shared_buffers/d;/listen_addresses/d' "$W/m/pgdata/postgresql.conf"
  printf "listen_addresses = ''\nshared_buffers = 256MB\nmaintenance_io_concurrency = %s\n" "$mioc" >> "$W/m/pgdata/postgresql.conf"
  "$PG/pg_ctl" -D "$W/m/pgdata" -l "$RUN/pg-$mioc.log" -o "-p 5441 -k $W/m" -w start >/dev/null
  t=$("$PG/psql" -h "$W/m" -p 5441 -U postgres -At -c "\timing on" -c "VACUUM (DISABLE_PAGE_SKIPPING) pgbench_accounts;" 2>&1 | grep -oE 'Time: [0-9.]+' | tail -1 | grep -oE '[0-9.]+')
  printf "  maintenance_io_concurrency=%-4s  cold VACUUM = %s ms\n" "$mioc" "$t" | tee -a "$RUN/score.txt"
  "$PG/pg_ctl" -D "$W/m/pgdata" -m immediate stop >/dev/null 2>&1; sudo umount "$W/m" 2>/dev/null; gf_stop
}
echo "D2: cold-from-MinIO VACUUM, maintenance_io_concurrency (scale=$SCALE)" | tee "$RUN/score.txt"
cold_vacuum 10; cold_vacuum 200
