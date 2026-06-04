#!/usr/bin/env bash
# F: PgBouncer single-process vs multi-worker (so_reuseport) connection scaling.
#
# Validates the plan's claim that one PgBouncer process bottlenecks a many-vCPU
# box. Postgres runs on plain disk (this is a pooler-CPU test, not storage).
# A short SELECT-only pgbench at high client count saturates the pooler; we vary
# the number of so_reuseport workers and read TPS.
set -euo pipefail
PG="$(dirname "$(command -v initdb)")"
W="$(mktemp -d "${WORKROOT:-/tmp}/gff.XXXXXX")"
RUN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/out/$(date +%Y%m%d-%H%M%S)-f"; mkdir -p "$RUN"
log(){ printf '\033[1;34m[F]\033[0m %s\n' "$*" >&2; }
cleanup(){ set +e; pkill -f "pgbouncer .*$W" 2>/dev/null
  "$PG/pg_ctl" -D "$W/pg" -m immediate stop >/dev/null 2>&1; rm -rf "$W"; }
trap cleanup EXIT INT TERM

command -v pgbouncer >/dev/null || { echo "pgbouncer not installed"; exit 1; }
# Ubuntu auto-starts a systemd pgbouncer on :6432 — stop it so our test owns the port.
sudo systemctl stop pgbouncer 2>/dev/null || true
sudo pkill -9 pgbouncer 2>/dev/null || true; sleep 1
CORES=$(nproc)
"$PG/initdb" -D "$W/pg" -A trust -U postgres --no-sync >/dev/null 2>&1
cat >> "$W/pg/postgresql.conf" <<EOF
max_connections = 600
shared_buffers = 512MB
fsync = off
listen_addresses = '127.0.0.1'
EOF
"$PG/pg_ctl" -D "$W/pg" -l "$RUN/pg.log" -o "-p 5440 -k $W" -w start >/dev/null
"$PG/pgbench" -h 127.0.0.1 -p 5440 -U postgres -i -q -s 10 postgres >/dev/null 2>&1

cat > "$W/pgb.ini" <<EOF
[databases]
* = host=127.0.0.1 port=5440 dbname=postgres
[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6432
auth_type = trust
auth_file = $W/users.txt
pool_mode = transaction
default_pool_size = 64
max_client_conn = 2000
so_reuseport = 1
logfile =
pidfile =
EOF
printf '"postgres" ""\n' > "$W/users.txt"

echo "F: PgBouncer so_reuseport scaling on $CORES vCPU (SELECT-only, -c 200)" | tee "$RUN/score.txt"
for N in 1 2 $CORES; do
  pkill -f "pgbouncer .*$W" 2>/dev/null || true; sleep 1
  # foreground processes backgrounded by the shell; so_reuseport shares :6432
  for i in $(seq 1 "$N"); do pgbouncer "$W/pgb.ini" >/dev/null 2>&1 & done
  sleep 2
  t=$("$PG/pgbench" -h 127.0.0.1 -p 6432 -U postgres -n -S -c 200 -j "$CORES" -T 20 postgres 2>/dev/null \
      | grep -oE 'tps = [0-9.]+' | tail -1 | grep -oE '[0-9.]+')
  printf "  workers=%-2s  tps=%s\n" "$N" "$t" | tee -a "$RUN/score.txt"
done