#!/usr/bin/env bash
# DECISIVE TEST: does so_reuseport multi-worker actually scale pooler throughput?
# Pooler-bound load (Ed25519 TLS + per-txn churn => ~400us/conn handshake, PG ~idle),
# so the limit is pooler CPU, not PG. Pin N poolers to N cores, drive from the rest,
# report tps AND aggregate pooler CPU (cores) for N=1,2,3.
#   linear tps + pooler_cpu≈N  => multi-worker scales => autoscaler is a real win
#   flat                       => it doesn't, and we kill the idea for good
set -uo pipefail
PG=/usr/lib/postgresql/18/bin
W=$(mktemp -d /mnt/scratch/mw.XXXXX); cd "$W"
DUR=${DUR:-15}; CL=${CL:-300}
cleanup(){ pkill -f "pgbouncer $W" 2>/dev/null; "$PG/pg_ctl" -D "$W/pg" -m immediate stop >/dev/null 2>&1; rm -rf "$W"; }
trap cleanup EXIT
sudo systemctl stop pgbouncer 2>/dev/null || true; sudo pkill -9 pgbouncer 2>/dev/null || true; sleep 1

"$PG/initdb" -D "$W/pg" -A trust -U postgres --no-sync >/dev/null 2>&1
cat >> "$W/pg/postgresql.conf" <<EOF
listen_addresses = '127.0.0.1'
port = 5440
unix_socket_directories = '$W'
max_connections = 2000
shared_buffers = 512MB
fsync = off
EOF
"$PG/pg_ctl" -D "$W/pg" -l "$W/pg.log" -w start >/dev/null
"$PG/pgbench" -h 127.0.0.1 -p 5440 -U postgres -i -q -s 5 postgres >/dev/null 2>&1
openssl req -x509 -newkey ed25519 -days 1 -nodes -keyout "$W/s.key" -out "$W/s.crt" -subj /CN=pgb >/dev/null 2>&1
chmod 600 "$W/s.key"
printf '"postgres" ""\n' > "$W/users.txt"
cat > "$W/pgb.ini" <<EOF
[databases]
* = host=127.0.0.1 port=5440 dbname=postgres
[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6432
unix_socket_dir =
auth_type = trust
auth_file = $W/users.txt
pool_mode = transaction
default_pool_size = 64
max_client_conn = 4000
so_reuseport = 1
client_tls_sslmode = require
client_tls_cert_file = $W/s.crt
client_tls_key_file = $W/s.key
logfile =
pidfile =
EOF

agg_ticks(){ local s=0 v; for p in $(pgrep -f "pgbouncer $W"); do v=$(awk '{print $14+$15}' "/proc/$p/stat" 2>/dev/null||echo 0); s=$((s+v)); done; echo "$s"; }

echo "=== so_reuseport multi-worker scaling | Ed25519 TLS + churn(-C) | -c$CL ${DUR}s ==="
for N in 1 2 3; do
  pkill -f "pgbouncer $W" 2>/dev/null; sleep 1
  for i in $(seq 0 $((N-1))); do taskset -c "$i" pgbouncer "$W/pgb.ini" >/dev/null 2>&1 & done
  sleep 2
  live=$(pgrep -cf "pgbouncer $W")
  a0=$(agg_ticks); t0=$(date +%s.%N)
  out=$(PGSSLMODE=require taskset -c "${N}-5" "$PG/pgbench" -h 127.0.0.1 -p 6432 -U postgres \
        -n -S -C -c "$CL" -j "$((6-N))" -T "$DUR" postgres 2>&1)
  t1=$(date +%s.%N); a1=$(agg_ticks)
  tps=$(echo "$out" | grep -oE 'tps = [0-9.]+' | tail -1 | grep -oE '[0-9.]+$')
  cores=$(awk -v a="$a0" -v b="$a1" -v x="$t0" -v y="$t1" 'BEGIN{printf "%.2f",(b-a)/100/(y-x)}')
  printf "N=%d (live=%s)  tps=%-9s  pooler_cpu=%-5s cores  per-pooler=%.2f\n" \
    "$N" "$live" "${tps:-FAIL}" "$cores" "$(awk -v c="$cores" -v n="$N" 'BEGIN{print c/n}')"
  pkill -f "pgbouncer $W" 2>/dev/null
done
