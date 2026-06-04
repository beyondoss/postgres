#!/usr/bin/env bash
# Pin ONE pgbouncer to its own core and find where it saturates, to pin X (pooler
# CPU per txn) under the conditions that matter for Beyond: TLS termination x
# connection churn. SELECT-only so PG is never the bottleneck. Measures tps AND the
# pooler's actual CPU (fraction of a core) so we KNOW it's the limit, not pgbench/PG.
#
# pgbouncer -> core 0 (alone); postgres + pgbench -> cores 1-5.
set -uo pipefail
PG=/usr/lib/postgresql/18/bin
W=$(mktemp -d /mnt/scratch/pool.XXXXX); cd "$W"
DUR="${DUR:-20}"; CL="${CL:-100}"
cleanup(){ pkill -f "pgbouncer $W" 2>/dev/null; "$PG/pg_ctl" -D "$W/pg" -m immediate stop >/dev/null 2>&1; rm -rf "$W"; }
trap cleanup EXIT

# --- postgres on plain disk, pinned off core 0 ---
"$PG/initdb" -D "$W/pg" -A trust -U postgres --no-sync >/dev/null 2>&1
cat >> "$W/pg/postgresql.conf" <<EOF
listen_addresses = '127.0.0.1'
port = 5440
unix_socket_directories = '$W'
max_connections = 800
shared_buffers = 1GB
fsync = off
EOF
taskset -c 1-5 "$PG/pg_ctl" -D "$W/pg" -l "$W/pg.log" -w start >/dev/null
taskset -c 1-5 "$PG/pgbench" -h 127.0.0.1 -p 5440 -U postgres -i -q -s 10 postgres >/dev/null 2>&1

# --- self-signed cert for pgbouncer TLS termination ---
# CERT=ed25519 matches what beyond-pg actually issues (rcgen PKCS_ED25519, src/tls.rs);
# CERT=rsa is the pessimistic RSA-2048 baseline. Ed25519 handshakes are ~10x cheaper.
case "${CERT:-ed25519}" in
  rsa) openssl req -x509 -newkey rsa:2048 -days 1 -nodes -keyout "$W/s.key" -out "$W/s.crt" -subj "/CN=pgb" >/dev/null 2>&1 ;;
  *)   openssl req -x509 -newkey ed25519  -days 1 -nodes -keyout "$W/s.key" -out "$W/s.crt" -subj "/CN=pgb" >/dev/null 2>&1 ;;
esac
chmod 600 "$W/s.key"
echo "cert: ${CERT:-ed25519}"
printf '"postgres" ""\n' > "$W/users.txt"

mk_ini(){ # $1 = tls 0/1
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
max_client_conn = 4000
logfile =
pidfile =
EOF
if [ "$1" = 1 ]; then cat >> "$W/pgb.ini" <<EOF
client_tls_sslmode = require
client_tls_cert_file = $W/s.crt
client_tls_key_file = $W/s.key
EOF
fi
}
ticks(){ awk '{print $14+$15}' "/proc/$1/stat" 2>/dev/null; }

run(){ # $1 label  $2 tls  $3 pgbench-extra  $4 sslmode
  pkill -f "pgbouncer $W" 2>/dev/null; sleep 1
  mk_ini "$2"
  taskset -c 0 pgbouncer "$W/pgb.ini" >/dev/null 2>&1 &
  sleep 2
  local pid; pid=$(pgrep -f "pgbouncer $W" | head -1)
  [ -z "$pid" ] && { printf "%-24s START-FAIL\n" "$1"; return; }
  local c0 t0 c1 t1 out tps cores us
  c0=$(ticks "$pid"); t0=$(date +%s.%N)
  out=$(PGSSLMODE="$4" taskset -c 1-5 "$PG/pgbench" -h 127.0.0.1 -p 6432 -U postgres \
        -n -S $3 -c "$CL" -j 5 -T "$DUR" postgres 2>&1)
  c1=$(ticks "$pid"); t1=$(date +%s.%N)
  tps=$(echo "$out" | grep -oE 'tps = [0-9.]+' | tail -1 | grep -oE '[0-9.]+$')
  cores=$(awk -v a="$c0" -v b="$c1" -v t0="$t0" -v t1="$t1" 'BEGIN{printf "%.2f",(b-a)/100/(t1-t0)}')
  us=$(awk -v tps="${tps:-0}" -v c="$cores" 'BEGIN{ if(tps>0) printf "%.1f", c*1e6/tps; else print "-"}')
  printf "%-24s tps=%-9s pooler=%-5s cores  X=%-6s us/txn\n" "$1" "${tps:-FAIL}" "$cores" "$us"
  pkill -f "pgbouncer $W" 2>/dev/null
}

echo "=== single pgbouncer pinned to 1 core | -S | -c$CL -j5 | ${DUR}s each ==="
dp=$(taskset -c 1-5 "$PG/pgbench" -h 127.0.0.1 -p 5440 -U postgres -n -S -c "$CL" -j 5 -T "$DUR" postgres 2>&1 | grep -oE 'tps = [0-9.]+' | tail -1 | grep -oE '[0-9.]+$')
printf "%-24s tps=%s   (5 PG cores, no pooler)\n" "direct-PG control" "$dp"
run "pgb  plain  persistent" 0 ""   disable
run "pgb  TLS    persistent" 1 ""   require
run "pgb  plain  churn(-C)"  0 "-C" disable
run "pgb  TLS    churn(-C)"  1 "-C" require
