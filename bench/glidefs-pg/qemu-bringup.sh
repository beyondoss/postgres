#!/usr/bin/env bash
# Stand up the isolated QEMU benchmark VM and provision it.
#
# WHY a VM: the host runs the production homelab glidefs (shared handoff socket +
# global ublk recovery), so a second glidefs can't co-run safely. A QEMU/KVM guest
# has its own kernel (nbd/ublk), RAM, and /run — fully isolated. We run glidefs
# INSIDE the guest and put Postgres on its nbd device.
#
# IMPORTANT: VM disks live on a DISK-backed dir (default /var/tmp), NOT /tmp —
# /tmp here is a 14G tmpfs shared with the homelab; the scratch qcow2 grows to
# many GB across runs and will ENOSPC the tmpfs (learned the hard way).
#
# After this prints READY, drive experiments with ssh on port 2222 (key: $VMDIR/id).
set -euo pipefail
VMDIR="${VMDIR:-/var/tmp/pgtune-qemu}"
BASE="${BASE:-/var/lib/k617-vm/noble-cloudimg.img}"
MEM="${MEM:-8192}"; CPUS="${CPUS:-6}"; SSHP="${SSHP:-2222}"
HARNESS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$VMDIR"; cd "$VMDIR"
SSH="ssh -i $VMDIR/id -p $SSHP -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR bench@127.0.0.1"
SCP="scp -i $VMDIR/id -P $SSHP -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
log(){ printf '\033[1;36m[bringup]\033[0m %s\n' "$*" >&2; }

# re-runnable: refuse if a VM is live, else clear stale disks
if [ -f qemu.pid ] && kill -0 "$(cat qemu.pid 2>/dev/null)" 2>/dev/null; then
  echo "a VM is already running (pid $(cat qemu.pid)); kill it first"; exit 1
fi
rm -f root.qcow2 scratch.qcow2
[ -f id ] || ssh-keygen -t ed25519 -N '' -f id -q
log "create disks (overlay backed by $BASE; scratch on disk)"
qemu-img create -f qcow2 -b "$BASE" -F qcow2 root.qcow2 40G >/dev/null
qemu-img create -f qcow2 scratch.qcow2 40G >/dev/null
cat > user-data <<EOF
#cloud-config
hostname: pgtune-bench
users:
  - name: bench
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    ssh_authorized_keys: ["$(cat id.pub)"]
ssh_pwauth: false
EOF
printf 'instance-id: pgtune-bench\nlocal-hostname: pgtune-bench\n' > meta-data
cloud-localds seed.img user-data meta-data

log "boot qemu (${MEM}MB ${CPUS}vcpu, ssh :$SSHP)"
qemu-system-x86_64 -enable-kvm -m "$MEM" -smp "$CPUS" -cpu host \
  -drive file=root.qcow2,if=virtio -drive file=scratch.qcow2,if=virtio \
  -drive file=seed.img,if=virtio,format=raw \
  -netdev user,id=n0,hostfwd=tcp::${SSHP}-:22 -device virtio-net-pci,netdev=n0 \
  -display none -serial file:serial.log -pidfile qemu.pid -daemonize

log "wait for ssh"
for _ in $(seq 1 60); do $SSH true 2>/dev/null && break; sleep 3; done
$SSH true 2>/dev/null || { tail -20 serial.log; echo "ssh never came up"; exit 1; }

log "provision: scratch fs + PG18 (PGDG) + pgbouncer + glidefs + harness"
$SSH 'set -e; sudo mkfs.ext4 -q -F /dev/vdb; sudo mkdir -p /mnt/scratch; sudo mount /dev/vdb /mnt/scratch; sudo chown bench:bench /mnt/scratch'
$SSH 'bash -s' <<'PROV'
set -e; export DEBIAN_FRONTEND=noninteractive
sudo install -d /usr/share/postgresql-common/pgdg
sudo curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc
. /etc/os-release
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null
sudo apt-get update -qq
sudo apt-get install -y -qq postgresql-18 jq fio pgbouncer >/dev/null
PROV
$SCP /usr/local/bin/glidefs bench@127.0.0.1:/tmp/glidefs >/dev/null && $SSH 'sudo install /tmp/glidefs /usr/local/bin/glidefs'
$SCP -r "$HARNESS" bench@127.0.0.1:/home/bench/glidefs-pg >/dev/null
$SSH 'chmod +x /home/bench/glidefs-pg/*.sh; /usr/lib/postgresql/18/bin/postgres --version; glidefs --version 2>/dev/null||true; pgbouncer --version|head -1'
log "READY — ssh -i $VMDIR/id -p $SSHP bench@127.0.0.1"
