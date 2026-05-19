#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

echo "==> Installing base packages..."

apt-get update -qq
apt-get install -y --no-install-recommends \
  iproute2 \
  iptables \
  curl \
  ca-certificates \
  locales \
  jq \
  e2fsprogs \
  rsync \
  git \
  zstd \
  netcat-openbsd \
  vim \
  awscli

echo "==> Generating en_US.UTF-8 locale..."
echo "en_US.UTF-8 UTF-8" > /etc/locale.gen
locale-gen
update-locale LANG=en_US.UTF-8

# /sbin/init → beyond-pg-init: a minimal sync PID 1 that supervises
# beyond-pg (the postgres-supervisor binary) under a handoff::Supervisor.
# Both binaries are installed by script 06.
ln -sf /usr/local/bin/beyond-pg-init /sbin/init

echo "==> 01-base-packages done"
