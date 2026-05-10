#!/usr/bin/env bash
set -euo pipefail

echo "==> Installing Rust toolchain for in-container build..."
# Build inside the container so the binary links against the same glibc it runs on.
# Eliminates host glibc version skew entirely.
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
  | sh -s -- -y --default-toolchain 1.92 --profile minimal
# shellcheck source=/dev/null
source "${HOME}/.cargo/env"

echo "==> Building beyond-pg and beyond-pg-sink from staged source..."
cargo build --release --manifest-path /tmp/beyond-pg-src/Cargo.toml

echo "==> Installing beyond-pg binary..."
install -m 0755 /tmp/beyond-pg-src/target/release/beyond-pg /usr/local/bin/beyond-pg

echo "==> Installing beyond-pg-sink binary..."
install -m 0755 /tmp/beyond-pg-src/target/release/beyond-pg-sink /usr/local/bin/beyond-pg-sink

echo "==> Creating hook directories..."
# Empty in MVP; tier-specific scripts drop in later via image updates.
for dir in pre-start post-start pre-stop pre-fork; do
  mkdir -p "/etc/postgresql/${POSTGRES_VERSION}/hooks/${dir}.d"
done

echo "==> 06-beyond-pg-install done"
