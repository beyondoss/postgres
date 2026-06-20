#!/usr/bin/env bash
# Host-side builder for the Beyond sibling Postgres extensions (beyond_auth,
# beyond_queue). Runs OUTSIDE any chroot/container — it needs the Rust + pgrx
# toolchain, which the minimal rootfs deliberately does not carry.
#
# For each extension it:
#   1. clones `git` @ `tag` from extensions.toml into a tmpdir (J-004a) — never a
#      hardcoded local path. A dev/homelab override (AUTH_EXT_GIT/AUTH_EXT_REF,
#      QUEUE_EXT_GIT/QUEUE_EXT_REF) lets you build a not-yet-pushed branch or a
#      local checkout without touching the canonical pins.
#   2. builds the pgrx extension for $POSTGRES_VERSION using the host pg_config,
#      emitting the CANONICAL install names beyond_auth / beyond_queue.
#   3. stages a tarball (rooted at `usr/...`) into $EXT_STAGE — the exact tree
#      04-beyond-extensions.sh extracts to / inside the rootfs.
#
# Output tarballs (consumed by 04 via AUTH_EXT_TARBALL / QUEUE_EXT_TARBALL):
#   $EXT_STAGE/beyond-auth-extension.tar.gz
#   $EXT_STAGE/beyond-queue-extension.tar.gz
#
# Env:
#   EXT_STAGE          (required) dir to write the staged tarballs into
#   POSTGRES_VERSION   (default 18)
#   TARGET_ARCH        (default amd64) — informational; build is for the host arch
#   PG_CONFIG          (default /usr/lib/postgresql/$POSTGRES_VERSION/bin/pg_config)
#   EXTENSIONS_TOML    (default <repo>/extensions.toml)
#   AUTH_EXT_GIT/AUTH_EXT_REF, QUEUE_EXT_GIT/QUEUE_EXT_REF  (dev overrides)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PG_REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
POSTGRES_VERSION="${POSTGRES_VERSION:-18}"
TARGET_ARCH="${TARGET_ARCH:-amd64}"
PG_CONFIG="${PG_CONFIG:-/usr/lib/postgresql/${POSTGRES_VERSION}/bin/pg_config}"
EXTENSIONS_TOML="${EXTENSIONS_TOML:-$PG_REPO/extensions.toml}"

info() { echo "==> [build-beyond-extensions] $*"; }
fail() { echo "  ✗ $*" >&2; exit 1; }

[[ -n "${EXT_STAGE:-}" ]] || fail "EXT_STAGE must be set (dir to write staged tarballs)"
mkdir -p "$EXT_STAGE"
[[ -x "$PG_CONFIG" ]] || fail "pg_config not found/executable at $PG_CONFIG (install postgresql-server-dev-${POSTGRES_VERSION})"
command -v cargo >/dev/null || fail "cargo not found — install the Rust toolchain"
command -v git >/dev/null || fail "git not found"

# Read git/tag from extensions.toml, honoring dev env overrides.
toml() { python3 -c "import tomllib,sys; d=tomllib.load(open('$EXTENSIONS_TOML','rb')); print(d$1)"; }
AUTH_GIT="${AUTH_EXT_GIT:-$(toml "['beyond']['auth']['git']")}"
AUTH_REF="${AUTH_EXT_REF:-$(toml "['beyond']['auth']['tag']")}"
QUEUE_GIT="${QUEUE_EXT_GIT:-$(toml "['beyond']['queue']['git']")}"
QUEUE_REF="${QUEUE_EXT_REF:-$(toml "['beyond']['queue']['tag']")}"

WORK="$(mktemp -d -t beyond-ext-build.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

# Clone `git` @ `ref` (branch | tag | sha; works for remote URLs and local paths).
clone_ref() { local git="$1" ref="$2" dst="$3"
  info "cloning $git @ $ref"
  git clone --quiet "$git" "$dst" || fail "git clone $git failed"
  git -C "$dst" checkout --quiet "$ref" || fail "git checkout $ref failed in $git"
}

# default_version from a .control file (authoritative for the --<ver>.sql name).
control_version() { grep -E '^default_version' "$1" | sed -E "s/.*=\s*'([^']+)'.*/\1/"; }

# ---- beyond_auth (pgrx package: emits the full usr/ tree directly) ----------
build_auth() {
  local src="$WORK/auth"
  clone_ref "$AUTH_GIT" "$AUTH_REF" "$src"
  command -v cargo-pgrx >/dev/null || fail "cargo-pgrx not found — cargo install cargo-pgrx --version =0.18.0 --locked"
  info "cargo pgrx package -p beyond-auth-extension (pg${POSTGRES_VERSION})"
  ( cd "$src" && PGRX_PG_CONFIG_PATH="$PG_CONFIG" cargo pgrx package \
      --pg-config "$PG_CONFIG" --no-default-features --features "pg${POSTGRES_VERSION}" \
      -p beyond-auth-extension >/dev/null )
  # pgrx names the package dir after the lib (beyond_auth), not the cargo package.
  local pkgdir="$src/target/release/beyond_auth-pg${POSTGRES_VERSION}"
  [[ -f "$pkgdir/usr/lib/postgresql/${POSTGRES_VERSION}/lib/beyond_auth.so" ]] \
    || fail "beyond_auth.so missing in $pkgdir — did lib.name=beyond_auth land?"
  tar -czf "$EXT_STAGE/beyond-auth-extension.tar.gz" -C "$pkgdir" .
  info "staged $EXT_STAGE/beyond-auth-extension.tar.gz"
}

# ---- beyond_queue (plain cargo build + manual assemble, mirrors release CI) --
build_queue() {
  local src="$WORK/queue"
  clone_ref "$QUEUE_GIT" "$QUEUE_REF" "$src"
  info "cargo build -p beyond-queue-extension (pg${POSTGRES_VERSION})"
  ( cd "$src" && PGRX_PG_CONFIG_PATH="$PG_CONFIG" cargo build --release \
      --no-default-features --features "pg${POSTGRES_VERSION}" \
      -p beyond-queue-extension >/dev/null )
  local so="$src/target/release/libbeyond_queue.so"
  [[ -f "$so" ]] || fail "libbeyond_queue.so missing — did lib.name=beyond_queue land?"
  local ctl="$src/beyond-queue-extension/beyond_queue.control"
  [[ -f "$ctl" ]] || fail "beyond_queue.control missing in clone"
  local ver; ver="$(control_version "$ctl")"
  local dist="$WORK/queue-dist"
  mkdir -p "$dist/usr/lib/postgresql/${POSTGRES_VERSION}/lib" \
           "$dist/usr/share/postgresql/${POSTGRES_VERSION}/extension"
  cp "$so" "$dist/usr/lib/postgresql/${POSTGRES_VERSION}/lib/beyond_queue.so"
  cp "$ctl" "$dist/usr/share/postgresql/${POSTGRES_VERSION}/extension/beyond_queue.control"
  cat "$src/beyond-queue-extension/sql/schema.sql" \
      "$src/tests/fixtures/hot_paths.sql" \
      "$src/tests/fixtures/load_pgrx_extension.sql" \
      > "$dist/usr/share/postgresql/${POSTGRES_VERSION}/extension/beyond_queue--${ver}.sql"
  tar -czf "$EXT_STAGE/beyond-queue-extension.tar.gz" -C "$dist" .
  info "staged $EXT_STAGE/beyond-queue-extension.tar.gz (v${ver})"
}

info "PG=${POSTGRES_VERSION} arch=${TARGET_ARCH} pg_config=${PG_CONFIG}"
info "auth: ${AUTH_GIT} @ ${AUTH_REF}"
info "queue: ${QUEUE_GIT} @ ${QUEUE_REF}"
build_auth
build_queue
info "done — staged tarballs in $EXT_STAGE"
