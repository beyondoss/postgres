#!/usr/bin/env bash
set -euo pipefail

# Install the Beyond sibling extensions (beyond_auth, beyond_queue) into the
# rootfs. Two sources, in priority order:
#
#   1. LOCAL tarball (AUTH_EXT_TARBALL / QUEUE_EXT_TARBALL) — a tree built on the
#      host by build-beyond-extensions.sh (clone-from-git + cargo). This is the
#      homelab/dev path: it works without a cut GitHub Release and without the
#      Rust toolchain inside this chroot.
#   2. GitHub RELEASE tarball (AUTH_EXT_GIT/AUTH_EXT_TAG, QUEUE_EXT_GIT/QUEUE_EXT_TAG)
#      — the production path. Asset name: {asset_prefix}-v{version}-pg{PG}-linux-{arch}.tar.gz
#      (version = tag after the last 'v': "server/v0.1.0" -> "0.1.0").
#
# Either way the tarball carries a tree rooted at usr/, extracted to /:
#   usr/lib/postgresql/{PG}/lib/{name}.so
#   usr/share/postgresql/{PG}/extension/{name}.control
#   usr/share/postgresql/{PG}/extension/{name}--{version}.sql

install_local() {
  local asset_prefix="$1" tarball="$2"
  echo "==> Installing ${asset_prefix} from local tarball ${tarball}..."
  [[ -f "${tarball}" ]] || { echo "ERROR: local tarball ${tarball} not found"; exit 1; }
  tar -xzf "${tarball}" -C /
  echo "==> Installed ${asset_prefix} (local)"
}

install_release() {
  local asset_prefix="$1" git_url="$2" tag="$3"
  local version="${tag##*v}"
  local tarball="${asset_prefix}-v${version}-pg${POSTGRES_VERSION}-linux-${TARGET_ARCH}.tar.gz"
  local url="${git_url}/releases/download/${tag}/${tarball}"
  local tmp
  tmp=$(mktemp /tmp/beyond-XXXXXX.tar.gz)
  echo "==> Downloading ${asset_prefix} ${tag} from ${url}..."
  curl -fsSL "${url}" -o "${tmp}" \
    || { echo "ERROR: ${asset_prefix} ${tag} not found at ${url}"; exit 1; }
  tar -xzf "${tmp}" -C /
  rm -f "${tmp}"
  echo "==> Installed ${asset_prefix} ${tag}"
}

# $1 asset_prefix, $2 local_tarball_var_value, $3 git, $4 tag
install_beyond_ext() {
  local asset_prefix="$1" local_tarball="$2" git_url="$3" tag="$4"
  if [[ -n "${local_tarball}" ]]; then
    install_local "${asset_prefix}" "${local_tarball}"
  else
    install_release "${asset_prefix}" "${git_url}" "${tag}"
  fi
}

echo "==> Installing Beyond sibling extensions..."

install_beyond_ext "beyond-auth-extension"  "${AUTH_EXT_TARBALL:-}"  "${AUTH_EXT_GIT:-}"  "${AUTH_EXT_TAG:-}"
install_beyond_ext "beyond-queue-extension" "${QUEUE_EXT_TARBALL:-}" "${QUEUE_EXT_GIT:-}" "${QUEUE_EXT_TAG:-}"

echo "==> 04-beyond-extensions done"
