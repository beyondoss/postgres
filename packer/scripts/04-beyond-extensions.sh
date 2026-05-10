#!/usr/bin/env bash
set -euo pipefail

# Download a Beyond sibling extension tarball from its GitHub Release and
# extract it directly to /. The tarball carries a full directory tree:
#   usr/lib/postgresql/{PG}/lib/{name}.so
#   usr/share/postgresql/{PG}/extension/{name}.control
#   usr/share/postgresql/{PG}/extension/{name}--{version}.sql
#
# Asset name: {asset_prefix}-v{version}-pg{PG}-linux-{arch}.tar.gz
# Version is derived by stripping everything up to and including the last 'v'
# in the tag: "server/v0.1.0" -> "0.1.0", "ext-v0.1.0" -> "0.1.0".
install_beyond_ext() {
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

echo "==> Installing Beyond sibling extensions..."

install_beyond_ext "beyond-auth-extension"  "${AUTH_EXT_GIT}"  "${AUTH_EXT_TAG}"
install_beyond_ext "beyond-queue-extension" "${QUEUE_EXT_GIT}" "${QUEUE_EXT_TAG}"

echo "==> 04-beyond-extensions done"
