#!/bin/bash
# Bless an image for GlideFS direct boot mode.
#
# Reads the blake3 digest from the accompanying .img.json, blesses the image
# under a content-addressed name (blake3:<hex>), then writes an S3 alias so
# the API can resolve the human-readable name to the current digest.
#
# Usage:
#   bless.sh --image /path/to/image.img --name ubuntu-noble-64g
#
# Requirements:
#   - GlideFS binary installed at /usr/local/bin/glidefs (or GLIDEFS_BIN)
#   - GlideFS config at /etc/glidefs/glidefs.toml (or GLIDEFS_CONFIG)
#   - Accompanying .img.json with a "blake3" field next to the image file
#   - aws CLI with credentials that can write to GLIDEFS_BUCKET
#   - ext4 raw image file
#
set -euo pipefail

# Default configuration
GLIDEFS_BIN="${GLIDEFS_BIN:-/usr/local/bin/glidefs}"
GLIDEFS_CONFIG="${GLIDEFS_CONFIG:-/etc/glidefs/glidefs.toml}"
# GLIDEFS_BUCKET: derive from config url (s3://bucket/...) if not set explicitly
if [[ -z "${GLIDEFS_BUCKET:-}" ]] && [[ -f "$GLIDEFS_CONFIG" ]]; then
    GLIDEFS_BUCKET=$(grep -oP 's3://\K[^/"]+' "$GLIDEFS_CONFIG" | head -1)
fi
: "${GLIDEFS_BUCKET:?GLIDEFS_BUCKET not set and could not be derived from $GLIDEFS_CONFIG}"

# Parse arguments
IMAGE_PATH=""
BASE_NAME=""

usage() {
    cat <<EOF
Usage: $(basename "$0") --image <path> --name <base-name>

Bless an ext4 image for GlideFS direct boot under a content-addressed name,
then write an S3 alias mapping the human-readable name to the digest.

Arguments:
  --image <path>      Path to the ext4 raw image file
  --name <base-name>  Human-readable base name (e.g., ubuntu-noble-64g)

Environment:
  GLIDEFS_BIN        GlideFS binary path (default: /usr/local/bin/glidefs)
  GLIDEFS_CONFIG     GlideFS config path (default: /etc/glidefs/glidefs.toml)
  GLIDEFS_BUCKET     S3 bucket for aliases (required)

Examples:
  GLIDEFS_BUCKET=my-glidefs-bucket \\
    $(basename "$0") --image /output/ubuntu-noble-64g-v1.img --name ubuntu-noble-64g
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --image)
            IMAGE_PATH="$2"
            shift 2
            ;;
        --name)
            BASE_NAME="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            ;;
    esac
done

# Validate arguments
if [[ -z "$IMAGE_PATH" ]]; then
    echo "Error: --image is required" >&2
    usage
fi

if [[ -z "$BASE_NAME" ]]; then
    echo "Error: --name is required" >&2
    usage
fi

if [[ ! -f "$IMAGE_PATH" ]]; then
    echo "Error: Image file not found: $IMAGE_PATH" >&2
    exit 1
fi

if [[ ! -x "$GLIDEFS_BIN" ]]; then
    echo "Error: GlideFS binary not found or not executable: $GLIDEFS_BIN" >&2
    exit 1
fi

if [[ ! -f "$GLIDEFS_CONFIG" ]]; then
    echo "Error: GlideFS config not found: $GLIDEFS_CONFIG" >&2
    exit 1
fi

# Read blake3 digest from accompanying .img.json
JSON_PATH="${IMAGE_PATH%.img}.img.json"
if [[ ! -f "$JSON_PATH" ]]; then
    # Fallback: the json may sit next to the image with the same stem
    JSON_PATH="${IMAGE_PATH%.img}.json"
fi
if [[ ! -f "$JSON_PATH" ]]; then
    echo "Error: .img.json not found at ${IMAGE_PATH%.img}.img.json" >&2
    exit 1
fi

BLAKE3_HEX=$(jq -r '.blake3 // ""' "$JSON_PATH")
if [[ -z "$BLAKE3_HEX" ]]; then
    echo "==> blake3 not in json, computing from image (this may take a moment)..."
    BLAKE3_HEX=$(b3sum "$IMAGE_PATH" | cut -d' ' -f1)
    # Patch the json so subsequent runs skip this step (best-effort, may need sudo)
    if jq --arg b "$BLAKE3_HEX" '.blake3 = $b' "$JSON_PATH" > "${JSON_PATH}.tmp" 2>/dev/null \
        && mv "${JSON_PATH}.tmp" "$JSON_PATH" 2>/dev/null; then
        echo "    Patched $JSON_PATH with blake3: ${BLAKE3_HEX:0:16}..."
    else
        rm -f "${JSON_PATH}.tmp"
        echo "    (could not patch json — permission denied, continuing)"
    fi
fi

DIGEST="blake3-${BLAKE3_HEX}"
MANIFEST_NAME="$DIGEST"

IMAGE_SIZE=$(stat -c%s "$IMAGE_PATH" 2>/dev/null || stat -f%z "$IMAGE_PATH")
IMAGE_SIZE_GB=$(awk "BEGIN {printf \"%.2f\", $IMAGE_SIZE / 1024 / 1024 / 1024}")

echo "==> Blessing image for GlideFS direct boot"
echo "    Image:    $IMAGE_PATH"
echo "    Size:     ${IMAGE_SIZE_GB}GB"
echo "    Digest:   $DIGEST"
echo "    Alias:    $BASE_NAME"
echo "    Config:   $GLIDEFS_CONFIG"
echo ""

# Bless under the content-addressed name (idempotent — same digest = same key)
GLIDEFS_BASE_PREFIX="${GLIDEFS_BASE_PREFIX:-bases}"
echo "==> Uploading image to GlideFS..."
"$GLIDEFS_BIN" bless \
    --image "$IMAGE_PATH" \
    --name "$MANIFEST_NAME" \
    --s3-prefix "$GLIDEFS_BASE_PREFIX" \
    --config "$GLIDEFS_CONFIG"

echo ""
echo "==> Writing S3 alias: aliases/${BASE_NAME} → ${DIGEST}"
printf '%s' "$DIGEST" | aws s3 cp - "s3://${GLIDEFS_BUCKET}/aliases/${BASE_NAME}" \
    --content-type "text/plain"

echo ""
echo "==> Done!"
echo ""
echo "  Manifest: bases/${DIGEST}"
echo "  Alias:    s3://${GLIDEFS_BUCKET}/aliases/${BASE_NAME}"
echo ""

echo "==> Cleaning up local image files..."
sudo rm -f "$IMAGE_PATH" "$JSON_PATH"
echo "    Removed $IMAGE_PATH"
