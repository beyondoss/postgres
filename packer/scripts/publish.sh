#!/bin/bash
# Publish rootfs images to S3 for fleet distribution
set -e

: "${S3_BUCKET:?S3_BUCKET required (e.g., paraglide-images-staging)}"
: "${AWS_REGION:=us-east-2}"

IMAGE_DIR="${IMAGE_DIR:-/var/lib/paraglide/images/rootfs}"
IMAGE_NAME="${1:-ubuntu-jammy}"
TIER="${2:-128g}"
S3_PREFIX="${S3_PREFIX:-rootfs}"

# Detect architecture from the image or environment
TARGET_ARCH="${TARGET_ARCH:-}"
if [ -z "$TARGET_ARCH" ]; then
  # Try to detect from Docker image inspection or default to host arch
  HOST_ARCH=$(uname -m)
  case "$HOST_ARCH" in
    x86_64) TARGET_ARCH="amd64" ;;
    aarch64|arm64) TARGET_ARCH="arm64" ;;
    *) TARGET_ARCH="amd64" ;;
  esac
fi
echo "==> Target architecture: $TARGET_ARCH"
echo "==> Tier: $TIER"

TIER_IMG="${IMAGE_NAME}-${TIER}.img"
if [ ! -f "$IMAGE_DIR/${TIER_IMG}" ]; then
    echo "Error: Image not found: $IMAGE_DIR/${TIER_IMG}"
    echo "Available images:"
    ls -la "$IMAGE_DIR"/*.img 2>/dev/null || echo "  (none)"
    exit 1
fi

METADATA_FILE=$(ls -t "$IMAGE_DIR/${IMAGE_NAME}-${TIER}"*.img.json 2>/dev/null | head -1)
if [ -z "$METADATA_FILE" ]; then
    echo "Error: No metadata file found for ${IMAGE_NAME}-${TIER}"
    echo "Available metadata files:"
    ls -la "$IMAGE_DIR/${IMAGE_NAME}"*.json 2>/dev/null || echo "  (none)"
    exit 1
fi

VERSION=$(jq -r '.version' "$METADATA_FILE")
BLAKE3=$(jq -r '.blake3' "$METADATA_FILE")
VERSIONED_IMG="${IMAGE_NAME}-${TIER}-${VERSION}.img"

if [ ! -f "$IMAGE_DIR/${VERSIONED_IMG}" ]; then
    VERSIONED_IMG="${TIER_IMG}"
fi

echo "==> Publishing ${IMAGE_NAME} tier=${TIER} (version: $VERSION, arch: $TARGET_ARCH)"
echo "    BLAKE3: $BLAKE3"

S3_KEY="${S3_PREFIX}/${IMAGE_NAME}/${TARGET_ARCH}/${VERSION}/${TIER}/${IMAGE_NAME}-${TIER}.img"
S3_META_KEY="${S3_PREFIX}/${IMAGE_NAME}/${TARGET_ARCH}/${VERSION}/${TIER}/metadata.json"

echo "==> Uploading to s3://${S3_BUCKET}/${S3_KEY}..."
aws s3 cp "$IMAGE_DIR/${VERSIONED_IMG}" "s3://${S3_BUCKET}/${S3_KEY}" \
    --region "$AWS_REGION" \
    --metadata "blake3=${BLAKE3},version=${VERSION}"

aws s3 cp "$METADATA_FILE" "s3://${S3_BUCKET}/${S3_META_KEY}" \
    --region "$AWS_REGION"

echo "==> Updating latest pointer..."
echo "$VERSION" | aws s3 cp - "s3://${S3_BUCKET}/${S3_PREFIX}/${IMAGE_NAME}/${TARGET_ARCH}/latest" \
    --region "$AWS_REGION"

MANIFEST_KEY="${S3_PREFIX}/manifest.json"
echo "==> Updating global manifest..."

EXISTING_MANIFEST=$(aws s3 cp "s3://${S3_BUCKET}/${MANIFEST_KEY}" - 2>/dev/null || echo '{"images":{}}')

NEW_MANIFEST=$(echo "$EXISTING_MANIFEST" | jq --arg name "$IMAGE_NAME" --arg version "$VERSION" --arg blake3 "$BLAKE3" --arg updated "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '
    .images[$name] = {
        "latest": $version,
        "blake3": $blake3,
        "updated_at": $updated
    } |
    .updated_at = $updated
')

echo "$NEW_MANIFEST" | aws s3 cp - "s3://${S3_BUCKET}/${MANIFEST_KEY}" \
    --region "$AWS_REGION" \
    --content-type "application/json"

echo ""
echo "==> Published successfully!"
echo "    s3://${S3_BUCKET}/${S3_KEY}"
echo ""
echo "Hosts can pull with:"
echo "    IMAGE_BUCKET=$S3_BUCKET packer/scripts/pull.sh $IMAGE_NAME $TIER"
