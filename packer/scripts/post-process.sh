#!/bin/bash
# Post-processor: Convert Docker image to ext4 for Firecracker direct boot.
#
# Postgres-image specific differences from beyond/packer/scripts/post-process.sh:
#   1. Default tier is 16g only. The rootfs is read-only (data on vdb); there is
#      no reason to build 512g or 1t tiers for a < 4 GB image.
#
set -e

: "${IMAGE_NAME:?IMAGE_NAME required}"
: "${OUTPUT_DIR:?OUTPUT_DIR required}"
: "${DOCKER_TAG:?DOCKER_TAG required}"
: "${IMAGE_VERSION:=latest}"

# Postgres rootfs is read-only. Default to 16g only; operator can override
# by setting BUILD_TIERS="16g 32g" etc. when calling the mise task.
BUILD_TIERS="${BUILD_TIERS:-16g}"

# Check if we're already inside a privileged container (has loop device access)
if losetup -f >/dev/null 2>&1; then
    echo "==> Running with loop device access"
    RUN_IN_CONTAINER=false
else
    echo "==> No loop device access, will run conversion in privileged container"
    RUN_IN_CONTAINER=true
fi

if [ "$RUN_IN_CONTAINER" = true ]; then
    # Export Docker image to tarball first (we can do this without privileges)
    BUILD_DIR=$(mktemp -d)
    echo "==> Exporting Docker image to tarball..."
    echo "    Docker tag: $DOCKER_TAG"

    CONTAINER_ID=$(docker create "$DOCKER_TAG")
    docker export "$CONTAINER_ID" > "$BUILD_DIR/rootfs.tar"
    docker rm "$CONTAINER_ID"
    docker rmi "$DOCKER_TAG" 2>/dev/null || true

    # Run the conversion in a privileged container
    mkdir -p "$OUTPUT_DIR"

    docker run --rm --privileged \
        -v "$BUILD_DIR:/build:ro" \
        -v "$OUTPUT_DIR:/output" \
        -e IMAGE_NAME="$IMAGE_NAME" \
        -e IMAGE_VERSION="$IMAGE_VERSION" \
        -e BUILD_TIERS="${BUILD_TIERS}" \
        ubuntu:jammy bash -c '
set -e
apt-get update && apt-get install -y e2fsprogs b3sum >/dev/null 2>&1

BUILD_DIR=/build
WORK_DIR=$(mktemp -d)
MOUNT_DIR=$(mktemp -d)
VERSIONED_NAME="${IMAGE_NAME}-${IMAGE_VERSION}"

cleanup() {
    umount "$MOUNT_DIR" 2>/dev/null || true
    losetup -D 2>/dev/null || true
    rm -rf "$WORK_DIR" "$MOUNT_DIR"
}
trap cleanup EXIT

echo "==> Extracting rootfs tarball..."
mkdir -p "$WORK_DIR/rootfs"
tar -xf "$BUILD_DIR/rootfs.tar" -C "$WORK_DIR/rootfs"

# =============================================================================
# Tiered raw .img (pre-sized for GlideFS direct boot)
# =============================================================================
ALL_TIERS="16g:16384 64g:16384 128g:32768 256g:32768 512g:32768 1t:65536"
TIERS=""
for tier_spec in $ALL_TIERS; do
    tier_size="${tier_spec%%:*}"
    for wanted in $BUILD_TIERS; do
        if [ "$tier_size" = "$wanted" ]; then
            TIERS="$TIERS $tier_spec"
            break
        fi
    done
done
TIERS="${TIERS# }"

for tier_spec in $TIERS; do
    tier_size="${tier_spec%%:*}"
    tier_inode_ratio="${tier_spec##*:}"
    tier_img="$WORK_DIR/${IMAGE_NAME}-${tier_size}.img"
    tier_versioned="${IMAGE_NAME}-${tier_size}-${IMAGE_VERSION}"

    echo "==> Creating ${tier_size} .img (inode ratio: ${tier_inode_ratio})..."
    truncate -s "${tier_size^^}" "$tier_img"
    mkfs.ext4 -F -L rootfs \
      -E lazy_itable_init=0,lazy_journal_init=0,nodiscard \
      -E stride=32,stripe_width=32 \
      -m 0 \
      -O sparse_super \
      -i "$tier_inode_ratio" \
      "$tier_img"

    LOOP_DEV=$(losetup -f --show "$tier_img")
    mount "$LOOP_DEV" "$MOUNT_DIR"
    cp -a "$WORK_DIR/rootfs/." "$MOUNT_DIR/"

    if [ ! -L "$MOUNT_DIR/sbin/init" ]; then
        ln -sf /lib/systemd/systemd "$MOUNT_DIR/sbin/init"
    fi

    echo "/dev/vda / ext4 noatime,nodiratime,commit=60 0 1" > "$MOUNT_DIR/etc/fstab"

    sync -f "$MOUNT_DIR"
    umount "$MOUNT_DIR"
    losetup -d "$LOOP_DEV"

    BLAKE3=$(b3sum "$tier_img" | cut -d" " -f1)
    TIER_SIZE=$(stat -c%s "$tier_img")

    mv "$tier_img" "/output/${tier_versioned}.img"
    ln -sf "${tier_versioned}.img" "/output/${IMAGE_NAME}-${tier_size}.img"

cat > "/output/${tier_versioned}.img.json" << EOF
{
  "name": "${IMAGE_NAME}-${tier_size}",
  "version": "${IMAGE_VERSION}",
  "tier": "${tier_size}",
  "inode_ratio": ${tier_inode_ratio},
  "blake3": "${BLAKE3}",
  "size": ${TIER_SIZE},
  "format": "ext4",
  "compression": "none",
  "created_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "boot_args": "root=/dev/vda console=ttyS0 reboot=k panic=1 pci=off",
  "boot_mode": "direct"
}
EOF
done

FIRST_TIER=$(echo "$BUILD_TIERS" | awk "{print \$1}")
ln -sf "${IMAGE_NAME}-${FIRST_TIER}.img" "/output/${IMAGE_NAME}.img"
echo "==> Build complete!"
'

    # Cleanup
    rm -rf "$BUILD_DIR"

    echo ""
    echo "==> Build complete!"
    echo "  RAW IMG:  $OUTPUT_DIR/${IMAGE_NAME}-${IMAGE_VERSION}.img"
    exit 0
fi

# Direct execution path (when we have loop device access)
BUILD_DIR=$(mktemp -d)
MOUNT_DIR=$(mktemp -d)

cleanup() {
    echo "==> Cleaning up..."
    umount "$MOUNT_DIR" 2>/dev/null || true
    losetup -D 2>/dev/null || true
    rm -rf "$BUILD_DIR" "$MOUNT_DIR"
}
trap cleanup EXIT

mkdir -p "$OUTPUT_DIR"

echo "==> Exporting Docker image to tarball..."
echo "    Docker tag: $DOCKER_TAG"

CONTAINER_ID=$(docker create "$DOCKER_TAG")
docker export "$CONTAINER_ID" > "$BUILD_DIR/rootfs.tar"
docker rm "$CONTAINER_ID"
docker rmi "$DOCKER_TAG" 2>/dev/null || true

echo "==> Extracting rootfs tarball..."
mkdir -p "$BUILD_DIR/rootfs"
tar -xf "$BUILD_DIR/rootfs.tar" -C "$BUILD_DIR/rootfs"

VERSIONED_NAME="${IMAGE_NAME}-${IMAGE_VERSION}"

# =============================================================================
# Tiered raw .img (pre-sized for GlideFS direct boot)
# =============================================================================
# Inode ratio (-i): 16384 for ≤ 64g, 32768 for larger.
# size:inode_ratio
ALL_TIERS="16g:16384 64g:16384 128g:32768 256g:32768 512g:32768 1t:65536"

TIERS=""
for tier_spec in $ALL_TIERS; do
    tier_size="${tier_spec%%:*}"
    for wanted in $BUILD_TIERS; do
        if [ "$tier_size" = "$wanted" ]; then
            TIERS="$TIERS $tier_spec"
            break
        fi
    done
done
TIERS="${TIERS# }"

for tier_spec in $TIERS; do
    tier_size="${tier_spec%%:*}"
    tier_inode_ratio="${tier_spec##*:}"
    tier_img="${BUILD_DIR}/${IMAGE_NAME}-${tier_size}.img"
    tier_versioned="${IMAGE_NAME}-${tier_size}-${IMAGE_VERSION}"

    echo "==> Creating ${tier_size} .img (inode ratio: ${tier_inode_ratio})..."
    truncate -s "${tier_size^^}" "$tier_img"
    mkfs.ext4 -F -L rootfs \
      -E lazy_itable_init=0,lazy_journal_init=0,nodiscard \
      -E stride=32,stripe_width=32 \
      -m 0 \
      -O sparse_super \
      -i "$tier_inode_ratio" \
      "$tier_img"

    LOOP_DEV=$(losetup -f --show "$tier_img")
    mount "$LOOP_DEV" "$MOUNT_DIR"
    cp -a "$BUILD_DIR/rootfs/." "$MOUNT_DIR/"

    if [ ! -L "$MOUNT_DIR/sbin/init" ]; then
        ln -sf /lib/systemd/systemd "$MOUNT_DIR/sbin/init"
    fi

    echo "/dev/vda / ext4 noatime,nodiratime,commit=60 0 1" > "$MOUNT_DIR/etc/fstab"

    sync -f "$MOUNT_DIR"
    umount "$MOUNT_DIR"
    losetup -d "$LOOP_DEV"

    # Skip e2fsck — just created, clean by definition.
    # e2fsck on large tiers (512g, 1t) takes minutes scanning empty block groups.

    BLAKE3=$(b3sum "$tier_img" | cut -d' ' -f1)
    TIER_SIZE=$(stat -c%s "$tier_img")
    TIER_DU=$(du -h "$tier_img" | cut -f1)

    mv "$tier_img" "$OUTPUT_DIR/${tier_versioned}.img"
    ln -sf "${tier_versioned}.img" "$OUTPUT_DIR/${IMAGE_NAME}-${tier_size}.img"

    cat > "$OUTPUT_DIR/${tier_versioned}.img.json" << EOF
{
  "name": "${IMAGE_NAME}-${tier_size}",
  "version": "${IMAGE_VERSION}",
  "tier": "${tier_size}",
  "inode_ratio": ${tier_inode_ratio},
  "blake3": "${BLAKE3}",
  "size": ${TIER_SIZE},
  "format": "ext4",
  "compression": "none",
  "created_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "boot_args": "root=/dev/vda console=ttyS0 reboot=k panic=1 pci=off",
  "boot_mode": "direct"
}
EOF

    echo "    Done: ${tier_size} (${TIER_DU} on disk)"
done

# Default symlink points to the first tier in BUILD_TIERS (16g by default).
FIRST_TIER=$(echo "$BUILD_TIERS" | awk '{print $1}')
ln -sf "${IMAGE_NAME}-${FIRST_TIER}.img" "$OUTPUT_DIR/${IMAGE_NAME}.img"

# =============================================================================
# Optional: GlideFS Bless (for direct boot mode)
# =============================================================================
if [ "${GLIDEFS_BLESS:-false}" = "true" ]; then
    GLIDEFS_BIN="${GLIDEFS_BIN:-/usr/local/bin/glidefs}"
    GLIDEFS_CONFIG="${GLIDEFS_CONFIG:-/etc/glidefs/glidefs.toml}"
    BASE_PREFIX="${GLIDEFS_BASE_PREFIX:-bases}"
    MANIFEST_NAME="${BASE_PREFIX}/${IMAGE_NAME}"

    if [ -x "$GLIDEFS_BIN" ] && [ -f "$GLIDEFS_CONFIG" ]; then
        echo "==> Blessing image for GlideFS direct boot..."
        echo "    Manifest: $MANIFEST_NAME"

        "$GLIDEFS_BIN" bless \
            --image "$OUTPUT_DIR/${VERSIONED_NAME}.img" \
            --name "$MANIFEST_NAME" \
            --config "$GLIDEFS_CONFIG" && \
        echo "    Blessed successfully!" || \
        echo "    Warning: GlideFS bless failed (non-fatal)"
    else
        echo "==> Skipping GlideFS bless: glidefs binary or config not found"
    fi
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "==> Build complete!"
echo ""
echo "  TIERED RAW .img (direct boot mode):"
for tier_spec in $TIERS; do
    tier_size="${tier_spec%%:*}"
    tier_versioned="${IMAGE_NAME}-${tier_size}-${IMAGE_VERSION}"
    echo "    ${tier_size}: $OUTPUT_DIR/${tier_versioned}.img"
done
FIRST_TIER=$(echo "$BUILD_TIERS" | awk '{print $1}')
echo "    Default: ${IMAGE_NAME}.img → ${IMAGE_NAME}-${FIRST_TIER}.img"
echo ""
