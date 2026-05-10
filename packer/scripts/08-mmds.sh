#!/bin/bash
set -e

echo "==> Installing MMDS support..."

cat > /usr/local/bin/mmds-client << 'MMDS_SCRIPT'
#!/bin/bash
MMDS_IP="${MMDS_IP:-169.254.169.254}"
MMDS_TOKEN_TTL="${MMDS_TOKEN_TTL:-300}"

get_token() {
    curl -s -X PUT "http://${MMDS_IP}/latest/api/token" \
        -H "X-metadata-token-ttl-seconds: ${MMDS_TOKEN_TTL}" 2>/dev/null
}

fetch() {
    local path="${1:-/}"
    local token
    token=$(get_token)
    if [ -n "$token" ]; then
        curl -s -H "Accept: application/json" -H "X-metadata-token: ${token}" "http://${MMDS_IP}${path}" 2>/dev/null
    else
        curl -s -H "Accept: application/json" "http://${MMDS_IP}${path}" 2>/dev/null
    fi
}

case "${1:-get}" in
    get)
        fetch "${2:-/}"
        ;;
    token)
        get_token
        ;;
    wait)
        for i in $(seq 1 30); do
            if fetch "/" >/dev/null 2>&1; then
                echo "MMDS available"
                exit 0
            fi
            sleep 0.5
        done
        echo "MMDS not available" >&2
        exit 1
        ;;
    *)
        echo "Usage: mmds-client [get|token|wait] [path]"
        exit 1
        ;;
esac
MMDS_SCRIPT
chmod +x /usr/local/bin/mmds-client

cat > /usr/local/bin/mmds-setup << 'MMDS_SETUP'
#!/bin/bash
set -e

MMDS_IP="169.254.169.254"
DEV="eth0"
MMDS_DATA_DIR="/run/mmds"

if ! ip route show | grep -q "$MMDS_IP"; then
    ip route add "$MMDS_IP" dev "$DEV" 2>/dev/null || true
    echo "Added route to MMDS ($MMDS_IP via $DEV)"
fi

mkdir -p "$MMDS_DATA_DIR"

# Wait for MMDS endpoint to be reachable (network must be up first).
echo "Waiting for MMDS..."
for i in $(seq 1 30); do
    if curl -s --connect-timeout 1 "http://${MMDS_IP}/" >/dev/null 2>&1; then
        echo "MMDS reachable"
        break
    fi
    sleep 0.5
done

# Now poll until MMDS has real data. The host sets MMDS via the Firecracker
# API ~10ms after spawning the process — the endpoint responds immediately
# but may return empty or "latest/" until the host calls PUT /mmds.
echo "Waiting for MMDS data..."
METADATA=""
for i in $(seq 1 30); do
    TOKEN=$(curl -s -X PUT "http://${MMDS_IP}/latest/api/token" \
        -H "X-metadata-token-ttl-seconds: 300" --connect-timeout 1 2>/dev/null || true)

    if [ -n "$TOKEN" ]; then
        METADATA=$(curl -s -H "Accept: application/json" \
            -H "X-metadata-token: ${TOKEN}" \
            "http://${MMDS_IP}/" --connect-timeout 1 2>/dev/null || true)
    else
        METADATA=$(curl -s -H "Accept: application/json" \
            "http://${MMDS_IP}/" --connect-timeout 1 2>/dev/null || true)
    fi

    if [ -n "$METADATA" ] && [ "$METADATA" != "latest/" ]; then
        echo "MMDS data available (attempt $i)"
        break
    fi
    sleep 0.01
done

if [ -n "$METADATA" ] && [ "$METADATA" != "latest/" ]; then
    echo "$METADATA" > "$MMDS_DATA_DIR/metadata.json"
    echo "Metadata cached to $MMDS_DATA_DIR/metadata.json"

    if command -v jq >/dev/null 2>&1; then
        HOSTNAME=$(echo "$METADATA" | jq -r '.latest."meta-data".hostname // .hostname // empty' 2>/dev/null)
        if [ -n "$HOSTNAME" ]; then
            echo "$HOSTNAME" > /etc/hostname
            hostname "$HOSTNAME"
            echo "Set hostname to $HOSTNAME"
        fi

        SSH_KEYS=$(echo "$METADATA" | jq -r '
            (.latest."meta-data"."public-keys" // ."public-keys" // {})
            | to_entries[]
            | .value."openssh-key" // .value
        ' 2>/dev/null | grep -v '^null$' || true)
        if [ -n "$SSH_KEYS" ]; then
            mkdir -p /root/.ssh
            echo "$SSH_KEYS" >> /root/.ssh/authorized_keys
            chmod 600 /root/.ssh/authorized_keys
            echo "Added SSH keys from MMDS"
        fi

        # Extract env vars and write to /etc/environment (available system-wide)
        ENV_VARS=$(echo "$METADATA" | jq -r '
            (.latest."meta-data".env // .env // {})
            | to_entries[]
            | "\(.key)=\(.value)"
        ' 2>/dev/null || true)
        if [ -n "$ENV_VARS" ]; then
            echo "$ENV_VARS" > /etc/environment
            echo "Wrote $(echo "$ENV_VARS" | wc -l) env vars to /etc/environment"
        fi
    fi
else
    echo "No MMDS data available (this is normal if MMDS is not configured)"
fi
MMDS_SETUP
chmod +x /usr/local/bin/mmds-setup

# mmds-setup.service is intentionally not enabled — the guest agent handles
# MMDS polling inline at startup (no curl subprocess, no shell script on the
# boot critical path). The mmds-setup and mmds-client scripts remain on disk
# for manual debugging: `mmds-client get /` or `mmds-setup` from a shell.

echo "==> MMDS support installed"
