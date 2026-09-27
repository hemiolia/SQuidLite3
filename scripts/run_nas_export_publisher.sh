#!/usr/bin/env bash
# Run NAS export artifacts publisher container.
# This script starts a Docker container on the NAS to publish generated exports
# (gui/index.html and 分析.xlsx) to a pre-authorized rclone remote.
set -euo pipefail
umask 077

ROOT=""
IMAGE=""
REMOTE=""
ONCE=0

usage() {
    cat <<'EOF'
Usage: run_nas_export_publisher.sh --root ABS --image IMAGE --remote NAME:PATH [--once]

Options:
  --root ABS         Absolute path to archive root directory on NAS (symlinks rejected)
  --image IMAGE      Docker image name (backup image containing Python 3 and rclone)
  --remote NAME:PATH Rclone remote prefix (e.g. exports_remote:exports)
  --once             Run a single verification cycle with --rm (rejects if daemon is running)
  -h, --help         Show this help message and exit
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --root)
            [[ $# -ge 2 ]] || { echo "Error: --root requires an argument" >&2; exit 2; }
            ROOT="$2"
            shift 2
            ;;
        --image)
            [[ $# -ge 2 ]] || { echo "Error: --image requires an argument" >&2; exit 2; }
            IMAGE="$2"
            shift 2
            ;;
        --remote)
            [[ $# -ge 2 ]] || { echo "Error: --remote requires an argument" >&2; exit 2; }
            REMOTE="$2"
            shift 2
            ;;
        --once)
            ONCE=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Error: unrecognized argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

verify_no_symlink_ancestors() {
    local target="$1"
    local desc="$2"

    if [[ "$target" != /* || "$target" == *"/.."* || "$target" =~ (^|/)\.\.(/|$) ]]; then
        echo "Error: $desc path invalid or contains parent traversal: $target" >&2
        exit 1
    fi

    local curr="$target"
    while [[ "$curr" != "/" && -n "$curr" ]]; do
        if [[ -L "$curr" ]]; then
            echo "Error: symlink rejected in $desc or ancestor: $curr" >&2
            exit 1
        fi
        local parent
        parent="$(dirname "$curr")"
        if [[ "$parent" == "$curr" ]]; then
            break
        fi
        curr="$parent"
    done
}

if [[ -z "$ROOT" ]]; then
    echo "Error: --root is required" >&2
    exit 2
fi

if [[ "$ROOT" != /* || "$ROOT" == / ]]; then
    echo "Error: --root must be an absolute directory (not /)" >&2
    exit 2
fi

if [[ "$ROOT" == *","* ]]; then
    echo "Error: --root contains comma (,), which is invalid for Docker mounts" >&2
    exit 2
fi

if [[ "$ROOT" == *"/.."* || "$ROOT" =~ (^|/)\.\.(/|$) ]]; then
    echo "Error: --root contains invalid parent traversal ('..'): $ROOT" >&2
    exit 2
fi

verify_no_symlink_ancestors "$ROOT" "--root"

if [[ ! -d "$ROOT" ]]; then
    echo "Error: --root must be an existing directory: $ROOT" >&2
    exit 2
fi

if [[ -z "$IMAGE" || "$IMAGE" == -* ]]; then
    echo "Error: --image is required and must not start with '-'" >&2
    exit 2
fi

if [[ "$IMAGE" == *"<"* || "$IMAGE" == *">"* || "$IMAGE" =~ [[:space:]] || "$IMAGE" =~ [[:cntrl:]] ]]; then
    echo "Error: --image contains invalid characters" >&2
    exit 2
fi

if [[ -z "$REMOTE" || "$REMOTE" == -* ]]; then
    echo "Error: --remote is required and must not start with '-'" >&2
    exit 2
fi

if [[ "$REMOTE" != *":"* ]]; then
    echo "Error: --remote must be in NAME:PATH format" >&2
    exit 2
fi

remote_name="${REMOTE%%:*}"
remote_path="${REMOTE#*:}"

if [[ ! "$remote_name" =~ ^[A-Za-z0-9_][A-Za-z0-9._-]*$ ]]; then
    echo "Error: remote name must start with alphanumeric or underscore and contain only [A-Za-z0-9._-]" >&2
    exit 2
fi

if [[ -z "$remote_path" || "$remote_path" == /* || "$remote_path" == */ || "$remote_path" == \\* || "$remote_path" == -* ]]; then
    echo "Error: remote path must be a non-empty relative path and cannot start with '-', '/', or '\\'" >&2
    exit 2
fi

if [[ "$remote_path" == *"\\"* || "$remote_path" =~ [[:cntrl:]] ]]; then
    echo "Error: remote path contains backslash or control characters" >&2
    exit 2
fi

IFS='/' read -r -a path_parts <<< "$remote_path"
for part in "${path_parts[@]}"; do
    if [[ "$part" == ".." || "$part" == "." || -z "$part" ]]; then
        echo "Error: remote path contains invalid path component: '$part'" >&2
        exit 2
    fi
done

EXPORTS_DIR="$ROOT/exports"
RUNTIME_DIR="$ROOT/runtime/export-publisher"
PUBLISHER_SCRIPT="$RUNTIME_DIR/nas_publish_exports.py"
STATE_DIR="$ROOT/state/export-publisher"
SECRETS_DIR="$ROOT/secrets/export-publisher"
RCLONE_CONF="$SECRETS_DIR/rclone.conf"

# Verify all mount paths and script root for lexical ancestor symlinks before creating state
verify_no_symlink_ancestors "$EXPORTS_DIR" "exports directory"
verify_no_symlink_ancestors "$RUNTIME_DIR" "runtime publisher directory"
verify_no_symlink_ancestors "$SECRETS_DIR" "secrets directory"
verify_no_symlink_ancestors "$STATE_DIR" "state directory"

if [[ ! -d "$EXPORTS_DIR" ]]; then
    echo "Error: exports directory does not exist: $EXPORTS_DIR" >&2
    exit 1
fi

if [[ ! -d "$RUNTIME_DIR" ]]; then
    echo "Error: runtime publisher directory does not exist: $RUNTIME_DIR" >&2
    exit 1
fi

verify_no_symlink_ancestors "$PUBLISHER_SCRIPT" "publisher script"
if [[ ! -f "$PUBLISHER_SCRIPT" ]]; then
    echo "Error: nas_publish_exports.py does not exist or is not a regular file: $PUBLISHER_SCRIPT" >&2
    exit 1
fi

if [[ ! -d "$SECRETS_DIR" ]]; then
    echo "Error: secrets directory does not exist: $SECRETS_DIR" >&2
    exit 1
fi

verify_no_symlink_ancestors "$RCLONE_CONF" "rclone config"
if [[ ! -f "$RCLONE_CONF" ]]; then
    echo "Error: rclone.conf does not exist or is not a regular file: $RCLONE_CONF" >&2
    exit 1
fi

if [[ ! -e "$STATE_DIR" ]]; then
    mkdir -p -m 0700 "$STATE_DIR" || {
        echo "Error: failed to create state directory: $STATE_DIR" >&2
        exit 1
    }
fi

verify_no_symlink_ancestors "$STATE_DIR" "state directory"
if [[ ! -d "$STATE_DIR" ]]; then
    echo "Error: state directory is not a directory: $STATE_DIR" >&2
    exit 1
fi

# Verify rclone.conf has 600 permissions (fail-closed if perm cannot be retrieved or != 600)
perm=$(stat -c '%a' "$RCLONE_CONF" 2>/dev/null || stat -f '%Lp' "$RCLONE_CONF" 2>/dev/null || true)
if [[ "$perm" != "600" ]]; then
    echo "Error: rclone.conf permissions must be 600 (found: '${perm:-empty}')" >&2
    exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "Error: docker command not found" >&2
    exit 1
fi

if ! docker inspect --type=image "$IMAGE" >/dev/null 2>&1; then
    echo "Error: docker image not found: $IMAGE" >&2
    exit 1
fi

CONTAINER_NAME="ikaring-archive-export-publisher"
container_exists=0
container_running=0

if docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
    container_exists=1
    if ! is_running=$(docker inspect --format '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null); then
        echo "Error: failed to inspect running state for container '$CONTAINER_NAME'" >&2
        exit 1
    fi
    if [[ "$is_running" == "true" ]]; then
        container_running=1
    elif [[ "$is_running" == "false" ]]; then
        container_running=0
    else
        echo "Error: unexpected container running state '$is_running' for '$CONTAINER_NAME'" >&2
        exit 1
    fi
fi

if (( ONCE )); then
    if (( container_running )); then
        echo "Error: daemon container '$CONTAINER_NAME' is currently running; refusing --once execution" >&2
        exit 1
    fi
else
    if (( container_exists )); then
        echo "Error: container '$CONTAINER_NAME' already exists; refusing to automatically stop or remove it" >&2
        exit 1
    fi
fi

DOCKER_COMMON_ARGS=(
    --user 1000:10
    --cap-drop ALL
    --security-opt no-new-privileges
    --read-only
    --tmpfs /tmp:rw,nosuid,nodev,size=32m
    --log-opt max-size=5m
    --log-opt max-file=3
    -e RCLONE_CONFIG=/publisher-secrets/rclone.conf
    -e PYTHONDONTWRITEBYTECODE=1
    -e PYTHONUNBUFFERED=1
    -e TZ=Asia/Tokyo
    --mount "type=bind,source=$EXPORTS_DIR,target=/exports,readonly"
    --mount "type=bind,source=$RUNTIME_DIR,target=/publisher,readonly"
    --mount "type=bind,source=$STATE_DIR,target=/state"
    --mount "type=bind,source=$SECRETS_DIR,target=/publisher-secrets"
)

if (( ONCE )); then
    exec docker run \
        --rm \
        "${DOCKER_COMMON_ARGS[@]}" \
        --entrypoint python3 \
        "$IMAGE" \
        /publisher/nas_publish_exports.py \
        --exports-dir /exports \
        --state-dir /state \
        --remote "$REMOTE"
else
    DAEMON_SCRIPT='
child=""
term_handler() {
    if [ -n "$child" ]; then
        kill -TERM "$child" 2>/dev/null
        wait "$child" 2>/dev/null
    fi
    exit 0
}
trap term_handler TERM INT
while :; do
    python3 /publisher/nas_publish_exports.py --exports-dir /exports --state-dir /state --remote "$1" &
    child=$!
    wait "$child" || true
    child=""
    sleep 300 &
    child=$!
    wait "$child" || true
    child=""
done
'
    exec docker run \
        -d \
        --name "$CONTAINER_NAME" \
        --restart unless-stopped \
        "${DOCKER_COMMON_ARGS[@]}" \
        --entrypoint /bin/sh \
        "$IMAGE" \
        -c "$DAEMON_SCRIPT" sh "$REMOTE"
fi
