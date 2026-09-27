#!/usr/bin/env bash
# Transfer only the collector source, build it on the NAS, then retain the old release.
set -euo pipefail
umask 077

NAS_HOST="${IKARING_NAS_HOST:-nas}"
NAS_ROOT="${IKARING_NAS_ROOT:-/home/Natsuki/ikaring-archive}"
IMAGE_NAME="${IKARING_IMAGE_NAME:-ikaring-archive:current}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

if [[ -z "$NAS_HOST" || "$NAS_HOST" == -* || -z "$IMAGE_NAME" || "$IMAGE_NAME" == -* ]]; then
    echo "Invalid NAS host or image name" >&2
    exit 2
fi

# An explicit source list keeps data and credentials out of the transfer and build
# context even when they are added to the repository tree later.
shopt -s nullglob
python_sources=(src/python/ikarchive/*.py)
node_sources=(src/node/*.mjs)
shopt -u nullglob
if (( ${#python_sources[@]} == 0 || ${#node_sources[@]} == 0 )); then
    echo "Collector sources are missing" >&2
    exit 2
fi
source_files=(
    archive.py package.json package-lock.json .npmrc sql/schema.sql
    deploy/nas/Dockerfile deploy/nas/Dockerfile.dockerignore
    config/font-unicode-ranges.json
    assets/fonts/Splatoon2-Unified.otf
    "${python_sources[@]}" "${node_sources[@]}"
)
for file in "${source_files[@]}"; do
    if [[ ! -f "$REPO_ROOT/$file" || -L "$REPO_ROOT/$file" ]]; then
        printf 'Missing or symbolic-link source: %s\n' "$file" >&2
        exit 2
    fi
done
# This repository's npmrc contains only the public scoped registry URL. Do not
# transfer it if credentials or other settings are added later.
if [[ "$(<"$REPO_ROOT/.npmrc")" != '@samuel:registry=https://gitlab.fancy.org.uk/api/v4/packages/npm/' ]]; then
    echo "Unexpected .npmrc contents; refusing to transfer possible credentials" >&2
    exit 2
fi

# SSH runs a fixed Bash program. The program and user-configured values travel
# as base64, so shell metacharacters cannot become remote shell syntax.
encode_arg() { printf %s "$1" | base64 | tr -d '\n'; }
root_arg="$(encode_arg "$NAS_ROOT")"
image_arg="$(encode_arg "$IMAGE_NAME")"

remote_script=$(cat <<'REMOTE_SCRIPT'
set -euo pipefail
umask 077
root="$(printf %s "$1" | base64 -d)"
image="$(printf %s "$2" | base64 -d)"
if [[ "$root" != /* || "$root" == / || ! -d "$root" || -L "$root" ]]; then
    echo "NAS root must be an existing absolute directory" >&2
    exit 2
fi
runtime="$root/runtime"
app="$runtime/app"
if [[ -L "$runtime" || -L "$app" || ( -e "$app" && ! -d "$app" ) ]]; then
    echo "Runtime path has an unexpected type" >&2
    exit 2
fi
mkdir -p -- "$runtime/releases"
# Serialize builds and app swaps without clearing another staging area.
exec 9> "$runtime/.deploy.lock"
flock -n 9 || { echo "Another deployment is running" >&2; exit 1; }
stage="$(mktemp -d "$runtime/.app-stage.XXXXXXXX")"
tar -C "$stage" -xf -
for required in archive.py package.json package-lock.json .npmrc sql/schema.sql deploy/nas/Dockerfile deploy/nas/Dockerfile.dockerignore; do
    if [[ ! -f "$stage/$required" ]]; then
        echo "Transfer is incomplete" >&2
        exit 2
    fi
done

# A failed build leaves runtime/app untouched. The unique stage is retained for
# diagnosis, never removed by this script or a concurrent invocation.
docker build --file "$stage/deploy/nas/Dockerfile" --tag "$image" "$stage"

# Verify that the generated image can execute under the runtime user without network
# and without mounts before replacing runtime/app. A failure retains the existing app.
if ! docker run --rm --network none "$image" python3 archive.py --help >/dev/null; then
    echo "Runtime pre-flight check failed for $image as the configured runtime user; retaining existing app" >&2
    exit 1
fi

release=""
old_app_moved=0
rollback() {
    status=$?
    if (( old_app_moved )) && [[ ! -e "$app" && -d "$release/app" ]]; then
        mv -- "$release/app" "$app" || echo "Rollback failed: $release/app" >&2
    fi
    exit "$status"
}
trap rollback ERR
if [[ -d "$app" ]]; then
    release="$(mktemp -d "$runtime/releases/app.XXXXXXXX")"
    mv -- "$app" "$release/app"
    old_app_moved=1
fi
mv -- "$stage" "$app"
trap - ERR
printf "Built %s; active source: %s; previous source: %s\n" "$image" "$app" "${release:-none}" >&2
REMOTE_SCRIPT
)
script_arg="$(encode_arg "$remote_script")"

echo "Preparing NAS collector source at $NAS_HOST:$NAS_ROOT" >&2
export COPYFILE_DISABLE=1
tar -C "$REPO_ROOT" -cf - "${source_files[@]}" |
    ssh "$NAS_HOST" "bash -c \"\$(printf %s $script_arg | base64 -d)\" _ $root_arg $image_arg"
printf '{"status":"ok"}\n'
