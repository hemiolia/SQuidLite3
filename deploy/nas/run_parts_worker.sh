#!/usr/bin/env bash
# SQuidLite3: 正本から意味単位の部品を作り、Google Drive へ送って SHA-256 で照合する常駐 worker を NAS で起動する。
# 使い方（NAS 上で）: bash run_parts_worker.sh <コードのディレクトリ>
# 正本 DB は mode=ro で読むだけ。database ディレクトリを書込み可で載せるのは SQLite の -shm のため。
set -euo pipefail
umask 077

CODE_DIR="${1:?コードのディレクトリを指定してください}"
ROOT=/home/Natsuki/ikaring-archive
NAME=squidlite3-parts
IMAGE="${SQUIDLITE3_IMAGE:-ikaring-archive:records-cli-20260928}"
RCLONE_BIN=/home/Natsuki/squidlite3-tools/bin/rclone
RCLONE_CONF_DIR="$ROOT/secrets/squidlite3-drive"
OUT="$ROOT/database/parts"
STATE="$ROOT/state/parts"
WORK="$ROOT/runtime/parts-work"

[[ -d "$CODE_DIR" && -f "$CODE_DIR/scripts/parts_worker.py" ]] || { echo "コードが見つかりません: $CODE_DIR" >&2; exit 2; }
[[ -x "$RCLONE_BIN" ]] || { echo "rclone が見つかりません: $RCLONE_BIN" >&2; exit 2; }
[[ -f "$RCLONE_CONF_DIR/rclone.conf" ]] || { echo "rclone 設定が見つかりません" >&2; exit 2; }
if docker ps -a --format '{{.Names}}' | grep -qx "$NAME"; then
  echo "同名のコンテナがあります。既存を止めて消してから実行してください: $NAME" >&2; exit 3
fi
mkdir -p "$OUT" "$STATE" "$WORK"

docker run -d --name "$NAME" --restart unless-stopped \
  --user 1000:10 --cap-drop ALL --security-opt no-new-privileges \
  -e HOME=/tmp -e SQLITE_TMPDIR=/w/work -e PYTHONDONTWRITEBYTECODE=1 -e TZ=Asia/Tokyo \
  --tmpfs /tmp \
  -v "$CODE_DIR":/app:ro \
  -v "$RCLONE_BIN":/usr/local/bin/rclone-sq3:ro \
  -v "$RCLONE_CONF_DIR":/w/rclone \
  -v "$ROOT/database":/data/database \
  -v "$STATE":/w/state \
  -v "$WORK":/w/work \
  -w /app "$IMAGE" \
  python3 scripts/parts_worker.py \
    --source /data/database/archive.sqlite3 \
    --out /data/database/parts \
    --state /w/state/state.sqlite3 \
    --work-dir /w/work \
    --remote squidlite3_drive:db \
    --rclone /usr/local/bin/rclone-sq3 \
    --rclone-config /w/rclone/rclone.conf \
    --interval 60
docker ps --filter name="$NAME" --format '{{.Names}} {{.Status}}'
