#!/usr/bin/env bash
set -euo pipefail
umask 077

echo "Error: 2026-09-29 rule. New backups are plaintext. This encryption path is disabled. Use verified_backup_support.py plaintext-snapshot." >&2
exit 2

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <DB_PATH> <BACKUP_DIR> <PASSPHRASE_FILE>" >&2
  exit 1
fi

DB_PATH="$1"
BACKUP_DIR="$2"
PASSPHRASE_FILE="$3"

case "$DB_PATH" in
  /*) ;;
  *) echo "Error: DB_PATH must be an absolute path: $DB_PATH" >&2; exit 1 ;;
esac
case "$BACKUP_DIR" in
  /*) ;;
  *) echo "Error: BACKUP_DIR must be an absolute path: $BACKUP_DIR" >&2; exit 1 ;;
esac
case "$PASSPHRASE_FILE" in
  /*) ;;
  *) echo "Error: PASSPHRASE_FILE must be an absolute path: $PASSPHRASE_FILE" >&2; exit 1 ;;
esac

if [ ! -f "$DB_PATH" ]; then
  echo "Error: Database file does not exist: $DB_PATH" >&2
  exit 1
fi

if [ ! -f "$PASSPHRASE_FILE" ]; then
  echo "Error: Passphrase file does not exist: $PASSPHRASE_FILE" >&2
  exit 1
fi

mkdir -p "$BACKUP_DIR"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SUPPORT_PY="${SCRIPT_DIR}/verified_backup_support.py"

if [ ! -f "$SUPPORT_PY" ]; then
  echo "Error: Required helper script not found: $SUPPORT_PY" >&2
  exit 1
fi

LOCK_FILE="${BACKUP_DIR}/.nas_create_verified_backup.lock"
exec 200>"$LOCK_FILE"
if command -v flock >/dev/null 2>&1; then
  if ! flock -n 200; then
    echo "Error: Another backup process is already running." >&2
    exit 1
  fi
else
  if ! python3 -c 'import fcntl, sys; fcntl.flock(200, fcntl.LOCK_EX | fcntl.LOCK_NB)' 2>/dev/null; then
    echo "Error: Another backup process is already running." >&2
    exit 1
  fi
fi

# PRAGMA page_count*page_sizeを基準に4倍+1GiB空き容量を事前検査
python3 "$SUPPORT_PY" check-space "$DB_PATH" "$BACKUP_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
ISO_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
UUID=$(python3 -c 'import uuid; print(uuid.uuid4().hex)')

# DB basenameはarchive固定の安全文字列にする
DB_STEM="archive"
RAW_BASENAME="${DB_STEM}_${TIMESTAMP}_${UUID}.sqlite3"
ENC_BASENAME="${RAW_BASENAME}.zst.gpg"
MANIFEST_BASENAME="${RAW_BASENAME}.manifest.json"

RAW_FINAL="${BACKUP_DIR}/${RAW_BASENAME}"
ENC_FINAL="${BACKUP_DIR}/${ENC_BASENAME}"
MANIFEST_FINAL="${BACKUP_DIR}/${MANIFEST_BASENAME}"

if [ -e "$RAW_FINAL" ] || [ -e "$ENC_FINAL" ] || [ -e "$MANIFEST_FINAL" ]; then
  echo "Error: Destination backup target already exists for basename: $RAW_BASENAME" >&2
  exit 1
fi

# 一時ファイルはmktempで当該実行専用
RAW_TMP=$(mktemp "${BACKUP_DIR}/.tmp_raw_${TIMESTAMP}_${UUID}_XXXXXX")
ENC_TMP=$(mktemp "${BACKUP_DIR}/.tmp_enc_${TIMESTAMP}_${UUID}_XXXXXX")
RESTORE_TMP=$(mktemp "${BACKUP_DIR}/.tmp_restore_${TIMESTAMP}_${UUID}_XXXXXX")
MANIFEST_TMP=$(mktemp "${BACKUP_DIR}/.tmp_manifest_${TIMESTAMP}_${UUID}_XXXXXX")

# cleanupはPython pathlib unlinkで厳密に自分のtmpのみ（rm変数禁止）
cleanup() {
  local exit_code=$?
  python3 -c '
import pathlib, sys
backup_dir = pathlib.Path(sys.argv[1]).resolve()
for arg in sys.argv[2:]:
    if not arg:
        continue
    try:
        p = pathlib.Path(arg).resolve()
        if p.parent == backup_dir and p.name.startswith(".tmp_") and (p.is_file() or p.is_symlink()):
            p.unlink(missing_ok=True)
    except Exception:
        pass
' "$BACKUP_DIR" "${RAW_TMP:-}" "${ENC_TMP:-}" "${RESTORE_TMP:-}" "${MANIFEST_TMP:-}"
  exit "$exit_code"
}
trap cleanup EXIT INT TERM

# SQLite path引用はPython sqlite3 backup API利用で回避、readonly sourceからbackupしquick_check
python3 "$SUPPORT_PY" backup-db "$DB_PATH" "$RAW_TMP"
RAW_CHECK="ok"

RAW_INFO=$(python3 "$SUPPORT_PY" hash-file-sha256 "$RAW_TMP")
RAW_BYTES=$(echo "$RAW_INFO" | awk '{print $1}')
RAW_SHA256=$(echo "$RAW_INFO" | awk '{print $2}')

zstd -c "$RAW_TMP" | gpg --batch --yes --pinentry-mode loopback --passphrase-file "$PASSPHRASE_FILE" --symmetric --cipher-algo AES256 -o "$ENC_TMP"

ENC_INFO=$(python3 "$SUPPORT_PY" hash-file-sha256 "$ENC_TMP")
ENC_BYTES=$(echo "$ENC_INFO" | awk '{print $1}')
ENC_SHA256=$(echo "$ENC_INFO" | awk '{print $2}')

gpg --batch --yes --pinentry-mode loopback --passphrase-file "$PASSPHRASE_FILE" --decrypt "$ENC_TMP" | zstd -d -c > "$RESTORE_TMP"

RESTORE_INFO=$(python3 "$SUPPORT_PY" hash-file-sha256 "$RESTORE_TMP")
RESTORE_SHA256=$(echo "$RESTORE_INFO" | awk '{print $2}')
if [ "$RESTORE_SHA256" != "$RAW_SHA256" ]; then
  echo "Error: Restored snapshot SHA-256 mismatch (restored: $RESTORE_SHA256, raw: $RAW_SHA256)" >&2
  exit 1
fi

python3 "$SUPPORT_PY" quick-check "$RESTORE_TMP"

# 検証が成功したRESTORE_TMPを即時安全削除
python3 "$SUPPORT_PY" safe-unlink "$BACKUP_DIR" ".tmp_restore_" "$RESTORE_TMP"
RESTORE_TMP=""

# manifestはPython json.dumpで安全出力
python3 "$SUPPORT_PY" write-manifest \
  --output "$MANIFEST_TMP" \
  --timestamp "$ISO_TIME" \
  --raw-basename "$RAW_BASENAME" \
  --raw-bytes "$RAW_BYTES" \
  --raw-sha256 "$RAW_SHA256" \
  --raw-quick-check "$RAW_CHECK" \
  --enc-basename "$ENC_BASENAME" \
  --enc-bytes "$ENC_BYTES" \
  --enc-sha256 "$ENC_SHA256" \
  --compression "zstd" \
  --cipher "AES256"

# 成功まで既存確定ファイル不変: 全検証完了後に原子的mvで確定
if [ -e "$RAW_FINAL" ] || [ -e "$ENC_FINAL" ] || [ -e "$MANIFEST_FINAL" ]; then
  echo "Error: Destination backup target already exists for basename: $RAW_BASENAME" >&2
  exit 1
fi

mv "$RAW_TMP" "$RAW_FINAL"
RAW_TMP=""

mv "$ENC_TMP" "$ENC_FINAL"
ENC_TMP=""

mv "$MANIFEST_TMP" "$MANIFEST_FINAL"
MANIFEST_TMP=""

# JSON出力エスケープをPython利用
python3 "$SUPPORT_PY" json-output \
  "status=ok" \
  "raw_path=${RAW_FINAL}" \
  "encrypted_path=${ENC_FINAL}" \
  "manifest_path=${MANIFEST_FINAL}"
