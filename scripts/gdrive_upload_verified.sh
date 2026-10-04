#!/usr/bin/env bash
set -euo pipefail
umask 077

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "Usage: $0 <REMOTE_ENC_PATH> <REMOTE_MANIFEST_PATH> [RCLONE_REMOTE_DIR]" >&2
  exit 1
fi

REMOTE_ENC_PATH="$1"
REMOTE_MANIFEST_PATH="$2"
RCLONE_REMOTE_DIR="${3:-gdrive_origin:SQuidLite3/backups}"
NAS_HOST="${IKARING_NAS_HOST:-nas}"

case "$REMOTE_ENC_PATH" in
  /*) ;;
  *) echo "Error: REMOTE_ENC_PATH must be an absolute path: $REMOTE_ENC_PATH" >&2; exit 1 ;;
esac
case "$REMOTE_MANIFEST_PATH" in
  /*) ;;
  *) echo "Error: REMOTE_MANIFEST_PATH must be an absolute path: $REMOTE_MANIFEST_PATH" >&2; exit 1 ;;
esac

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SUPPORT_PY="${SCRIPT_DIR}/verified_backup_support.py"

if [ ! -f "$SUPPORT_PY" ]; then
  echo "Error: Required helper script not found: $SUPPORT_PY" >&2
  exit 1
fi

ENC_BASENAME=$(basename "$REMOTE_ENC_PATH")
MANIFEST_BASENAME=$(basename "$REMOTE_MANIFEST_PATH")
NAS_BACKUP_DIR=$(dirname "$REMOTE_ENC_PATH")

UUID=$(python3 -c 'import uuid; print(uuid.uuid4().hex)')

# SSH引数shlex.quote相当を使いパス特殊文字でコマンド注入不可
SSH_ENC_PATH=$(python3 "$SUPPORT_PY" quote "$REMOTE_ENC_PATH")
SSH_MANIFEST_PATH=$(python3 "$SUPPORT_PY" quote "$REMOTE_MANIFEST_PATH")
SSH_BACKUP_DIR=$(python3 "$SUPPORT_PY" quote "$NAS_BACKUP_DIR")
SSH_UUID=$(python3 "$SUPPORT_PY" quote "$UUID")

NAS_VERIFY_TMP=""
TMP_ENC_REMOTE=""
TMP_MANIFEST_REMOTE=""

# cleanupはPython unlink自分のtmpのみ、失敗時既存object削除禁止
cleanup() {
  local exit_code=$?
  if [ -n "${NAS_VERIFY_TMP:-}" ]; then
    local ssh_verify_tmp
    ssh_verify_tmp=$(python3 -c 'import shlex, sys; print(shlex.quote(sys.argv[1]))' "$NAS_VERIFY_TMP")
    ssh "$NAS_HOST" "python3 -c '
import pathlib, sys
backup_dir = pathlib.Path(sys.argv[1]).resolve()
p = pathlib.Path(sys.argv[2]).resolve()
if p.parent == backup_dir and p.name.startswith(\".gdrive_verify_\") and (p.is_file() or p.is_symlink()):
    p.unlink(missing_ok=True)
' $SSH_BACKUP_DIR $ssh_verify_tmp" 2>/dev/null || true
  fi

  # 失敗時既存object削除禁止: 一時アップロードオブジェクトのみ安全に削除し既存物は残す
  if [ -n "${TMP_ENC_REMOTE:-}" ]; then
    rclone deletefile "$TMP_ENC_REMOTE" 2>/dev/null || true
  fi
  if [ -n "${TMP_MANIFEST_REMOTE:-}" ]; then
    rclone deletefile "$TMP_MANIFEST_REMOTE" 2>/dev/null || true
  fi
  exit "$exit_code"
}
trap cleanup EXIT INT TERM

# sshでsource size, sha256, md5を取得（値のみ、秘密なし）
if ! SRC_ENC_INFO=$(ssh "$NAS_HOST" "python3 -c '
import hashlib, os, sys
p = sys.argv[1]
if not os.path.isfile(p):
    sys.stderr.write(f\"Source encrypted file not found: {p}\n\")
    sys.exit(1)
st = os.stat(p)
h_sha = hashlib.sha256()
h_md5 = hashlib.md5()
with open(p, \"rb\") as f:
    for chunk in iter(lambda: f.read(1048576), b\"\"):
        h_sha.update(chunk)
        h_md5.update(chunk)
print(f\"{st.st_size} {h_sha.hexdigest()} {h_md5.hexdigest()}\")
' $SSH_ENC_PATH"); then
  echo "Error: Failed to inspect source encrypted file on NAS: $REMOTE_ENC_PATH" >&2
  exit 1
fi

SRC_ENC_SIZE=$(echo "$SRC_ENC_INFO" | awk '{print $1}')
SRC_ENC_SHA256=$(echo "$SRC_ENC_INFO" | awk '{print $2}')
SRC_ENC_MD5=$(echo "$SRC_ENC_INFO" | awk '{print $3}')

if ! SRC_MANIFEST_INFO=$(ssh "$NAS_HOST" "python3 -c '
import hashlib, os, sys
p = sys.argv[1]
if not os.path.isfile(p):
    sys.stderr.write(f\"Source manifest file not found: {p}\n\")
    sys.exit(1)
st = os.stat(p)
h_sha = hashlib.sha256()
h_md5 = hashlib.md5()
with open(p, \"rb\") as f:
    for chunk in iter(lambda: f.read(1048576), b\"\"):
        h_sha.update(chunk)
        h_md5.update(chunk)
print(f\"{st.st_size} {h_sha.hexdigest()} {h_md5.hexdigest()}\")
' $SSH_MANIFEST_PATH"); then
  echo "Error: Failed to inspect source manifest file on NAS: $REMOTE_MANIFEST_PATH" >&2
  exit 1
fi

SRC_MANIFEST_SIZE=$(echo "$SRC_MANIFEST_INFO" | awk '{print $1}')
SRC_MANIFEST_SHA256=$(echo "$SRC_MANIFEST_INFO" | awk '{print $2}')
SRC_MANIFEST_MD5=$(echo "$SRC_MANIFEST_INFO" | awk '{print $3}')

# manifestをNASで読み取りJSON検証
if ! SRC_MANIFEST_CONTENT=$(ssh "$NAS_HOST" "python3 -c '
import sys, pathlib
p = pathlib.Path(sys.argv[1])
if not p.is_file():
    sys.stderr.write(f\"Manifest file not found: {p}\n\")
    sys.exit(1)
sys.stdout.write(p.read_text(encoding=\"utf-8\"))
' $SSH_MANIFEST_PATH"); then
  echo "Error: Failed to read manifest content from NAS: $REMOTE_MANIFEST_PATH" >&2
  exit 1
fi

# encrypted basename/bytes/sha256一致、verification.sha256_match=true、quick_check=okをupload前に検証。不一致なら送信なし。
python3 "$SUPPORT_PY" verify-manifest-content \
  --manifest "$SRC_MANIFEST_CONTENT" \
  --enc-basename "$ENC_BASENAME" \
  --src-enc-size "$SRC_ENC_SIZE" \
  --src-enc-sha256 "$SRC_ENC_SHA256"

TARGET_ENC_REMOTE="${RCLONE_REMOTE_DIR}/${ENC_BASENAME}"
TARGET_MANIFEST_REMOTE="${RCLONE_REMOTE_DIR}/${MANIFEST_BASENAME}"

# ローカル/remote listing失敗を不存在扱いしない
if ! REMOTE_LISTING=$(rclone lsf "$RCLONE_REMOTE_DIR"); then
  echo "Error: Failed to list remote directory: $RCLONE_REMOTE_DIR" >&2
  exit 1
fi

# 同名存在拒否
if echo "$REMOTE_LISTING" | grep -Fxq "$ENC_BASENAME"; then
  echo "Error: Remote target already exists: $TARGET_ENC_REMOTE" >&2
  exit 1
fi
if echo "$REMOTE_LISTING" | grep -Fxq "$MANIFEST_BASENAME"; then
  echo "Error: Remote target already exists: $TARGET_MANIFEST_REMOTE" >&2
  exit 1
fi

TMP_ENC_REMOTE="${RCLONE_REMOTE_DIR}/${ENC_BASENAME}.uploading-${UUID}"

# rclone rcatで remote filename.uploading-UUID へstream upload
ssh "$NAS_HOST" "cat -- $SSH_ENC_PATH" | rclone rcat "$TMP_ENC_REMOTE"

# 確定前にuploadingオブジェクトのsize/md5検証し--immutableでmoveto
if ! TMP_ENC_SIZE=$(rclone size --json "$TMP_ENC_REMOTE" | python3 -c 'import sys, json; print(json.load(sys.stdin)["bytes"])'); then
  echo "Error: Failed to get size of uploading encrypted file: $TMP_ENC_REMOTE" >&2
  exit 1
fi
if ! TMP_ENC_MD5=$(rclone md5sum "$TMP_ENC_REMOTE" | awk '{print $1}'); then
  echo "Error: Failed to get MD5 of uploading encrypted file: $TMP_ENC_REMOTE" >&2
  exit 1
fi

if [ "$TMP_ENC_SIZE" -ne "$SRC_ENC_SIZE" ]; then
  echo "Error: Uploading encrypted file size mismatch: remote $TMP_ENC_SIZE vs source $SRC_ENC_SIZE" >&2
  exit 1
fi
if [ "$TMP_ENC_MD5" != "$SRC_ENC_MD5" ]; then
  echo "Error: Uploading encrypted file MD5 mismatch: remote $TMP_ENC_MD5 vs source $SRC_ENC_MD5" >&2
  exit 1
fi

rclone moveto --immutable "$TMP_ENC_REMOTE" "$TARGET_ENC_REMOTE"
TMP_ENC_REMOTE=""

# NAS一時ファイルはmode600で独占作成
if ! NAS_VERIFY_TMP=$(ssh "$NAS_HOST" "python3 -c '
import os, sys, tempfile, pathlib
backup_dir = pathlib.Path(sys.argv[1]).resolve()
uuid_str = sys.argv[2]
if not backup_dir.is_dir():
    sys.stderr.write(f\"Backup directory does not exist: {backup_dir}\n\")
    sys.exit(1)
fd, path = tempfile.mkstemp(prefix=f\".gdrive_verify_{uuid_str}_\", dir=str(backup_dir))
os.close(fd)
os.chmod(path, 0o600)
print(path)
' $SSH_BACKUP_DIR $SSH_UUID"); then
  echo "Error: Failed to create exclusive temporary verify file on NAS" >&2
  exit 1
fi

SSH_VERIFY_TMP=$(python3 "$SUPPORT_PY" quote "$NAS_VERIFY_TMP")

# 全量roundtrip SHA256検証を維持
rclone cat "$TARGET_ENC_REMOTE" | ssh "$NAS_HOST" "cat > $SSH_VERIFY_TMP"

if ! VERIFY_SHA256=$(ssh "$NAS_HOST" "python3 -c '
import hashlib, os, sys
p = sys.argv[1]
if not os.path.isfile(p):
    sys.stderr.write(f\"Verification temp file not found: {p}\n\")
    sys.exit(1)
h = hashlib.sha256()
with open(p, \"rb\") as f:
    for chunk in iter(lambda: f.read(1048576), b\"\"):
        h.update(chunk)
print(h.hexdigest())
' $SSH_VERIFY_TMP"); then
  echo "Error: Failed to compute SHA-256 of downloaded verification file on NAS" >&2
  exit 1
fi

# NAS一時ファイルをPython pathlib unlinkで厳密に自分のtmpのみ安全削除
ssh "$NAS_HOST" "python3 -c '
import pathlib, sys
backup_dir = pathlib.Path(sys.argv[1]).resolve()
p = pathlib.Path(sys.argv[2]).resolve()
if p.parent == backup_dir and p.name.startswith(\".gdrive_verify_\") and (p.is_file() or p.is_symlink()):
    p.unlink(missing_ok=True)
' $SSH_BACKUP_DIR $SSH_VERIFY_TMP"
NAS_VERIFY_TMP=""

if [ "$VERIFY_SHA256" != "$SRC_ENC_SHA256" ]; then
  echo "Error: Verification roundtrip SHA-256 mismatch: downloaded $VERIFY_SHA256 vs source $SRC_ENC_SHA256" >&2
  exit 1
fi

# manifestはroundtrip成功後の最後に確定
TMP_MANIFEST_REMOTE="${RCLONE_REMOTE_DIR}/${MANIFEST_BASENAME}.uploading-${UUID}"

ssh "$NAS_HOST" "cat -- $SSH_MANIFEST_PATH" | rclone rcat "$TMP_MANIFEST_REMOTE"

if ! TMP_MANIFEST_SIZE=$(rclone size --json "$TMP_MANIFEST_REMOTE" | python3 -c 'import sys, json; print(json.load(sys.stdin)["bytes"])'); then
  echo "Error: Failed to get size of uploading manifest: $TMP_MANIFEST_REMOTE" >&2
  exit 1
fi
if ! TMP_MANIFEST_MD5=$(rclone md5sum "$TMP_MANIFEST_REMOTE" | awk '{print $1}'); then
  echo "Error: Failed to get MD5 of uploading manifest: $TMP_MANIFEST_REMOTE" >&2
  exit 1
fi

if [ "$TMP_MANIFEST_SIZE" -ne "$SRC_MANIFEST_SIZE" ]; then
  echo "Error: Manifest size mismatch: remote $TMP_MANIFEST_SIZE vs source $SRC_MANIFEST_SIZE" >&2
  exit 1
fi
if [ "$TMP_MANIFEST_MD5" != "$SRC_MANIFEST_MD5" ]; then
  echo "Error: Manifest MD5 mismatch: remote $TMP_MANIFEST_MD5 vs source $SRC_MANIFEST_MD5" >&2
  exit 1
fi

rclone moveto --immutable "$TMP_MANIFEST_REMOTE" "$TARGET_MANIFEST_REMOTE"
TMP_MANIFEST_REMOTE=""

# JSON出力エスケープをPython利用
python3 "$SUPPORT_PY" json-output \
  "status=ok" \
  "uploaded_encrypted=${TARGET_ENC_REMOTE}" \
  "uploaded_manifest=${TARGET_MANIFEST_REMOTE}" \
  "bytes=${SRC_ENC_SIZE}" \
  "sha256=${SRC_ENC_SHA256}" \
  "md5=${SRC_ENC_MD5}"
