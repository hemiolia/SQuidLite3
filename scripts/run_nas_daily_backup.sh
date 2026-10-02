#!/usr/bin/env bash
set -euo pipefail
umask 077

ROOT_DIR="/home/Natsuki/ikaring-archive"

command -v docker >/dev/null 2>&1 || {
  echo "Error: docker binary not found" >&2
  exit 1
}

BACKUP_DIR="${ROOT_DIR}/backups/daily"
LOG_DIR="${ROOT_DIR}/logs/backup"

mkdir -p "${BACKUP_DIR}" "${LOG_DIR}"

LOCK_FILE="${LOG_DIR}/.daily.lock"
exec 200>"${LOCK_FILE}"
if ! flock -n 200; then
  echo "Error: Another daily backup is already running" >&2
  exit 1
fi

TIMESTAMP="$(date +'%Y%m%d_%H%M%S')"
LOG_FILE="$(mktemp "${LOG_DIR}/backup_${TIMESTAMP}_XXXXXX.log")"

set +e
docker run \
  --rm \
  --name ikaring-archive-backup \
  --user 1000:10 \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  -v "${ROOT_DIR}/database:/database:rw" \
  -v "${BACKUP_DIR}:/backups:rw" \
  -v "${ROOT_DIR}/secrets/backup:/secrets:rw" \
  -e RCLONE_CONFIG=/secrets/rclone-write.conf \
  ikaring-archive-backup:current \
  --db /database/archive.sqlite3 \
  --backup-dir /backups \
  --passphrase-file /secrets/gdrive-backup-passphrase.txt \
  --remote ikaring_offsite: \
  >"${LOG_FILE}" 2>&1
EXIT_CODE=$?
set -e

exit "${EXIT_CODE}"
