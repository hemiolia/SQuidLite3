FROM debian:bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RCLONE_CONFIG=/secrets/rclone.conf \
    GNUPGHOME=/tmp/gnupg \
    TZ=Asia/Tokyo

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    sqlite3 \
    zstd \
    gnupg \
    rclone \
    ca-certificates \
    util-linux \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY scripts/nas_create_verified_backup.sh /app/scripts/
COPY scripts/verified_backup_support.py /app/scripts/
COPY scripts/nas_backup_cycle.py /app/scripts/
COPY scripts/nas_backup_scheduler.py /app/scripts/

RUN find /app/scripts -type d -exec chmod 755 {} + \
    && find /app/scripts -type f -exec chmod 755 {} +

USER 1000:10

ENTRYPOINT ["python3", "/app/scripts/nas_backup_cycle.py"]
