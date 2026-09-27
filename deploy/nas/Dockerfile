FROM node:22-bookworm-slim

ENV NODE=/usr/local/bin/node \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    IKARING_ARCHIVE_DATA_DIR=/data \
    NXAPI_DATA_PATH=/auth \
    TZ=Asia/Tokyo

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    sqlite3 \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json package-lock.json .npmrc ./
RUN npm ci --omit=dev

# Copy only runtime inputs. Database, authentication, spool and logs are mounts.
COPY archive.py ./
COPY scripts/data_root.py ./scripts/
COPY src/python/ikarchive/ ./src/python/ikarchive/
COPY src/node/ ./src/node/
COPY sql/schema.sql ./sql/
COPY config/font-unicode-ranges.json ./config/
COPY assets/fonts/Splatoon2-Unified.otf ./assets/fonts/

RUN find archive.py scripts src sql config assets -type d -exec chmod 755 {} + \
    && find archive.py scripts src sql config assets -type f -exec chmod 644 {} +

USER 1000:10
CMD ["python3", "archive.py", "--db", "/data/database/archive.sqlite3", "watch", "--interval", "120"]
