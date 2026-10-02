# Build only from a clean, allowlisted staging context containing the three
# public scripts copied below. Keep this as a uniquely tagged candidate image;
# do not retag or replace the running backup image during an active cycle.
FROM ikaring-archive-backup:current
USER root
COPY verified_backup_support.py /app/scripts/verified_backup_support.py
COPY nas_backup_cycle.py /app/scripts/nas_backup_cycle.py
COPY nas_create_verified_backup.sh /app/scripts/nas_create_verified_backup.sh
RUN chmod 755 /app/scripts/verified_backup_support.py \
        /app/scripts/nas_backup_cycle.py \
        /app/scripts/nas_create_verified_backup.sh \
    && python3 -m py_compile /app/scripts/verified_backup_support.py /app/scripts/nas_backup_cycle.py
USER 1000:10
