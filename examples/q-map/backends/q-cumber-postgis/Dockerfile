FROM postgis/postgis:16-3.4

# Use a subdirectory so initdb works even when the mount point
# contains hidden files (.lost+found, etc.)
ENV PGDATA=/var/lib/postgresql/data/pgdata

COPY scripts/restore-dumps.sh /usr/local/bin/restore-dumps.sh
COPY scripts/entrypoint-wrapper.sh /usr/local/bin/entrypoint-wrapper.sh
RUN chmod +x /usr/local/bin/restore-dumps.sh /usr/local/bin/entrypoint-wrapper.sh

COPY sql/init /docker-entrypoint-initdb.d

# Embed pgdump files into the image so deploys are self-contained.
# Local dev can still override via volume mount (-v ./dumps:/dumps:ro).
COPY dumps/*.pgdump /dumps/

ENTRYPOINT ["entrypoint-wrapper.sh"]
CMD ["postgres"]
