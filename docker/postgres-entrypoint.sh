#!/bin/sh
set -eu

case "$POSTGRES_PASSWORD" in
    ''|change-me*|replace-with*) echo 'Set a nonempty, non-example POSTGRES_PASSWORD' >&2; exit 1 ;;
esac

# Names are inserted into pg_hba.conf, never shell commands or SQL.
for name in "$POSTGRES_USER" "$POSTGRES_DB" "$WAREHOUSE_USER" "$VIEWER_USER" "$AIRFLOW_DB_USER"; do
    case "$name" in
        ''|*[!a-zA-Z0-9_]*) echo 'Database/account names must contain only letters, numbers, and underscores' >&2; exit 1 ;;
    esac
done
cat > /tmp/platform-pg_hba.conf <<EOF
local all $POSTGRES_USER scram-sha-256
local all all reject
host all $POSTGRES_USER all scram-sha-256
host $POSTGRES_DB $WAREHOUSE_USER,$VIEWER_USER all scram-sha-256
host airflow $AIRFLOW_DB_USER all scram-sha-256
host all all all reject
EOF
chmod 644 /tmp/platform-pg_hba.conf
exec /usr/local/bin/docker-entrypoint.sh postgres -c hba_file=/tmp/platform-pg_hba.conf "$@"
