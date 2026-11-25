#!/usr/bin/env bash
# Run on the VM: bash /opt/data-platform/releases/RELEASE/docker/deploy.sh
set -euo pipefail
umask 077

root=/opt/data-platform
release=$(cd "$(dirname "$0")/.." && pwd -P)
[[ "$release" == "$root"/releases/* ]] || { echo "Run from a release directory." >&2; exit 1; }
[[ -f "$root/.env" && -s "$root/tunnel-token" && -s "$root/dashboard-url" ]]
[[ -s "$release/images.env" ]]
[[ $(stat -c %a "$root/.env") == 600 ]] || { echo ".env must have mode 600." >&2; exit 1; }

# Both Actions and manual rollbacks use the same host-side lock.
exec 9>"$root/deploy.lock"
flock -w 1800 9
# Keep Postgres's mount path stable across releases to avoid unnecessary restarts.
entrypoint="$root/postgres-entrypoint.sh"
if [[ ! -f "$entrypoint" ]]; then
    install -m 644 "$release/docker/postgres-entrypoint.sh" "$entrypoint"
fi
cmp -s "$release/docker/postgres-entrypoint.sh" "$entrypoint" || {
    echo "Postgres startup script changed; update it during database maintenance first." >&2
    exit 1
}

compose() {
    docker compose --project-name data-platform --project-directory "$release" \
        --env-file "$root/.env" --env-file "$release/images.env" \
        -f "$release/compose.yaml" -f "$release/compose.production.yaml" "$@"
}

postgres() {
    # Credentials expand in the container, not on the host.
    # shellcheck disable=SC2016
    compose exec -T postgres sh -c '
        export PGHOST=127.0.0.1 PGUSER="$POSTGRES_USER" PGPASSWORD="$POSTGRES_PASSWORD"
        exec "$@"
    ' sh "$@"
}

sql() {
    postgres psql -d "$1" -At -v ON_ERROR_STOP=1 -c "$2"
}

url=$(cat "$root/dashboard-url")
[[ "$url" =~ ^https://[a-zA-Z0-9.-]+$ ]] || { echo "Set dashboard-url to https://YOUR_HOSTNAME (no trailing slash)." >&2; exit 1; }
compose config --quiet

paused_by_deploy=false
on_exit() {
    status=$?
    if (( status != 0 )); then
        echo "Deployment failed. Previous release files and all volumes are retained." >&2
        compose ps >&2 || true
        if "$paused_by_deploy"; then
            # Airflow may already be stopped; restore scheduling directly in metadata.
            sql airflow "UPDATE dag SET is_paused = false WHERE dag_id = 'eia930_ingestion'" > /dev/null || true
        fi
    fi
}
trap on_exit EXIT

echo "Pulling release images"
compose --profile dbt pull
if [[ -z $(compose ps --status running -q postgres) ]]; then
    compose up -d --no-deps --no-build --wait --wait-timeout 180 postgres
fi

metadata_exists=$(sql postgres "SELECT count(*) FROM pg_database WHERE datname = 'airflow'")
prior_paused=""
if [[ "$metadata_exists" == 1 ]] && [[ $(sql airflow "SELECT to_regclass('public.dag') IS NOT NULL") == t ]]; then
    prior_paused=$(sql airflow "SELECT is_paused FROM dag WHERE dag_id = 'eia930_ingestion'")
    if [[ "$prior_paused" == f ]]; then
        compose exec -T airflow-api-server airflow dags pause eia930_ingestion
        paused_by_deploy=true
    fi
    echo "Waiting for existing tasks to finish"
    deadline=$((SECONDS + 1500))
    while [[ $(sql airflow "SELECT count(*) FROM task_instance WHERE state IN ('running', 'queued')") != 0 ]]; do
        (( SECONDS < deadline )) || { echo "Tasks did not drain; deployment stopped." >&2; exit 1; }
        sleep 10
    done
fi

compose stop airflow-scheduler airflow-dag-processor airflow-api-server
mkdir -p "$root/backups"
backup="$root/backups/$(date -u +%Y%m%dT%H%M%SZ)-$(basename "$release").sql.gz"
echo "Backing up databases before initialization"
postgres pg_dumpall | gzip > "$backup.partial"
mv "$backup.partial" "$backup"

echo "Preparing database accounts and Airflow metadata"
compose up -d --no-deps --no-build --wait --wait-timeout 180 postgres
compose run --rm --no-deps database-init
compose run --rm --no-deps airflow-init
# The one-off runs above completed initialization; do not start them again via dependencies.
compose up -d --no-deps --no-build --wait --wait-timeout 300 \
    dashboard airflow-api-server airflow-scheduler airflow-dag-processor cloudflared

echo "Checking tunnel connectivity and public dashboard"
compose exec -T dashboard python - <<'PY'
import time
import urllib.request

for attempt in range(30):
    try:
        urllib.request.urlopen("http://cloudflared:2000/ready", timeout=5)
        break
    except OSError:
        if attempt == 29:
            raise SystemExit("Cloudflare tunnel did not become ready")
        time.sleep(2)
PY
curl --fail --silent --show-error --retry 12 --retry-all-errors --retry-delay 5 \
    --connect-timeout 10 --max-time 20 "$url/api/health" > /dev/null

echo "Waiting for EIA-930 DAG discovery"
deadline=$((SECONDS + 180))
until [[ $(sql airflow "SELECT count(*) FROM dag WHERE dag_id = 'eia930_ingestion'") == 1 ]]; do
    (( SECONDS < deadline )) || { echo "DAG was not discovered." >&2; exit 1; }
    sleep 5
done
if [[ "$prior_paused" != t ]]; then
    compose exec -T airflow-api-server airflow dags unpause eia930_ingestion
fi
paused_by_deploy=false

previous=$(readlink -f "$root/current" || true)
if [[ -n "$previous" && "$previous" != "$release" ]]; then
    ln -sfn "$previous" "$root/previous"
fi
ln -sfn "$release" "$root/current"
echo "Deployed $(basename "$release"). Backup: $backup"
