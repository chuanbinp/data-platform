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
if ! command -v jq > /dev/null; then
    apt-get update
    apt-get install -y jq
fi
# Keep Postgres's mount path stable across releases to avoid unnecessary restarts.
entrypoint="$root/postgres-entrypoint.sh"
if [[ ! -f "$entrypoint" ]]; then
    install -m 644 "$release/docker/postgres-entrypoint.sh" "$entrypoint"
fi
cmp -s "$release/docker/postgres-entrypoint.sh" "$entrypoint" || {
    echo "Postgres startup script changed; update it during database maintenance first." >&2
    exit 1
}

# shellcheck source=platform-common.sh
source "$release/docker/platform-common.sh"

url=$(cat "$root/dashboard-url")
[[ "$url" =~ ^https://[a-zA-Z0-9.-]+$ ]] || { echo "Set dashboard-url to https://YOUR_HOSTNAME (no trailing slash)." >&2; exit 1; }
compose config --quiet
# Validate backup access before changing running services. The timer is enabled
# only at the end, after application health checks pass.
load_backup_bucket
check_backup_bucket
backup_objects > /dev/null

paused_by_deploy=""
on_exit() {
    status=$?
    if (( status != 0 )); then
        echo "Deployment failed. Previous release files and all volumes are retained." >&2
        compose ps >&2 || true
        compose logs --no-color --tail 80 airflow-api-server airflow-scheduler airflow-dag-processor >&2 || true
        # Airflow may already be stopped; restore scheduling directly in metadata.
        for dag_id in $paused_by_deploy; do
            sql airflow "UPDATE dag SET is_paused = false WHERE dag_id = '$dag_id'" > /dev/null || true
        done
    fi
}
trap on_exit EXIT

echo "Pulling release images"
compose --profile dbt pull
if [[ -z $(compose ps --status running -q postgres) ]]; then
    compose up -d --no-deps --no-build --wait --wait-timeout 180 postgres
fi

metadata_exists=$(sql postgres "SELECT count(*) FROM pg_database WHERE datname = 'airflow'")
dag_ids=(eia930_ingestion nws_ingestion)
previously_paused=""
if [[ "$metadata_exists" == 1 ]] && [[ $(sql airflow "SELECT to_regclass('public.dag') IS NOT NULL") == t ]]; then
    for dag_id in "${dag_ids[@]}"; do
        prior_paused=$(sql airflow "SELECT is_paused FROM dag WHERE dag_id = '$dag_id'")
        if [[ "$prior_paused" == t ]]; then
            previously_paused+=" $dag_id"
        elif [[ "$prior_paused" == f ]]; then
            compose exec -T airflow-api-server airflow dags pause "$dag_id"
            paused_by_deploy+=" $dag_id"
        fi
    done
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
echo "Building and validating warehouse views before restarting services"
compose run --rm --no-deps dbt build
# Warm up the API before starting the scheduler and DAG processor on the small VM.
# Initialization already completed; do not start those one-off services again.
echo "Starting dashboard and Airflow API"
compose up -d --no-deps --no-build --wait --wait-timeout 600 \
    dashboard airflow-api-server cloudflared
echo "Starting Airflow scheduler and DAG processor"
compose up -d --no-deps --no-build --wait --wait-timeout 600 \
    airflow-scheduler airflow-dag-processor

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

echo "Waiting for ingestion DAG discovery"
for dag_id in "${dag_ids[@]}"; do
    deadline=$((SECONDS + 180))
    until [[ $(sql airflow "SELECT count(*) FROM dag WHERE dag_id = '$dag_id'") == 1 ]]; do
        (( SECONDS < deadline )) || { echo "DAG $dag_id was not discovered." >&2; exit 1; }
        sleep 5
    done
done
for dag_id in "${dag_ids[@]}"; do
    if [[ " ${previously_paused} " != *" $dag_id "* ]]; then
        compose exec -T airflow-api-server airflow dags unpause "$dag_id"
    fi
done
paused_by_deploy=""

previous=$(readlink -f "$root/current" || true)
if [[ -n "$previous" && "$previous" != "$release" ]]; then
    ln -sfn "$previous" "$root/previous"
fi
ln -sfn "$release" "$root/current"
install -m 644 "$release/docker/data-platform-backup.service" /etc/systemd/system/
install -m 644 "$release/docker/data-platform-backup.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now data-platform-backup.timer
echo "Deployed $(basename "$release"). Backup: $backup"
