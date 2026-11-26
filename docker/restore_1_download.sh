#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/platform-common.sh"
[[ $# == 1 ]] || fail 'Usage: restore_1_download.sh --list | slot-N.sql.gz'
backup_context
backup_objects > "$work/objects.json"
if [[ $1 == --list ]]; then
    jq -r '.[] | [.timeCreated, .name, .metadata.release] | @tsv' "$work/objects.json"
    exit
fi
[[ $1 =~ ^slot-[1-7]\.sql\.gz$ ]] || fail 'Choose a slot from --list.'
object=$(jq -ce --arg slot "$1" '.[] | select(.name == $slot)' "$work/objects.json")
admin=$(jq -er '.metadata.admin | select(test("^[a-zA-Z0-9_]+$"))' <<< "$object")
image=$(jq -er '.metadata.image | select(test("^postgres:17@sha256:[a-f0-9]{64}$"))' <<< "$object")
generation=$(jq -r '.generation' <<< "$object")
gcloud storage cp "gs://$bucket/$1#$generation" "$work/backup.sql.gz" --quiet
gzip -t "$work/backup.sql.gz"

volume="data-platform-restore-$(date -u +%Y%m%dT%H%M%SZ)-$$"
container="$volume"
if docker volume inspect "$volume" > /dev/null 2>&1; then fail 'Restore volume already exists.'; fi
volume_created=false
container_created=false
success=false
cleanup() {
    if "$container_created"; then
        docker stop --time 60 "$container" > /dev/null
        docker rm "$container" > /dev/null
    fi
    if "$volume_created" && ! "$success"; then docker volume rm "$volume" > /dev/null; fi
    rm -rf "$work"
}
trap cleanup EXIT
docker volume create --label com.docker.compose.project=data-platform \
    --label com.docker.compose.volume=warehouse_pgdata \
    --label data-platform.restore=true "$volume" > /dev/null
volume_created=true
docker create --name "$container" --network none \
    -v "$volume:/var/lib/postgresql/data" -e POSTGRES_HOST_AUTH_METHOD=trust \
    -e POSTGRES_USER="$admin" -e POSTGRES_DB=postgres "$image" > /dev/null
container_created=true
docker start "$container" > /dev/null
ready=false
for ((attempt=0; attempt<120; attempt++)); do
    # The entrypoint's temporary server has no TCP listener.
    if docker exec "$container" pg_isready -h 127.0.0.1 -U "$admin" -d postgres > /dev/null 2>&1; then ready=true; break; fi
    sleep 1
done
"$ready" || fail 'Restore Postgres did not start.'
# Skip only the first CREATE ROLE: identical text later could be table data.
gzip -dc "$work/backup.sql.gz" |
    awk -v role="CREATE ROLE $admin;" -v quoted="CREATE ROLE \"$admin\";" \
        '!skipped && ($0 == role || $0 == quoted) {skipped=1; next} {print}' |
    docker exec -i "$container" psql -X -U "$admin" -d postgres -v ON_ERROR_STOP=1 > /dev/null
success=true
cleanup
trap - EXIT
echo "Restored successfully into volume: $volume"
echo "Production is unchanged. Next: sudo bash $release/docker/restore_2_swap.sh $volume"
