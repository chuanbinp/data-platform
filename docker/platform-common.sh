#!/usr/bin/env bash
# Shared production context. Callers set root and release before sourcing.
fail() { echo "$*" >&2; exit 1; }

compose() {
    local env_files=(--env-file "$root/.env" --env-file "$release/images.env")
    if [[ -f "$root/postgres-volume.env" ]]; then
        env_files+=(--env-file "$root/postgres-volume.env")
    fi
    docker compose --project-name data-platform --project-directory "$release" \
        "${env_files[@]}" \
        -f "$release/compose.yaml" -f "$release/compose.production.yaml" "$@"
}

postgres() {
    # Credentials expand in the container, never in the host command line.
    compose exec -T postgres sh -c '
        export PGHOST=127.0.0.1 PGUSER="$POSTGRES_USER" PGPASSWORD="$POSTGRES_PASSWORD"
        exec "$@"
    ' sh "$@"
}

sql() {
    postgres psql -d "$1" -At -v ON_ERROR_STOP=1 -c "$2"
}

platform_context() {
    umask 077
    root=${PLATFORM_ROOT:-/opt/data-platform}
    exec 9>"$root/deploy.lock"
    flock -w 1800 9
    release=$(readlink -f "$root/current")
}

backup_context() {
    platform_context
    load_backup_bucket
    work=$(mktemp -d)
    trap 'rm -rf "$work"' EXIT
}

load_backup_bucket() {
    bucket=$(compose config --environment | sed -n 's/^BACKUP_BUCKET=//p')
    bucket=${bucket:-data-platform-pg-backups}
    [[ "$bucket" =~ ^[a-z0-9][a-z0-9.-]+$ ]] || fail 'Invalid BACKUP_BUCKET in .env.'
}

save_volume_state() {
    {
        printf 'POSTGRES_VOLUME_NAME=%s\n' "$1"
        if [[ -n ${2:-} ]]; then printf 'POSTGRES_PREVIOUS_VOLUME_NAME=%s\n' "$2"; fi
    } > "$root/postgres-volume.env.partial"
    chmod 600 "$root/postgres-volume.env.partial"
    mv "$root/postgres-volume.env.partial" "$root/postgres-volume.env"
}

read_volume_state() {
    local container mounted
    active=$(compose config --format json | jq -er '.volumes.warehouse_pgdata.name')
    previous=""
    if [[ -f "$root/postgres-volume.env" ]]; then
        previous=$(sed -n 's/^POSTGRES_PREVIOUS_VOLUME_NAME=//p' "$root/postgres-volume.env")
    fi
    [[ "$active" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]+$ ]] || fail 'Invalid active volume name.'
    [[ -z "$previous" || "$previous" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]+$ ]] || fail 'Invalid previous volume name.'
    container=$(compose ps -a -q postgres)
    if [[ -n "$container" ]]; then
        mounted=$(docker inspect "$container" | jq -er '.[0].Mounts[] | select(.Destination == "/var/lib/postgresql/data") | .Name')
        [[ "$mounted" == "$active" ]] || fail 'Configured volume differs from the Postgres mount; stopped.'
    fi
}

start_restored_services() {
    compose up -d --no-deps --no-build --wait --wait-timeout 180 postgres
    [[ $(sql postgres 'SELECT 1') == 1 ]] || fail 'Postgres authentication check failed.'
    compose up -d --no-deps --no-build --wait --wait-timeout 300 dashboard airflow-api-server cloudflared
    echo 'Airflow scheduler and DAG processor remain stopped. Review restored tasks before resuming.'
}

check_backup_bucket() {
    gcloud storage buckets describe "gs://$bucket" --raw --format=json | jq -e '
      (.versioning.enabled != true) and
      ((.softDeletePolicy.retentionDurationSeconds // "0" | tonumber) == 0) and
      (.retentionPolicy == null) and ((.lifecycle.rule // []) == []) and
      (.iamConfiguration.publicAccessPrevention == "enforced") and
      (.iamConfiguration.uniformBucketLevelAccess.enabled == true)
    ' > /dev/null || fail 'Backup bucket unavailable or misconfigured. Run docker/setup-gcp.sh first.'
}

# Return at most seven objects, oldest first, including any historical versions.
# Raw API fields keep the small listing independent of CLI display formatting.
backup_objects() {
    curl --fail --silent --show-error --connect-timeout 10 --max-time 60 \
        -H "Authorization: Bearer $(gcloud auth print-access-token)" \
        "https://storage.googleapis.com/storage/v1/b/$bucket/o?versions=true&maxResults=8" |
      jq -e '
        if .nextPageToken then error("Too many backup objects") else .items // [] end |
        if length > 7 or (map(.name) | length != (unique | length)) or
           any(.[]; (.name | test("^slot-[1-7]\\.sql\\.gz$") | not))
        then error("Use a dedicated bucket containing only seven backup slots") else . end |
        sort_by(.timeCreated | rtrimstr("Z"))
      '
}
