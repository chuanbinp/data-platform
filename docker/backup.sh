#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/platform-common.sh"
backup_context
check_backup_bucket
backup_objects > "$work/objects.json"

# Fill empty slots, then atomically overwrite the oldest completed backup.
slot=$(jq -r '([range(1;8) | "slot-\(.).sql.gz"] - map(.name))[0] // .[0].name' "$work/objects.json")
generation=$(jq -r --arg slot "$slot" '[.[] | select(.name == $slot) | .generation][0] // "0"' "$work/objects.json")
config=$(compose config --format json)
admin=$(jq -r '.services.postgres.environment.POSTGRES_USER' <<< "$config")
image=$(jq -r '.services.postgres.image' <<< "$config")
unset config

postgres pg_dumpall --no-password | gzip > "$work/backup.sql.gz"
gzip -t "$work/backup.sql.gz"
# Composite uploads would create extra temporary objects in the bucket.
CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_ENABLED=false \
    gcloud storage cp "$work/backup.sql.gz" "gs://$bucket/$slot" \
        --if-generation-match="$generation" \
        --custom-metadata="admin=$admin,image=$image,release=$(basename "$release")" --quiet
echo "Backup complete: gs://$bucket/$slot"
