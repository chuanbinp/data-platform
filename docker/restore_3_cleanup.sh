#!/usr/bin/env bash
# Accept the active database and delete only its recorded previous volume.
set -euo pipefail
source "$(dirname "$0")/platform-common.sh"
[[ $# == 0 ]] || fail 'Usage: restore_3_cleanup.sh'
platform_context
read_volume_state
[[ -n "$previous" && "$previous" != "$active" ]] || fail 'No distinct previous volume to delete.'
[[ $(sql postgres 'SELECT 1') == 1 ]] || fail 'Active Postgres health check failed.'
attached=$(docker ps -aq --filter "volume=$previous")
[[ -z "$attached" ]] || fail 'Previous volume is attached to a container; cleanup stopped.'
# A retry also clears stale state if deletion succeeded before an interruption.
volumes=$(docker volume ls -q)
if grep -Fxq -- "$previous" <<< "$volumes"; then docker volume rm "$previous"; fi
save_volume_state "$active"
echo "Kept active volume $active. Removed previous volume $previous."
