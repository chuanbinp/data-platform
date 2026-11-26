#!/usr/bin/env bash
# Switch production volumes; --undo exchanges the active and previous volumes.
set -euo pipefail
source "$(dirname "$0")/platform-common.sh"
[[ $# == 1 ]] || fail 'Usage: restore_2_swap.sh VOLUME_NAME | --undo'
platform_context
read_volume_state
if [[ $1 == --undo ]]; then
    [[ -n "$previous" ]] || fail 'No previous volume recorded.'
    volume=$previous
    docker volume inspect "$volume" > /dev/null
else
    [[ -z "$previous" ]] || fail 'Use --undo or run restore_3_cleanup.sh before another swap.'
    volume=$1
    [[ "$volume" =~ ^data-platform-restore-[a-zA-Z0-9-]+$ ]] || fail 'Use the volume printed by restore_1_download.sh.'
    docker volume inspect "$volume" | jq -e '.[0].Labels["data-platform.restore"] == "true"' > /dev/null || fail 'Not a restored volume.'
fi
[[ "$volume" != "$active" ]] || fail 'That volume is already active.'
echo "Switching Postgres from $active to $volume. Both volumes are retained."
compose --profile dbt stop
compose rm -f postgres
attached=$(docker ps -aq --filter "volume=$volume")
[[ -z "$attached" ]] || fail 'Target volume is attached to another container.'
save_volume_state "$volume" "$active"
echo "Undo: sudo bash $release/docker/restore_2_swap.sh --undo"
start_restored_services
