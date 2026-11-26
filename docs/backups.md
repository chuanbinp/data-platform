# Postgres backups and recovery

Backups run daily at **3 a.m. New York time**, with no catch-up after downtime.
Each backup is one compressed SQL dump of all databases, roles, and permissions.
The private GCS bucket retains the last **seven successful runs**, including manual
runs, by filling seven slots and then atomically replacing the oldest.

## Setup

Run the GCP setup script once to create the bucket and grant the VM access.
BACKUP_BUCKET in the VM's .env defaults to data-platform-pg-backups. Pass the same
value to GCP setup when using a custom bucket.

After merging to master, CI/CD:

- Installs `jq` if missing and checks bucket access before stopping services.
- Deploys the application and backup scripts.
- Installs and enables the daily timer after application health checks pass.

To test a backup and restore manually on the VM:

~~~sh
cd /opt/data-platform/current/docker
sudo bash backup.sh
sudo bash restore_1_download.sh --list
sudo bash restore_1_download.sh slot-1.sql.gz
~~~

The download script prints the new volume name and the exact swap command.
Production stays unchanged. Delete unused test volumes with docker volume rm NAME.

## Recovery: download → swap or undo → cleanup

Pause deployments and external writers during recovery. Use the application
release and .env credentials matching the backup. All scripts share the deployment
lock.

~~~sh
cd /opt/data-platform/current/docker

# 1. Download and restore a backup into a fresh volume.
sudo bash restore_1_download.sh --list
sudo bash restore_1_download.sh slot-3.sql.gz

# 2. Use the volume name printed by step 1.
sudo bash restore_2_swap.sh RESTORED_VOLUME
~~~

Inspect the recovered data. If it is wrong, undo before cleanup:

~~~sh
sudo bash restore_2_swap.sh --undo
~~~

Once satisfied with the active database, complete step 3:

~~~sh
sudo bash restore_3_cleanup.sh
~~~

Swap stops services, saves the selected volumes, checks Postgres authentication,
and starts the dashboard, Airflow API, and tunnel. Review the recovered data and
Airflow task states. Scheduling stays stopped until you resume it below.

Both swap and undo save the selection atomically in
/opt/data-platform/postgres-volume.env:

~~~dotenv
POSTGRES_VOLUME_NAME=data-platform-restore-20261125T150000Z-12345
POSTGRES_PREVIOUS_VOLUME_NAME=data-platform_warehouse_pgdata
~~~

Deployments load this override after .env, so the active volume survives redeploys.
Undo exchanges the two names. Until cleanup, only undo is allowed; another new swap
cannot overwrite the rollback reference. A failed startup retains both volumes
and the undo command remains available.

Cleanup checks the actual Postgres mount, configuration, and database health.
It deletes only the recorded previous volume when unattached, then clears that
reference. It keeps the active volume's name. There is no copying, renaming, or
production downtime during cleanup. After cleanup, undo is no longer available.

## Resume ingestion

Swap leaves Airflow scheduling stopped. Review queued/running tasks in Airflow
before restarting it. Run this block on the VM; it waits for any deployment or
backup to finish, then starts scheduling:

~~~sh
sudo bash <<'SH'
set -euo pipefail
root=/opt/data-platform
exec 9>"$root/deploy.lock"
flock -w 1800 9
release=$(readlink -f "$root/current")
source "$release/docker/platform-common.sh"
compose up -d --no-deps --no-build --wait airflow-scheduler airflow-dag-processor
SH
~~~

## Check backups

Show the next scheduled backup:

~~~sh
sudo systemctl list-timers data-platform-backup.timer
~~~

Read recent backup logs:

~~~sh
sudo journalctl -u data-platform-backup.service --since '2 days ago'
~~~

## Remember

- Keep the bucket dedicated to these backups. Leave versioning, soft delete, and
  lifecycle rules disabled so extra copies do not accumulate. If soft delete was
  previously enabled, use a fresh bucket.
- Restore brings back a saved backup. You cannot recover changes made between
  backups, and each database is captured at a slightly different time.
- Save .env, application images/configuration, and Airflow logs separately.
  Deployment backups stored on the VM are separate from the seven cloud backups.
- Leave enough disk space for the downloaded backup and a second database volume.
- Check the logs for failures; automatic alerts are not configured.
- Never run docker compose down -v during recovery: it can delete your volumes.

