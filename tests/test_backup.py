"""Test the backup scripts with fake cloud tools and an opt-in real restore."""
import gzip
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid

import pytest

PROJECT = Path(__file__).resolve().parents[1]
IMAGE = re.search(r"image: (postgres:17@sha256:[0-9a-f]+)", (PROJECT / "compose.yaml").read_text())[1]


@pytest.fixture
def environment(tmp_path):
    root = tmp_path / "platform"
    root.mkdir()
    (root / "current").symlink_to(PROJECT)
    (root / ".env").write_text('BACKUP_BUCKET=test-backups\n')
    objects = tmp_path / "objects.json"
    objects.write_text('{"items":[]}')
    settings = tmp_path / "bucket.json"
    settings.write_text(json.dumps({"iamConfiguration": {
        "publicAccessPrevention": "enforced", "uniformBucketLevelAccess": {"enabled": True}}}))
    dump = tmp_path / "dump.sql.gz"
    dump.write_bytes(gzip.compress(b"CREATE ROLE admin;\nCOPY statements FROM stdin;\nCREATE ROLE admin;\n\\.\n"))
    log = tmp_path / "commands.jsonl"
    volumes = tmp_path / "volumes.json"
    volumes.write_text('{"data-platform_warehouse_pgdata":{}}')
    mount = tmp_path / "mount"
    mount.write_text("data-platform_warehouse_pgdata")
    binaries = tmp_path / "bin"
    binaries.mkdir()
    stub = binaries / "stub"
    stub.write_text(f"#!{sys.executable}\n" + r'''
import gzip, json, os, pathlib, shutil, sys
name, args = pathlib.Path(sys.argv[0]).name, sys.argv[1:]
with open(os.environ['MOCK_LOG'], 'a') as log:
    log.write(json.dumps([name, *args]) + '\n')
state = pathlib.Path(os.environ['MOCK_OBJECTS'])
if name == 'stat':
    print('600')
elif name in ('systemctl', 'install', 'apt-get'):
    pass  # Host setup is recorded, never executed by these tests.
elif name == 'curl':
    print(state.read_text())
elif name == 'gcloud':
    if args[:3] == ['storage', 'buckets', 'describe']:
        print(pathlib.Path(os.environ['MOCK_BUCKET']).read_text())
    elif args[0] == 'auth':
        print('token')
    elif args[:2] == ['storage', 'cp']:
        source, target = args[2:4]
        if source.startswith('gs://'):
            shutil.copyfile(os.environ['MOCK_DUMP'], target)
        else:
            if os.environ.get('FAIL_UPLOAD'): sys.exit(1)
            gzip.decompress(pathlib.Path(source).read_bytes())
            objects = json.loads(state.read_text())['items']
            slot = target.rsplit('/', 1)[1]
            prior = next((o for o in objects if o['name'] == slot), None)
            assert '--if-generation-match=' + (prior['generation'] if prior else '0') in args
            assert os.environ['CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_ENABLED'] == 'false'
            number = max([int(o['generation']) for o in objects] or [0]) + 1
            objects = [o for o in objects if o['name'] != slot]
            metadata = next(a.split('=', 1)[1] for a in args if a.startswith('--custom-metadata='))
            timestamp = '2026-11-25T03:00:00Z' if number == 1 else f'2026-11-25T03:00:00.{number:03d}Z'
            objects.append({'name':slot, 'generation':str(number),
                'timeCreated':timestamp,
                'metadata':dict(pair.split('=',1) for pair in metadata.split(','))})
            assert len(objects) <= 7
            state.write_text(json.dumps({'items':objects}))
elif name == 'docker':
    volumes_path = pathlib.Path(os.environ['MOCK_VOLUMES'])
    volumes = json.loads(volumes_path.read_text())
    mount = pathlib.Path(os.environ['MOCK_MOUNT'])
    pointer = pathlib.Path(os.environ['PLATFORM_ROOT'], 'postgres-volume.env')
    saved = dict(line.split('=',1) for line in pointer.read_text().splitlines()) if pointer.exists() else {}
    active = saved.get('POSTGRES_VOLUME_NAME', 'data-platform_warehouse_pgdata')
    if args[:2] == ['volume', 'inspect']:
        volume = args[-1]
        if volume not in volumes: sys.exit(1)
        print(json.dumps([{'Labels': volumes[volume]}]))
    elif args[:2] == ['volume', 'create']:
        volumes[args[-1]] = dict(args[i+1].split('=',1) for i, arg in enumerate(args) if arg == '--label')
        volumes_path.write_text(json.dumps(volumes))
    elif args[:2] == ['volume', 'rm']:
        del volumes[args[-1]]
        volumes_path.write_text(json.dumps(volumes))
    elif args[:2] == ['volume', 'ls']:
        print('\n'.join(volumes))
    elif args[0] == 'inspect':
        print(json.dumps([{'Mounts': [{'Destination':'/var/lib/postgresql/data', 'Name':mount.read_text()}]}]))
    elif args[0] == 'start' and os.environ.get('FAIL_START'):
        sys.exit(1)
    elif args[0] == 'ps':
        if os.environ.get('ATTACHED_VOLUME'): print('external-container')
    elif args[0] == 'compose' and 'ps' in args:
        if mount.exists(): print('production-postgres')
    elif args[0] == 'compose' and 'rm' in args:
        mount.unlink(missing_ok=True)
    elif args[0] == 'compose' and 'up' in args:
        if os.environ.get('FAIL_RESTART'): sys.exit(1)
        if args[-1] == 'postgres': mount.write_text(active)
    elif '--environment' in args:
        print(pathlib.Path(os.environ['PLATFORM_ROOT'], '.env').read_text())
    elif 'config' in args:
        print(json.dumps({'services': {'postgres': {'image':os.environ['MOCK_IMAGE'],
            'environment': {'POSTGRES_USER':'admin'}}}, 'volumes': {'warehouse_pgdata': {'name':active}}}))
    elif 'pg_dumpall' in args:
        print('CREATE ROLE admin;\nSELECT 1;')
        if os.environ.get('FAIL_DUMP'): sys.exit(1)
    elif 'psql' in args:
        if '-c' in args:
            print('0' if os.environ.get('FAIL_HEALTH') else '1')
            sys.exit(0)
        # Preserve identical text in COPY data after skipping the bootstrap role.
        assert sys.stdin.read().count('CREATE ROLE admin;') == 1
        if os.environ.get('FAIL_RESTORE'): sys.exit(1)
''')
    stub.chmod(0o755)
    for command in ["gcloud", "curl", "docker", "stat", "systemctl", "install", "apt-get"]:
        (binaries / command).symlink_to(stub)
    if not shutil.which("flock"):
        (binaries / "flock").symlink_to(stub)  # macOS unit tests; CI uses real flock.
    return dict(os.environ, PLATFORM_ROOT=str(root), PATH=f"{binaries}:{os.environ['PATH']}",
                MOCK_LOG=str(log), MOCK_OBJECTS=str(objects), MOCK_BUCKET=str(settings),
                MOCK_DUMP=str(dump), MOCK_IMAGE=IMAGE, MOCK_VOLUMES=str(volumes), MOCK_MOUNT=str(mount))


def execute(environment, script, *args):
    return subprocess.run(["bash", str(PROJECT / "docker" / script), *args],
                          env=environment, capture_output=True, text=True)


def commands(environment):
    return [json.loads(line) for line in Path(environment["MOCK_LOG"]).read_text().splitlines()]


def test_ten_runs_retain_only_latest_seven(environment):
    for run in range(1, 11):
        result = execute(environment, "backup.sh")
        assert result.returncode == 0, result.stderr
        objects = json.loads(Path(environment["MOCK_OBJECTS"]).read_text())["items"]
        assert len(objects) == min(run, 7)
        assert {int(o["generation"]) for o in objects} == set(range(max(1, run - 6), run + 1))


@pytest.mark.parametrize("failure", ["FAIL_DUMP", "FAIL_UPLOAD"])
def test_failed_backup_preserves_previous_objects(environment, failure):
    assert execute(environment, "backup.sh").returncode == 0
    before = Path(environment["MOCK_OBJECTS"]).read_text()
    environment[failure] = "1"
    assert execute(environment, "backup.sh").returncode != 0
    assert Path(environment["MOCK_OBJECTS"]).read_text() == before


def test_versioning_and_unrelated_objects_are_rejected(environment):
    settings = Path(environment["MOCK_BUCKET"])
    original = settings.read_text()
    settings.write_text(json.dumps(json.loads(original) | {"versioning": {"enabled": True}}))
    assert execute(environment, "backup.sh").returncode != 0
    settings.write_text(original)
    Path(environment["MOCK_OBJECTS"]).write_text('{"items":[{"name":"unrelated"}]}')
    assert execute(environment, "backup.sh").returncode != 0


@pytest.mark.parametrize("bucket_ready", [True, False])
def test_deployment_checks_bucket_before_stopping_and_enables_timer_after_health(environment, bucket_ready):
    root = Path(environment["PLATFORM_ROOT"])
    release = root / "releases" / "test-release"
    scripts = release / "docker"
    scripts.mkdir(parents=True)
    for name in ("deploy.sh", "platform-common.sh", "postgres-entrypoint.sh",
                 "data-platform-backup.service", "data-platform-backup.timer"):
        shutil.copyfile(PROJECT / "docker" / name, scripts / name)
    deploy = scripts / "deploy.sh"
    deploy.write_text(deploy.read_text().replace("root=/opt/data-platform", "root=$PLATFORM_ROOT", 1))
    shutil.copyfile(scripts / "postgres-entrypoint.sh", root / "postgres-entrypoint.sh")
    (release / "images.env").write_text("AIRFLOW_IMAGE=test-only\n")
    (root / "tunnel-token").write_text("test-only")
    (root / "dashboard-url").write_text("https://dashboard.example")
    if not bucket_ready:
        bucket = Path(environment["MOCK_BUCKET"])
        bucket.write_text(json.dumps(json.loads(bucket.read_text()) | {"versioning": {"enabled": True}}))
    result = subprocess.run(["bash", str(deploy)], env=environment, capture_output=True, text=True)
    log = commands(environment)
    enable = [i for i, c in enumerate(log) if c == ["systemctl", "enable", "--now", "data-platform-backup.timer"]]
    stops = [i for i, c in enumerate(log) if c[0] == "docker" and "stop" in c]
    if bucket_ready:
        assert result.returncode == 0, result.stderr
        check = next(i for i, c in enumerate(log) if c[:4] == ["gcloud", "storage", "buckets", "describe"])
        health = next(i for i, c in enumerate(log) if c[0] == "curl" and c[-1] == "https://dashboard.example/api/health")
        assert check < stops[0] < health < enable[0]
    else:
        assert result.returncode != 0
        assert not stops and not enable
        assert "Backup bucket unavailable or misconfigured" in result.stderr


def test_restore_preserves_new_volume_and_pins_download_generation(environment):
    assert execute(environment, "backup.sh").returncode == 0
    assert "slot-1.sql.gz" in execute(environment, "restore_1_download.sh", "--list").stdout
    result = execute(environment, "restore_1_download.sh", "slot-1.sql.gz")
    assert result.returncode == 0, result.stderr
    assert any("gs://test-backups/slot-1.sql.gz#1" in c for c in commands(environment))
    assert not any(c[:3] == ["docker", "volume", "rm"] for c in commands(environment))
    assert "Production is unchanged" in result.stdout


@pytest.mark.parametrize("failure", ["FAIL_RESTORE", "FAIL_START"])
def test_failed_restore_removes_only_its_new_volume(environment, failure):
    assert execute(environment, "backup.sh").returncode == 0
    environment[failure] = "1"
    assert execute(environment, "restore_1_download.sh", "slot-1.sql.gz").returncode != 0
    removed = [c[-1] for c in commands(environment) if c[:3] == ["docker", "volume", "rm"]]
    assert len(removed) == 1 and removed[0].startswith("data-platform-restore-")


def downloaded_volume(environment):
    assert execute(environment, "backup.sh").returncode == 0
    result = execute(environment, "restore_1_download.sh", "slot-1.sql.gz")
    assert result.returncode == 0, result.stderr
    return re.search(r"volume: (\S+)", result.stdout)[1]


def saved_state(environment):
    pointer = Path(environment["PLATFORM_ROOT"]) / "postgres-volume.env"
    return dict(line.split("=", 1) for line in pointer.read_text().splitlines())


def test_swap_undo_and_cleanup_preserve_selected_database(environment):
    volume = downloaded_volume(environment)
    original = "data-platform_warehouse_pgdata"
    assert execute(environment, "restore_2_swap.sh", volume).returncode == 0
    assert saved_state(environment) == {
        "POSTGRES_VOLUME_NAME": volume, "POSTGRES_PREVIOUS_VOLUME_NAME": original}
    assert Path(environment["MOCK_MOUNT"]).read_text() == volume
    assert execute(environment, "restore_2_swap.sh", "--undo").returncode == 0
    assert saved_state(environment) == {
        "POSTGRES_VOLUME_NAME": original, "POSTGRES_PREVIOUS_VOLUME_NAME": volume}
    assert execute(environment, "restore_3_cleanup.sh").returncode == 0
    assert saved_state(environment) == {"POSTGRES_VOLUME_NAME": original}
    assert list(json.loads(Path(environment["MOCK_VOLUMES"]).read_text())) == [original]
    assert execute(environment, "restore_2_swap.sh", "--undo").returncode != 0


def test_cleanup_accepts_restore_and_leaves_unrelated_volumes_alone(environment):
    volume = downloaded_volume(environment)
    assert execute(environment, "restore_2_swap.sh", volume).returncode == 0
    path = Path(environment["MOCK_VOLUMES"])
    path.write_text(json.dumps(json.loads(path.read_text()) | {"unrelated": {}}))
    Path(environment["MOCK_LOG"]).write_text("")
    result = execute(environment, "restore_3_cleanup.sh")
    assert result.returncode == 0, result.stderr
    assert set(json.loads(path.read_text())) == {volume, "unrelated"}
    assert saved_state(environment) == {"POSTGRES_VOLUME_NAME": volume}
    assert Path(environment["MOCK_MOUNT"]).read_text() == volume
    assert not any("stop" in c for c in commands(environment))


def test_swap_rejects_attached_target_before_saving_state(environment):
    volume = downloaded_volume(environment)
    environment["ATTACHED_VOLUME"] = "1"
    assert execute(environment, "restore_2_swap.sh", volume).returncode != 0
    assert not (Path(environment["PLATFORM_ROOT"]) / "postgres-volume.env").exists()


def test_pending_pair_cannot_be_overwritten_by_another_swap(environment):
    volume = downloaded_volume(environment)
    assert execute(environment, "restore_2_swap.sh", volume).returncode == 0
    before = saved_state(environment)
    assert execute(environment, "restore_2_swap.sh", "data-platform-restore-another").returncode != 0
    assert saved_state(environment) == before


@pytest.mark.parametrize("failure", ["ATTACHED_VOLUME", "FAIL_HEALTH", "MOUNT_MISMATCH"])
def test_cleanup_refuses_unsafe_deletion(environment, failure):
    volume = downloaded_volume(environment)
    assert execute(environment, "restore_2_swap.sh", volume).returncode == 0
    before = Path(environment["MOCK_VOLUMES"]).read_text()
    if failure == "MOUNT_MISMATCH":
        Path(environment["MOCK_MOUNT"]).write_text("unexpected-volume")
    else:
        environment[failure] = "1"
    assert execute(environment, "restore_3_cleanup.sh").returncode != 0
    assert Path(environment["MOCK_VOLUMES"]).read_text() == before
    assert saved_state(environment)["POSTGRES_PREVIOUS_VOLUME_NAME"] == "data-platform_warehouse_pgdata"


def test_swap_refuses_configuration_mount_mismatch(environment):
    volume = downloaded_volume(environment)
    Path(environment["MOCK_MOUNT"]).write_text("unexpected-volume")
    assert execute(environment, "restore_2_swap.sh", volume).returncode != 0
    assert not (Path(environment["PLATFORM_ROOT"]) / "postgres-volume.env").exists()


def test_failed_swap_keeps_both_volumes_and_can_be_undone(environment):
    volume = downloaded_volume(environment)
    environment["FAIL_RESTART"] = "1"
    assert execute(environment, "restore_2_swap.sh", volume).returncode != 0
    assert saved_state(environment)["POSTGRES_PREVIOUS_VOLUME_NAME"] == "data-platform_warehouse_pgdata"
    assert len(json.loads(Path(environment["MOCK_VOLUMES"]).read_text())) == 2
    del environment["FAIL_RESTART"]
    assert execute(environment, "restore_2_swap.sh", "--undo").returncode == 0
    assert Path(environment["MOCK_MOUNT"]).read_text() == "data-platform_warehouse_pgdata"


def test_cleanup_can_finish_after_previous_volume_was_already_deleted(environment):
    volume = downloaded_volume(environment)
    assert execute(environment, "restore_2_swap.sh", volume).returncode == 0
    Path(environment["MOCK_VOLUMES"]).write_text(json.dumps({volume: {}}))
    assert execute(environment, "restore_3_cleanup.sh").returncode == 0
    assert saved_state(environment) == {"POSTGRES_VOLUME_NAME": volume}


@pytest.mark.backup_docker
@pytest.mark.skipif(os.environ.get("RUN_BACKUP_DOCKER_TESTS") != "1", reason="Opt-in Docker round trip")
def test_real_dumpall_restore(environment):
    name = "backup-source-" + uuid.uuid4().hex
    restored_volume = None

    def docker(*args):
        return subprocess.check_output(["docker", *args])

    def wait(container):
        for _ in range(120):
            ready = subprocess.run(["docker", "exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "admin"], capture_output=True)
            if ready.returncode == 0:
                return
            time.sleep(1)
        pytest.fail("Postgres did not start")

    try:
        docker("run", "-d", "--name", name, "--network", "none", "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
               "-e", "POSTGRES_USER=admin", "-e", "POSTGRES_DB=postgres", IMAGE)
        wait(name)
        docker("exec", name, "psql", "-U", "admin", "-d", "postgres", "-c",
               "CREATE TABLE readings(id serial PRIMARY KEY, value integer); INSERT INTO readings(value) VALUES (42)")
        dump = docker("exec", name, "pg_dumpall", "-U", "admin")
        Path(environment["MOCK_DUMP"]).write_bytes(gzip.compress(dump))
        assert execute(environment, "backup.sh").returncode == 0
        # Use real Docker for restoration, keeping only the cloud mocked.
        (Path(environment["PATH"].split(":")[0]) / "docker").unlink()
        result = execute(environment, "restore_1_download.sh", "slot-1.sql.gz")
        assert result.returncode == 0, result.stderr
        restored_volume = re.search(r"volume: (\S+)", result.stdout)[1]
        docker("run", "-d", "--name", name + "-verify", "--network", "none",
               "-v", restored_volume + ":/var/lib/postgresql/data", IMAGE)
        wait(name + "-verify")
        value = docker("exec", name + "-verify", "psql", "-U", "admin", "-d", "postgres", "-Atc", "SELECT value FROM readings")
        assert value.strip() == b"42"
    finally:
        subprocess.run(["docker", "rm", "-fv", name, name + "-verify"], capture_output=True)
        if restored_volume:
            docker("volume", "rm", restored_volume)

