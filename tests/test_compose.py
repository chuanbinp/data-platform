"""Check the merged deployment configuration without starting containers."""

import io
import json
import shutil
import subprocess
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    not shutil.which("docker"), reason="Docker Compose is required"
)


def configuration(tmp_path, *, production=False, volume=None):
    # Explicit dummy settings prevent Compose from loading the developer's .env.
    lines = PROJECT.joinpath(".env.example").read_text().splitlines()
    env_file = tmp_path / "env"
    settings = (
        line + "compose-test-only" if line.endswith("=") else line
        for line in lines
    )
    env_file.write_text("\n".join(settings))
    if volume:
        with env_file.open("a") as stream:
            stream.write(f"\nPOSTGRES_VOLUME_NAME={volume}\n")
    command = [
        "docker", "compose", "--env-file", str(env_file),
        "--profile", "dbt", "-f", str(PROJECT / "compose.yaml"),
    ]
    if production:
        command += ["-f", str(PROJECT / "compose.production.yaml")]
    output = subprocess.check_output(command + ["config", "--format", "json"])
    return json.loads(output)


def test_production_restore_volume_override(tmp_path):
    default = configuration(tmp_path, production=True)
    assert default["volumes"]["warehouse_pgdata"]["name"] == "data-platform_warehouse_pgdata"
    restored = configuration(tmp_path, production=True, volume="data-platform-restore-test")
    assert restored["volumes"]["warehouse_pgdata"]["name"] == "data-platform-restore-test"
    assert restored["volumes"]["airflow_logs"] == default["volumes"]["airflow_logs"]


def test_shared_helper_keeps_volume_selection_across_releases(tmp_path):
    root = tmp_path / "platform"
    root.mkdir()
    settings = PROJECT.joinpath(".env.example").read_text().splitlines()
    root.joinpath(".env").write_text("\n".join(
        line + "test-only" if line.endswith("=") else line for line in settings
    ))
    root.joinpath("postgres-volume.env").write_text(
        "POSTGRES_VOLUME_NAME=data-platform-restore-selected\n"
        "POSTGRES_PREVIOUS_VOLUME_NAME=data-platform_warehouse_pgdata\n"
    )
    for name in ("first-release", "next-release"):
        release = tmp_path / name
        release.mkdir()
        for filename in ("compose.yaml", "compose.production.yaml"):
            release.joinpath(filename).symlink_to(PROJECT / filename)
        release.joinpath("images.env").touch()
        command = ["bash", "-c", 'root=$1; release=$2; source "$3"; compose config "${@:4}"',
                   "test", str(root), str(release), str(PROJECT / "docker/platform-common.sh")]
        config = json.loads(subprocess.check_output(command + ["--format", "json"]))
        assert config["volumes"]["warehouse_pgdata"]["name"] == "data-platform-restore-selected"
    environment = subprocess.check_output(command + ["--environment"], text=True)
    assert "BACKUP_BUCKET=data-platform-pg-backups" in environment.splitlines()


def test_local_dashboard_is_loopback_only(tmp_path):
    config = configuration(tmp_path)
    services = config["services"]
    assert "dashboard-proxy" not in services
    assert "cloudflared" not in services
    ports = services["dashboard"]["ports"]
    assert len(ports) == 1
    assert ports[0]["host_ip"] == "127.0.0.1"
    assert ports[0]["published"] == "8080"
    assert not services["dashboard"]["environment"].get("TRUSTED_TUNNEL_IP")


def test_production_isolation_and_baked_images(tmp_path):
    config = configuration(tmp_path, production=True)
    services = config["services"]
    dashboard, tunnel = services["dashboard"], services["cloudflared"]
    assert not dashboard.get("ports")
    assert not tunnel.get("ports")
    assert set(dashboard["networks"]) == {"tunnel", "warehouse"}
    assert set(tunnel["networks"]) == {"tunnel", "explorer"}
    assert config["networks"]["tunnel"]["internal"]
    assert config["networks"]["warehouse"]["internal"]
    connector_ip = tunnel["networks"]["tunnel"]["ipv4_address"]
    assert dashboard["environment"]["TRUSTED_TUNNEL_IP"] == connector_ip
    assert "@sha256:" in tunnel["image"]
    assert tunnel["secrets"] and not tunnel.get("environment", {}).get("TUNNEL_TOKEN")
    for name, service in services.items():
        assert not service.get("build"), f"{name} must use a prebuilt image"
        for port in service.get("ports", []):
            assert port["host_ip"] == "127.0.0.1"
    for name in (
        "airflow-init", "airflow-api-server", "airflow-scheduler", "airflow-dag-processor"
    ):
        service = services[name]
        assert all(volume["type"] == "volume" for volume in service["volumes"])
        assert service["environment"]["AIRFLOW__CORE__PARALLELISM"] == "1"
        assert service["environment"]["DBT_THREADS"] == "1"
    assert not services["dbt"].get("volumes")
    postgres_mounts = {
        volume["target"]: volume for volume in services["postgres"]["volumes"]
    }
    assert postgres_mounts["/var/lib/postgresql/data"]["type"] == "volume"
    entrypoint = postgres_mounts["/usr/local/bin/platform-postgres-entrypoint.sh"]
    assert entrypoint["source"] == "/opt/data-platform/postgres-entrypoint.sh"
    assert entrypoint["read_only"]


def test_production_airflow_startup_grace_preserves_health_probes(tmp_path):
    local = configuration(tmp_path)["services"]
    production = configuration(tmp_path, production=True)["services"]
    for name in ("airflow-api-server", "airflow-scheduler", "airflow-dag-processor"):
        health = production[name]["healthcheck"]
        assert health["test"] == local[name]["healthcheck"]["test"]
        assert health["start_period"] == "3m0s"
        assert health["retries"] == local[name]["healthcheck"]["retries"]
    for services in (local, production):
        dependencies = services["airflow-dag-processor"]["depends_on"]
        assert dependencies["airflow-api-server"]["condition"] == "service_healthy"
        assert dependencies["postgres"]["condition"] == "service_healthy"
        assert dependencies["airflow-init"]["condition"] == "service_completed_successfully"


@pytest.mark.parametrize("status,hostnames,exit_code", [
    ("healthy", ["current-container"], 0),
    ("healthy", ["old-container", "current-container"], 0),
    ("healthy", ["old-container"], 1),
    ("healthy", [], 1),
    ("healthy", None, 1),
    ("unhealthy", ["current-container"], 1),
])
def test_dag_processor_probe_checks_own_heartbeat(tmp_path, monkeypatch, status, hostnames, exit_code):
    import socket
    import urllib.request

    probe = configuration(tmp_path)["services"]["airflow-dag-processor"]["healthcheck"]["test"]
    assert probe[:3] == ["CMD", "python", "-c"]

    def respond(url, timeout):
        assert url == "http://airflow-api-server:8080/api/v2/monitor/health"
        assert timeout == 5
        instances = None if hostnames is None else [{"hostname": name} for name in hostnames]
        return io.BytesIO(json.dumps({"dag_processor": {"status": status, "instances": instances}}).encode())

    monkeypatch.setattr(socket, "gethostname", lambda: "current-container")
    monkeypatch.setattr(urllib.request, "urlopen", respond)
    with pytest.raises(SystemExit) as result:
        exec(probe[3], {})
    assert int(result.value.code) == exit_code


def test_dag_processor_probe_fails_when_api_is_unavailable(tmp_path, monkeypatch):
    import urllib.error
    import urllib.request

    probe = configuration(tmp_path)["services"]["airflow-dag-processor"]["healthcheck"]["test"]

    def unavailable(*args, **kwargs):
        raise urllib.error.URLError("API unavailable")

    monkeypatch.setattr(urllib.request, "urlopen", unavailable)
    # An uncaught request error exits the probe with a failure, never healthy.
    with pytest.raises(urllib.error.URLError):
        exec(probe[3], {})
