"""Check the merged deployment configuration without starting containers."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    not shutil.which("docker"), reason="Docker Compose is required"
)


def configuration(tmp_path, *, production=False):
    # Explicit dummy settings prevent Compose from loading the developer's .env.
    lines = PROJECT.joinpath(".env.example").read_text().splitlines()
    env_file = tmp_path / "env"
    settings = (
        line + "compose-test-only" if line.endswith("=") else line
        for line in lines
    )
    env_file.write_text("\n".join(settings))
    command = [
        "docker", "compose", "--env-file", str(env_file),
        "--profile", "dbt", "-f", str(PROJECT / "compose.yaml"),
    ]
    if production:
        command += ["-f", str(PROJECT / "compose.production.yaml")]
    output = subprocess.check_output(command + ["config", "--format", "json"])
    return json.loads(output)


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
    for service in services.values():
        assert not service.get("build")
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
