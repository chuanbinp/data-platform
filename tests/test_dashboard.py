"""Public API boundaries and real PostgreSQL viewer permissions."""

import json
import os
import secrets
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

pytest.importorskip("fastapi")
pytest.importorskip("psycopg_pool")
from fastapi.testclient import TestClient

from data_platform.dashboard.app import app, encode
from data_platform.dashboard.provision import provision


def test_exact_values_and_utc():
    value = {
        "number": Decimal("123456789.123456789123456789"),
        "bigint": 2**63 - 1,
        "null": None,
        "empty": "",
        "time": datetime(2026, 11, 24, tzinfo=timezone.utc),
        "array": [Decimal("0.1")],
    }
    assert encode(value) == {
        "number": "123456789.123456789123456789",
        "bigint": "9223372036854775807",
        "null": None,
        "empty": "",
        "time": "2026-11-24T00:00:00Z",
        "array": ["0.1"],
    }


@pytest.fixture
def explorer(monkeypatch):
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set TEST_DATABASE_URL for viewer integration tests")
    suffix = uuid4().hex
    namespaces = [f"test_{layer}_{suffix}" for layer in ("bronze", "silver", "gold")]
    outsider = f"test_private_{suffix}"
    user, password = f"test_viewer_{suffix}", secrets.token_urlsafe(20)
    options = conninfo_to_dict(dsn)
    for key, value in {
        "POSTGRES_HOST": options.get("host", "127.0.0.1"),
        "POSTGRES_PORT": options.get("port", "5432"),
        "POSTGRES_DB": options["dbname"],
        "VIEWER_USER": user,
        "VIEWER_PASSWORD": password,
    }.items():
        monkeypatch.setenv(key, value)
    for layer, namespace in zip(("BRONZE", "SILVER", "GOLD"), namespaces):
        monkeypatch.setenv(f"DBT_{layer}_SCHEMA", namespace)
    try:
        with psycopg.connect(dsn) as admin:
            provision(admin, user, password, namespaces)
            admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(outsider)))
            admin.execute(
                sql.SQL("CREATE TABLE {} (secret text)").format(
                    sql.Identifier(outsider, "hidden")
                )
            )
            # Created after provisioning: verifies default grants, including views.
            admin.execute(
                sql.SQL("""
                CREATE TABLE {} (id integer PRIMARY KEY, period text, respondent text,
                                 value numeric, note text, observed_at timestamptz)
            """).format(sql.Identifier(namespaces[0], "readings"))
            )
            admin.execute(
                sql.SQL("""
                INSERT INTO {} SELECT i, '2026-11-24T00', 'NYIS',
                    10.123456789123456789, CASE WHEN i=1 THEN NULL WHEN i=2 THEN ''
                    ELSE '<script>alert(1)</script>' END, '2026-11-24T01:00:00Z'
                FROM generate_series(1, 205) i
            """).format(sql.Identifier(namespaces[0], "readings"))
            )
            admin.execute(
                sql.SQL(
                    "CREATE VIEW {} AS SELECT *, (period || ':00:00Z')::timestamptz AS typed_period FROM {}"
                ).format(
                    sql.Identifier(namespaces[1], "typed"),
                    sql.Identifier(namespaces[0], "readings"),
                )
            )
        with TestClient(app) as client:
            yield client, namespaces, user, password, options, outsider
    finally:
        with psycopg.connect(dsn) as admin:
            for namespace in [*namespaces, outsider]:
                admin.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(namespace)
                    )
                )
            admin.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(user)))
            admin.execute(
                sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(user))
            )


@pytest.mark.integration
def test_browsing_pagination_filters_and_precision(explorer):
    client, namespaces, *_ = explorer
    response = client.get("/api/relations")
    assert response.status_code == 200
    assert {(r["schema"], r["name"]) for r in response.json()["relations"]} == {
        (namespaces[0], "readings"),
        (namespaces[1], "typed"),
    }
    base = f"/api/relations/{namespaces[0]}/readings"
    columns = client.get(f"{base}/columns").json()["columns"]
    assert {c["name"] for c in columns} == {
        "id",
        "period",
        "respondent",
        "value",
        "note",
        "observed_at",
    }
    first = client.get(f"{base}/rows", params={"sort": "id", "direction": "asc"}).json()
    assert len(first["rows"]) == 100 and first["has_more"]
    assert first["rows"][0]["note"] is None and first["rows"][1]["note"] == ""
    assert first["rows"][0]["value"] == "10.123456789123456789"
    assert first["rows"][0]["observed_at"].endswith("Z")
    second = client.get(
        f"{base}/rows", params={"sort": "id", "direction": "asc", "page": 2}
    ).json()
    assert second["rows"][0]["id"] == 101
    third = client.get(
        f"{base}/rows", params={"sort": "id", "direction": "asc", "page": 3}
    ).json()
    assert len(third["rows"]) == 5 and not third["has_more"]
    assert client.get("/api/health").status_code == 200


@pytest.mark.integration
def test_api_rejects_identifiers_injection_and_writes(explorer):
    client, namespaces, *_, outsider = explorer
    base = f"/api/relations/{namespaces[0]}/readings/rows"
    for params in (
        {"sort": "id; DROP TABLE readings"},
        {"match_column": "secret", "match_value": json.dumps("x")},
        {"direction": "desc; select 1"},
        {"page_size": 101},
        {"page": 0},
        {"page": 102, "page_size": 100},
    ):
        assert client.get(base, params=params).status_code == 422
    injection = "' OR 1=1; DROP TABLE readings; --"
    assert (
        client.get(
            base,
            params={"match_column": "respondent", "match_value": json.dumps(injection)},
        ).json()["rows"]
        == []
    )
    for namespace in (outsider, "pg_catalog", "information_schema", "public"):
        assert client.get(f"/api/relations/{namespace}/hidden/rows").status_code == 404
    assert (
        client.get(f"/api/relations/{namespaces[0]}/missing/columns").status_code == 404
    )
    assert client.post(base, json={"sql": "DROP TABLE readings"}).status_code == 405
    assert client.get("/docs").status_code == 404
    response = client.get("/")
    assert response.status_code == 200
    assert "script-src 'self'" in response.headers["content-security-policy"]
    assert client.get(base).headers["cache-control"] == "no-store"


@pytest.mark.integration
def test_default_observation_sort(explorer):
    client, namespaces, *_ = explorer
    base = f"/api/relations/{namespaces[0]}/readings/rows"
    with psycopg.connect(os.environ["TEST_DATABASE_URL"]) as admin:
        admin.execute(
            sql.SQL(
                "UPDATE {} SET observed_at = observed_at + id * INTERVAL '1 minute'"
            ).format(sql.Identifier(namespaces[0], "readings"))
        )
        admin.execute(
            sql.SQL(
                "CREATE VIEW {} AS SELECT id, observed_at AS last_observed_at FROM {}"
            ).format(
                sql.Identifier(namespaces[2], "latest"),
                sql.Identifier(namespaces[0], "readings"),
            )
        )
    default = client.get(base).json()
    assert default["rows"][0]["id"] == 205
    assert default["sort"] == {
        "column": "observed_at",
        "direction": "desc",
        "default": True,
    }
    assert client.get(base, params={"direction": "asc"}).json()["rows"][0]["id"] == 205
    ascending = client.get(
        base, params={"sort": "observed_at", "direction": "asc"}
    ).json()
    assert ascending["rows"][0]["id"] == 1
    assert ascending["sort"]["default"] is False
    assert (
        client.get(base, params={"sort": "observed_at", "direction": "desc"}).json()[
            "rows"
        ][0]["id"]
        == 205
    )
    latest = client.get(f"/api/relations/{namespaces[2]}/latest/rows").json()
    assert latest["sort"]["column"] == "last_observed_at"
    assert latest["rows"][0]["id"] == 205


@pytest.mark.integration
def test_header_value_lookup_and_exact_filters(explorer):
    client, namespaces, *_ = explorer
    base = f"/api/relations/{namespaces[0]}/readings"
    # A unique value on a later page must still be offered in the header.
    with psycopg.connect(os.environ["TEST_DATABASE_URL"]) as admin:
        admin.execute(
            sql.SQL("UPDATE {} SET respondent='LATE_PAGE' WHERE id=205").format(
                sql.Identifier(namespaces[0], "readings")
            )
        )
        admin.execute(
            sql.SQL("UPDATE {} SET note='null' WHERE id=3").format(
                sql.Identifier(namespaces[0], "readings")
            )
        )
    options = client.get(f"{base}/values", params={"column": "respondent"}).json()
    assert options["values"] == ["LATE_PAGE", "NYIS"] and not options["has_more"]
    assert client.get(
        f"{base}/values", params={"column": "respondent", "search": "late"}
    ).json()["values"] == ["LATE_PAGE"]
    options = client.get(f"{base}/values", params={"column": "id"}).json()
    assert len(options["values"]) == 100 and options["has_more"]
    assert (
        "205"
        in client.get(
            f"{base}/values", params={"column": "id", "search": "205"}
        ).json()["values"]
    )
    options = client.get(f"{base}/values", params={"column": "note"}).json()["values"]
    assert None in options and "" in options and "null" in options
    for value, expected in ((None, 1), ("", 2), ("null", 3)):
        data = client.get(
            f"{base}/rows",
            params={"match_column": "note", "match_value": json.dumps(value)},
        ).json()
        assert [row["id"] for row in data["rows"]] == [expected]
    exact = [
        ("match_column", "respondent"),
        ("match_value", json.dumps("NYIS")),
        ("match_column", "id"),
        ("match_value", json.dumps("20")),
    ]
    assert [
        r["id"] for r in client.get(f"{base}/rows", params=exact).json()["rows"]
    ] == [20]
    # Multiple values in one column use OR; another column still uses AND.
    multiple = [*exact, ("match_column", "id"), ("match_value", json.dumps("21"))]
    assert {
        r["id"] for r in client.get(f"{base}/rows", params=multiple).json()["rows"]
    } == {20, 21}
    narrowed = [
        *multiple,
        ("match_column", "note"),
        ("match_value", json.dumps("does not exist")),
    ]
    assert client.get(f"{base}/rows", params=narrowed).json()["rows"] == []
    # NULL and an empty string can be selected together, without matching literal 'null'.
    nullable = [
        ("match_column", "note"),
        ("match_value", "null"),
        ("match_column", "note"),
        ("match_value", json.dumps("")),
    ]
    assert {
        r["id"] for r in client.get(f"{base}/rows", params=nullable).json()["rows"]
    } == {1, 2}
    # With no selections the column is unrestricted again.
    assert len(client.get(f"{base}/rows").json()["rows"]) == 100
    for invalid in (
        {"match_column": "note"},
        {"match_value": '"x"'},
        {"match_column": "secret", "match_value": '"x"'},
        {"match_column": "note", "match_value": "not-json"},
        {"match_column": "note", "match_value": "42"},
        {"match_column": "note", "match_value": '"' + "x" * 10_001 + '"'},
    ):
        assert client.get(f"{base}/rows", params=invalid).status_code == 422
    injection = "' OR 1=1 --"
    assert (
        client.get(
            f"{base}/rows",
            params={"match_column": "note", "match_value": json.dumps(injection)},
        ).json()["rows"]
        == []
    )
    assert (
        client.get(
            f"{base}/values", params={"column": "id; DROP TABLE readings"}
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/api/relations/pg_catalog/pg_class/values", params={"column": "relname"}
        ).status_code
        == 404
    )


@pytest.mark.integration
def test_database_permissions_survive_view_recreation(explorer):
    client, namespaces, user, password, options, outsider = explorer
    viewer_options = {**options, "user": user, "password": password}
    with psycopg.connect(**viewer_options, autocommit=True) as viewer:
        assert (
            viewer.execute("SHOW default_transaction_read_only").fetchone()[0] == "on"
        )
        assert viewer.execute("SHOW statement_timeout").fetchone()[0] == "5s"
        # Turn off the default as an attacker might: ACLs must still reject writes.
        viewer.execute("SET default_transaction_read_only = off")
        for statement in (
            sql.SQL("INSERT INTO {} (id) VALUES (999)").format(
                sql.Identifier(namespaces[0], "readings")
            ),
            sql.SQL("UPDATE {} SET value=0").format(
                sql.Identifier(namespaces[0], "readings")
            ),
            sql.SQL("DELETE FROM {}").format(sql.Identifier(namespaces[0], "readings")),
            sql.SQL("DROP TABLE {}").format(sql.Identifier(namespaces[0], "readings")),
            sql.SQL("CREATE TABLE {} (id int)").format(
                sql.Identifier(namespaces[0], "forbidden")
            ),
            sql.SQL("SELECT * FROM {}").format(sql.Identifier(outsider, "hidden")),
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                viewer.execute(statement)
        assert (
            viewer.execute(
                "SELECT rolsuper, rolcreatedb, rolcreaterole, rolbypassrls FROM pg_roles WHERE rolname=current_user"
            ).fetchone()
            == (False,) * 4
        )
    with psycopg.connect(os.environ["TEST_DATABASE_URL"]) as admin:
        provision(admin, user, password, namespaces)  # idempotent, existing volume
        admin.execute(
            sql.SQL("DROP VIEW {}").format(sql.Identifier(namespaces[1], "typed"))
        )
        admin.execute(
            sql.SQL("CREATE VIEW {} AS SELECT * FROM {}").format(
                sql.Identifier(namespaces[1], "typed"),
                sql.Identifier(namespaces[0], "readings"),
            )
        )
    assert client.get(f"/api/relations/{namespaces[1]}/typed/rows").status_code == 200
    # The same cluster also hosts Airflow, whose private tables must be unreadable.
    with psycopg.connect(os.environ["TEST_DATABASE_URL"], autocommit=True) as admin:
        exists = admin.execute(
            "SELECT 1 FROM pg_database WHERE datname='airflow'"
        ).fetchone()
    if exists:
        with psycopg.connect(**{**viewer_options, "dbname": "airflow"}) as viewer:
            assert not viewer.execute("""
                SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='public' AND c.relkind='r' AND has_table_privilege(c.oid,'SELECT'))
            """).fetchone()[0]


@pytest.mark.integration
def test_refresh_discovers_relations_and_returns_sanitized_errors(
    explorer, monkeypatch
):
    client, namespaces, *_ = explorer
    with psycopg.connect(os.environ["TEST_DATABASE_URL"]) as admin:
        admin.execute(
            sql.SQL("CREATE TABLE {} (value int)").format(
                sql.Identifier(namespaces[2], "new_data")
            )
        )
    assert any(
        r["name"] == "new_data"
        for r in client.get("/api/relations").json()["relations"]
    )
    assert (
        client.get(f"/api/relations/{namespaces[2]}/new_data/rows").json()["rows"] == []
    )

    def unavailable(*args, **kwargs):
        raise psycopg.OperationalError("internal error with secret-password and SQL")

    monkeypatch.setattr(app.state.pool, "connection", unavailable)
    response = client.get("/api/relations")
    assert response.status_code == 503
    assert "secret-password" not in response.text and "SQL" not in response.text
