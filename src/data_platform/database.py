"""Initialize database accounts; run only as the administrator, before services.

Existing application objects are reassigned in place, preserving their contents.
Runtime services never receive this administrator's credentials.
"""

import logging
import os

import psycopg
from psycopg import sql

from data_platform.dashboard import schemas
from data_platform.dashboard.provision import provision
from data_platform.sources.eia930 import DATASETS, _vintage_table_sql

logger = logging.getLogger(__name__)


def require_secret(name):
    value = os.environ.get(name, "")
    if not value or value.startswith(("change-me", "replace-with")):
        raise ValueError(f"Set a nonempty, non-example {name}")
    return value


def connect(database):
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=database,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        autocommit=True,
    )


def prepare_schemas(connection, namespaces, owner):
    """Initialize private schemas and transfer existing application objects."""
    for namespace in namespaces:
        connection.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION {}").format(
                sql.Identifier(namespace),
                sql.Identifier(owner),
            )
        )
        connection.execute(
            sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                sql.Identifier(namespace),
                sql.Identifier(owner),
            )
        )
        connection.execute(
            sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(
                sql.Identifier(namespace)
            )
        )
        objects = connection.execute(
            """
            SELECT c.relname, c.relkind FROM pg_class c
            JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname=%s AND c.relkind IN ('r','p','v','m','S','f')
              AND c.relowner <> (SELECT oid FROM pg_roles WHERE rolname=%s)
            ORDER BY CASE WHEN c.relkind='S' THEN 1 ELSE 0 END, c.relname
        """,
            (namespace, owner),
        ).fetchall()
        for name, kind in objects:
            object_type = {
                "v": "VIEW",
                "m": "MATERIALIZED VIEW",
                "S": "SEQUENCE",
                "f": "FOREIGN TABLE",
            }.get(kind, "TABLE")
            connection.execute(
                sql.SQL("ALTER {} {} OWNER TO {}").format(
                    sql.SQL(object_type),
                    sql.Identifier(namespace, name),
                    sql.Identifier(owner),
                )
            )


def prepare_eia930_sources(connection):
    """Allow dbt to build empty views before the first ingestion on a fresh VM."""
    bronze = os.getenv("DBT_BRONZE_SCHEMA", "bronze")
    for settings in DATASETS.values():
        connection.execute(_vintage_table_sql(
            bronze, settings["table"], settings["category_column"],
        ))


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    for name in (
        "POSTGRES_PASSWORD",
        "WAREHOUSE_PASSWORD",
        "AIRFLOW_DB_PASSWORD",
        "VIEWER_PASSWORD",
    ):
        require_secret(name)
    warehouse = os.environ["POSTGRES_DB"]
    writer = os.environ["WAREHOUSE_USER"]
    airflow = os.environ["AIRFLOW_DB_USER"]
    viewer = os.environ["VIEWER_USER"]
    if len({writer, airflow, viewer, os.environ["POSTGRES_USER"]}) != 4:
        raise ValueError(
            "Administrator, writer, Airflow, and viewer accounts must differ"
        )
    with connect("postgres") as connection:
        for user, password in (
            (writer, os.environ["WAREHOUSE_PASSWORD"]),
            (airflow, os.environ["AIRFLOW_DB_PASSWORD"]),
        ):
            if not connection.execute(
                "SELECT 1 FROM pg_roles WHERE rolname=%s", (user,)
            ).fetchone():
                connection.execute(
                    sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(user))
                )
            connection.execute(
                sql.SQL("""
                ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                NOREPLICATION NOBYPASSRLS PASSWORD {}
            """).format(sql.Identifier(user), sql.Literal(password))
            )
        if not connection.execute(
            "SELECT 1 FROM pg_database WHERE datname='airflow'"
        ).fetchone():
            connection.execute(
                sql.SQL("CREATE DATABASE airflow OWNER {}").format(
                    sql.Identifier(airflow)
                )
            )
        connection.execute(
            sql.SQL("ALTER DATABASE airflow OWNER TO {}").format(
                sql.Identifier(airflow)
            )
        )
        for (database,) in connection.execute(
            "SELECT datname FROM pg_database WHERE datallowconn"
        ).fetchall():
            connection.execute(
                sql.SQL("REVOKE CONNECT, TEMPORARY ON DATABASE {} FROM PUBLIC").format(
                    sql.Identifier(database)
                )
            )
        # CREATE permits schemas inside the warehouse, never new databases.
        for database, user, privileges in (
            (warehouse, writer, "CONNECT, CREATE"),
            ("airflow", airflow, "CONNECT, TEMPORARY"),
        ):
            connection.execute(
                sql.SQL("GRANT {} ON DATABASE {} TO {}").format(
                    sql.SQL(privileges), sql.Identifier(database), sql.Identifier(user)
                )
            )
    with connect(warehouse) as connection, connection.transaction():
        connection.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        prepare_eia930_sources(connection)
        prepare_schemas(connection, schemas(), writer)
        provision(
            connection, viewer, os.environ["VIEWER_PASSWORD"], schemas(), owner=writer
        )
    with connect("airflow") as connection, connection.transaction():
        prepare_schemas(connection, ["public"], airflow)
    logger.info("Database accounts and ownership ready; existing data retained")


if __name__ == "__main__":
    main()
