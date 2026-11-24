"""Provision viewer grants separately; never give the web app admin credentials."""

import logging
import os

import psycopg
from psycopg import sql

from data_platform.dashboard import schemas

logger = logging.getLogger(__name__)


def provision(connection, user, password, allowed_schemas):
    if not user or not password:
        raise ValueError("Set a nonempty VIEWER_USER and VIEWER_PASSWORD")
    owner, database = connection.execute(
        "SELECT current_user, current_database()"
    ).fetchone()
    if user == owner:
        raise ValueError("Viewer must differ from the pipeline/database owner")
    role = sql.Identifier(user)
    existing = connection.execute(
        "SELECT oid FROM pg_roles WHERE rolname = %s", (user,)
    ).fetchone()
    if existing:
        # Refuse to repurpose an existing privileged/owning/member role.
        unsafe = connection.execute(
            """
            SELECT EXISTS(SELECT 1 FROM pg_auth_members WHERE member = %s OR roleid = %s)
                OR EXISTS(SELECT 1 FROM pg_shdepend WHERE refobjid = %s AND deptype = 'o')
        """,
            (existing[0], existing[0], existing[0]),
        ).fetchone()[0]
        if unsafe:
            raise ValueError(
                "Existing viewer role owns objects or has role memberships; use a new role"
            )
    else:
        connection.execute(sql.SQL("CREATE ROLE {} LOGIN").format(role))
    connection.execute(
        sql.SQL("""
        ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
        NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4 PASSWORD {}
    """).format(role, sql.Literal(password))
    )
    connection.execute(
        sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(role)
    )
    connection.execute(
        sql.SQL("ALTER ROLE {} SET statement_timeout = '5s'").format(role)
    )
    connection.execute(sql.SQL("ALTER ROLE {} SET lock_timeout = '1s'").format(role))
    connection.execute(
        sql.SQL("ALTER ROLE {} SET idle_in_transaction_session_timeout = '10s'").format(
            role
        )
    )
    connection.execute(
        sql.SQL("REVOKE ALL ON DATABASE {} FROM {}").format(
            sql.Identifier(database), role
        )
    )
    connection.execute(
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(database), role
        )
    )
    for schema in allowed_schemas:
        namespace = sql.Identifier(schema)
        connection.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION {}").format(
                namespace, sql.Identifier(owner)
            )
        )
        connection.execute(
            sql.SQL("REVOKE ALL ON SCHEMA {} FROM {}").format(namespace, role)
        )
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(namespace, role)
        )
        connection.execute(
            sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM {}").format(
                namespace, role
            )
        )
        connection.execute(
            sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                namespace, role
            )
        )
        connection.execute(
            sql.SQL("""
            ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} GRANT SELECT ON TABLES TO {}
        """).format(sql.Identifier(owner), namespace, role)
        )
    logger.info("Viewer grants ready: schemas=%s", ",".join(allowed_schemas))


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    try:
        with psycopg.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=5,
        ) as connection:
            provision(
                connection,
                os.environ["VIEWER_USER"],
                os.environ["VIEWER_PASSWORD"],
                schemas(),
            )
    except Exception as error:
        logger.error("Viewer provisioning failed: error_type=%s", type(error).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
