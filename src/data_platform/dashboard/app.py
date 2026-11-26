"""Serve a bounded, read-only API and the buildless data explorer."""

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Literal

import psycopg
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout, TooManyRequests

from data_platform.dashboard import SOURCES, relation_source, schemas
from data_platform.dashboard.security import ApiLimits, RequestBounds

logger = logging.getLogger(__name__)
STATIC = Path(__file__).with_name("static")
DEFAULT_SORT_COLUMNS = (
    "observed_at",
    "last_observed_at",
    "inserted_at",
    "last_inserted_at",
    "period",
)


def database_options():
    # Deliberately do not fall back to the ingestion/admin credentials.
    return {
        "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
        "port": os.getenv("POSTGRES_PORT", "5000"),
        "dbname": os.getenv("POSTGRES_DB", "warehouse"),
        "user": os.environ["VIEWER_USER"],
        "password": os.environ["VIEWER_PASSWORD"],
        "connect_timeout": 5,
        "options": "-c default_transaction_read_only=on -c statement_timeout=5000 "
        "-c lock_timeout=1000 -c timezone=UTC -c search_path=pg_catalog",
        "application_name": "warehouse-viewer",
        "row_factory": dict_row,
    }


@asynccontextmanager
async def lifespan(app):
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    pool = ConnectionPool(
        kwargs=database_options(),
        min_size=1,
        max_size=4,
        max_waiting=16,
        timeout=3,
        open=False,
    )
    app.state.pool = pool
    try:
        pool.open(wait=True, timeout=10)
        with pool.connection() as connection:
            role = connection.execute("""
                SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
                FROM pg_roles WHERE rolname = current_user
            """).fetchone()
            if any(role.values()):
                raise RuntimeError(
                    "Dashboard requires a non-administrative viewer login"
                )
        logger.info("Dashboard ready: schemas=%s pool_max=4", ",".join(schemas()))
        yield
    finally:
        pool.close()


app = FastAPI(
    title="Warehouse Explorer",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.add_middleware(ApiLimits)
app.add_middleware(RequestBounds)


@app.middleware("http")
async def response_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "same-origin"
    # Tabulator creates inline styles for column sizing; scripts stay local.
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
        "connect-src 'self'; img-src 'self' data:; object-src 'none'; "
        "base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
    )
    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    return response


@app.exception_handler(psycopg.Error)
@app.exception_handler(PoolTimeout)
@app.exception_handler(TooManyRequests)
async def database_error(request: Request, error):
    # No SQL, credentials, exception details, or query values in public responses/logs.
    logger.warning(
        "Dashboard query failed: path=%s error_type=%s",
        request.url.path,
        type(error).__name__,
    )
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Data is temporarily unavailable. Narrow your filters or retry shortly."
        },
    )


def relation(connection, schema, table):
    if schema not in schemas():
        raise HTTPException(404, "Relation not found")
    found = connection.execute(
        """
        SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('r','p','v','m')
          AND has_schema_privilege(n.oid, 'USAGE') AND has_table_privilege(c.oid, 'SELECT')
    """,
        (schema, table),
    ).fetchone()
    if not found:
        raise HTTPException(404, "Relation not found")
    return connection.execute(
        """
        SELECT attname AS name, format_type(atttypid, atttypmod) AS type,
               NOT attnotnull AS nullable
        FROM pg_attribute WHERE attrelid = %s AND attnum > 0 AND NOT attisdropped
        ORDER BY attnum
    """,
        (found["oid"],),
    ).fetchall()


def encode(value):
    """Send exact numbers as strings; JavaScript must not round warehouse values."""
    if isinstance(value, Decimal):
        return str(value)
    if (
        isinstance(value, int)
        and not isinstance(value, bool)
        and abs(value) > 2**53 - 1
    ):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo:
            value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: encode(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [encode(item) for item in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    # Float NaN/Infinity and binary/custom PostgreSQL types remain displayable.
    return str(value)


@app.get("/api/health")
def health():
    with app.state.pool.connection() as connection:
        connection.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/api/relations")
def relations():
    with app.state.pool.connection() as connection:
        rows = connection.execute(
            """
            SELECT n.nspname AS schema, c.relname AS name,
                   CASE WHEN c.relkind IN ('v','m') THEN 'view' ELSE 'table' END AS kind
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s) AND c.relkind IN ('r','p','v','m')
              AND has_schema_privilege(n.oid, 'USAGE')
              AND has_table_privilege(c.oid, 'SELECT')
            ORDER BY n.nspname, c.relname
        """,
            (list(schemas()),),
        ).fetchall()
    return {
        "schemas": schemas(),
        "sources": [{"id": source["id"], "label": source["label"]} for source in SOURCES],
        "relations": [dict(row, source=relation_source(row["name"])) for row in rows],
    }


@app.get("/api/relations/{schema}/{table}/columns")
def columns(schema: str, table: str):
    with app.state.pool.connection() as connection:
        return {"columns": relation(connection, schema, table)}


@app.get("/api/relations/{schema}/{table}/values")
def column_values(
    schema: str,
    table: str,
    column: str = Query(max_length=63),
    search: str = Query("", max_length=500),
):
    """Look up distinct values across the relation, not just the visible page."""
    with app.state.pool.connection() as connection:
        metadata = relation(connection, schema, table)
        if column not in {item["name"] for item in metadata}:
            raise HTTPException(422, "Unknown column")
        records = connection.execute(
            sql.SQL("""
            SELECT DISTINCT {column}::text AS value FROM {table}
            WHERE strpos(lower(coalesce({column}::text, 'NULL')), lower(%s)) > 0
            ORDER BY value NULLS FIRST LIMIT 101
        """).format(column=sql.Identifier(column), table=sql.Identifier(schema, table)),
            (search,),
        ).fetchall()
    return {
        "values": [item["value"] for item in records[:100]],
        "has_more": len(records) > 100,
    }


def filter_conditions(columns, raw_values):
    """Build parameterized filters: OR within a column, AND across columns."""
    if len(columns) != len(raw_values):
        raise HTTPException(422, "Supply a column and value for every exact filter")
    matches = {}
    for column, raw in zip(columns, raw_values):
        # JSON distinguishes SQL NULL, an empty string, and the literal 'null'.
        try:
            if len(raw) > 10_000:
                raise ValueError
            value = json.loads(raw)
            if value is not None and not isinstance(value, str):
                raise ValueError
        except (ValueError, TypeError):
            raise HTTPException(422, "Invalid exact filter value") from None
        matches.setdefault(column, []).append(value)
    conditions, values = [], []
    for column, selected in matches.items():
        conditions.append(
            sql.SQL("({0}::text = ANY(%s::text[]) OR ({0} IS NULL AND %s))").format(
                sql.Identifier(column)
            )
        )
        values.extend(
            ([value for value in selected if value is not None], None in selected)
        )
    where = (
        sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conditions)
        if conditions
        else sql.SQL("")
    )
    return where, values


def sort_order(metadata, sort, direction, schema):
    """Use layer-specific defaults unless a column sort was requested."""
    column_types = {column["name"]: column["type"] for column in metadata}
    layers = schemas()
    if schema == layers[0]:
        preferred = ("observed_at",)
    elif schema == layers[-1]:
        preferred = ("period", "known_from")
    else:
        preferred = ("period", "observed_at")
    columns = [column for column in preferred if column in column_types]
    if not columns:
        fallback = next(
            (column for column in DEFAULT_SORT_COLUMNS if column in column_types),
            next(iter(column_types), None),
        )
        columns = [fallback] if fallback else []
    if sort:
        columns = [sort]
    direction = direction if sort else "desc"
    clauses = []
    for column in columns:
        expression = sql.Identifier(column)
        if column_types[column] == "json":
            expression = sql.SQL("{}::jsonb").format(expression)
        clauses.append(sql.SQL("{} {} NULLS LAST").format(
            expression, sql.SQL(direction.upper())
        ))
    clauses.append(sql.SQL("to_jsonb(t)::text ASC"))
    return sql.SQL(", ").join(clauses), {
        "column": columns[0] if columns else None,
        "columns": columns,
        "direction": direction,
        "default": not sort,
    }


@app.get("/api/relations/{schema}/{table}/rows")
def rows(
    schema: str,
    table: str,
    page: int = Query(1, ge=1, le=10_000_000),
    page_size: int = Query(100, ge=1, le=100),
    sort: str | None = Query(None, max_length=63),
    direction: Literal["asc", "desc"] = "desc",
    match_column: list[str] = Query(default=[], max_length=500),
    match_value: list[str] = Query(default=[], max_length=500),
):
    if (page - 1) * page_size > 10_000:
        raise HTTPException(422, "Pagination limit reached. Narrow your filters to see more records.")
    with app.state.pool.connection() as connection:
        metadata = relation(connection, schema, table)
        names = {column["name"] for column in metadata}
        if (sort and sort not in names) or any(
            column not in names for column in match_column
        ):
            raise HTTPException(422, "Unknown column")
        where, values = filter_conditions(match_column, match_value)
        order, sorting = sort_order(metadata, sort, direction, schema)
        query = sql.SQL(
            "SELECT t.* FROM {} AS t{} ORDER BY {} LIMIT %s OFFSET %s"
        ).format(sql.Identifier(schema, table), where, order)
        values.extend((page_size + 1, (page - 1) * page_size))
        records = connection.execute(query, values).fetchall()
    return JSONResponse(
        {
            "rows": encode(records[:page_size]),
            "page": page,
            "has_more": len(records) > page_size,
            "sort": sorting,
            "refreshed_at": datetime.now(timezone.utc).isoformat(),
        }
    )


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


app.mount("/static", StaticFiles(directory=STATIC), name="static")
