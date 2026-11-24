# Data Platform

A reusable Python ingestion platform with Airflow, Docker, and Postgres.
EIA-930 electricity data is the first source.

## Setup

Requires Docker Compose. Run commands from the project directory.

```sh
cp .env.example .env
```

Set the platform credentials: `POSTGRES_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`,
and a random `AIRFLOW_JWT_SECRET`. For EIA-930, also set `EIA_API_KEY`
([register here](https://www.eia.gov/opendata/register.php)).

```sh
docker compose up -d --build --wait
```

- **Airflow:** <http://localhost:8000>; log in with the admin credentials in `.env`.
- **Postgres:** `127.0.0.1:5000`, database `warehouse`; credentials are in `.env`.
- Enable `eia930_ingestion` in Airflow to poll every 15 minutes. View progress in task logs.

Ports come from `.env`. Initial database credentials and the Airflow account are
created once; editing `.env` does not update existing passwords.

## Architecture

### Reusable platform

| Component | Responsibility |
| --- | --- |
| `src/data_platform/downloader.py` | HTTP retries, exponential backoff, and jitter for any API |
| `src/data_platform/ingestor.py` | Atomic Postgres loads and changed-only vintage history for any source |
| `compose.yaml` | Shared Postgres and Airflow services with persistent volumes |
| `Dockerfile.airflow` | Installs the Python package and dependencies for tasks |
| `docker/airflow_init.py` | Creates and initializes Airflow's metadata database |

One Postgres container hosts the `warehouse` and `airflow` databases. Both persist
in `warehouse_pgdata` named volume; task logs use `airflow_logs`. Containers connect to `postgres:5432`.
Each vintage retains `observed_at`, `inserted_at`, and `ingestion_run_id`.
A → B → A keeps all three versions; unchanged polls add no rows.

### EIA-930-specific pipeline

| Component | Responsibility |
| --- | --- |
| `src/data_platform/sources/eia930.py` | API pagination, field mapping, Bronze table definitions, and CLI |
| `dags/eia930_ingestion.py` | Dataset selection, polling schedule, and ingestion window |

```mermaid
flowchart TD
    subgraph dag["Airflow DAG · every 15 minutes"]
        region[ingest_region_data]
        fuel[ingest_fuel_type_data]
    end

    subgraph platform["Reusable platform"]
        downloader["Downloader · retries and backoff"]
        ingestor["PostgresIngestor · changed-only vintages"]
    end

    api[EIA API] -->|HTTP responses| downloader
    downloader --> client["EIA930Client · pagination and field mapping"]
    region -.->|calls| client
    fuel -.->|calls| client
    client -->|source records| ingestor

    subgraph bronze["Postgres · warehouse database · bronze schema"]
        region_table[eia930_region_data]
        fuel_table[eia930_fuel_type_data]
    end
    ingestor --> region_table
    ingestor --> fuel_table
```

Solid arrows show data flow; dotted arrows show function calls. Both ingestion
tasks can run in parallel, with one active DAG run at a time.
Each task loads its corresponding Bronze table in one transaction. To add another
source, provide its client, table definition, and DAG using the shared platform.

Planned next: dbt staging views in `silver`, followed by marts in `gold` and a dashboard.

## Development setup and tests

Install [uv](https://docs.astral.sh/uv/) and sync the Python environment:

```sh
uv sync --locked --extra dev --python 3.12
```

Python source and DAGs are mounted into Airflow, so edits are picked up without
rebuilding. Rebuild the image after changing dependencies or `Dockerfile.airflow`.

**Offline tests:** no API key or database required. Database and Airflow tests
are skipped when their prerequisites are unavailable.

```sh
uv run --locked pytest
```

**Database tests:** start Postgres and replace the example password with your
configured password. Tests create and remove isolated schemas.

```sh
docker compose up -d --wait postgres
TEST_DATABASE_URL='postgresql://admin:YOUR_PASSWORD@127.0.0.1:5000/warehouse' \
  uv run --locked pytest -m integration
```

**Airflow DAG tests:** run inside the built Airflow image.

```sh
docker compose run --rm --no-deps \
  --volume ./tests:/opt/data-platform/tests:ro \
  --volume ./dags:/opt/data-platform/dags:ro \
  airflow-scheduler python -m unittest discover \
  -s /opt/data-platform/tests -p test_airflow_dag.py
```

Tests cover HTTP retries, API pagination, field mapping, atomic rollback,
changed-only vintages, point-in-time queries, and DAG parameters.
