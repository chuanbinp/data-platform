# Data Platform

A reusable Python ingestion platform with Airflow, dbt, Docker, and Postgres.
EIA-930 electricity data is the first source.

## Setup

Requires Docker Compose. Run commands from the project directory.

```sh
cp .env.example .env
```

Set `POSTGRES_PASSWORD`, `WAREHOUSE_PASSWORD`, `AIRFLOW_DB_PASSWORD`, a separate random `VIEWER_PASSWORD`,
`AIRFLOW_ADMIN_PASSWORD`, a random `AIRFLOW_JWT_SECRET`, and `EIA_API_KEY`
([register here](https://www.eia.gov/opendata/register.php)).

```sh
chmod 600 .env
docker compose up -d --build --wait
```

- **Airflow:** <http://localhost:8000>; use the admin credentials in `.env`.
- **Postgres:** `127.0.0.1:5000`, database `warehouse`; credentials are in `.env`.
- **Data explorer:** <http://localhost:8080>; read-only Bronze, Silver, and Gold tables and views.
- Enable `eia930_ingestion` to poll every 15 minutes. Progress and errors appear in task logs.

Ports and schemas are configured in `.env`.
Changing `.env` does not update existing database or Airflow account passwords.

After successful ingestion, you can also run dbt manually:

```sh
docker compose run --rm dbt debug
docker compose run --rm dbt build
```

The dbt service reuses the Airflow image and exits after each command.
One Postgres container hosts both databases (analytics data + airflow metadata); `warehouse_pgdata` persists their
contents and `airflow_logs` persists task logs. Containers use `postgres:5432`.
`docker compose down` preserves volumes.

## Architecture

### Reusable platform

| Component | Purpose |
| --- | --- |
| `src/data_platform/downloader.py` | HTTP retries, exponential backoff, and jitter |
| `src/data_platform/ingestor.py` | Atomic Postgres loads and changed-only vintage history |
| `compose.yaml` / `Dockerfile.airflow` | Shared infrastructure and separate Airflow/dbt Python environments |
| `docker/airflow_init.py` / `dbt/profiles.yml` | Airflow initialization and dbt warehouse connection |
| `src/data_platform/dashboard/` / `Dockerfile.dashboard` | Reusable, read-only data explorer and viewer provisioning |

### EIA-930 pipeline

**Legend:** blue = shared platform, green = pipeline-specific code/data,
gray = external source. 

**Task orchestration:** two independent chains, scheduled every 15 minutes.

```mermaid
flowchart TB
    subgraph runtime["Shared platform · Airflow scheduling and dbt execution"]
        direction TB
        subgraph region["EIA-930 · Region pipeline"]
            direction LR
            rb["bronze_region_data<br/>Python: download and ingest"]
            rs["silver_region_data<br/>dbt: source tests and staging"]
            rg["gold_region_data<br/>dbt: history, latest mart, and tests"]
            rb --> rs --> rg
        end
        subgraph fuel["EIA-930 · Fuel pipeline"]
            direction LR
            fb["bronze_fuel_type_data<br/>Python: download and ingest"]
            fs["silver_fuel_type_data<br/>dbt: source tests and staging"]
            fg["gold_fuel_type_data<br/>dbt: history, latest mart, and tests"]
            fb --> fs --> fg
        end
    end
    classDef default fill:#eaf6e9,stroke:#2e6b35,color:#193a1d
    style runtime stroke:#1e4e8c
    style region stroke:#2e6b35
    style fuel stroke:#2e6b35
```

**Data flow:** shared Python components load source-specific tables; dbt builds
the downstream views through model references.

```mermaid
flowchart TD
    api["External · EIA API"]
    download["Shared · Downloader<br/>Retries, backoff, and jitter"]
    client["EIA-930 · EIA930Client<br/>Pagination and field mapping"]
    ingest["Shared · PostgresIngestor<br/>Atomic loads; append changed vintages"]
    api --> download --> client --> ingest

    subgraph bronze["Bronze · source tables"]
        region_raw["eia930_region_hourly_vintages"]
        fuel_raw["eia930_fuel_type_hourly_vintages"]
    end
    ingest -->|region records| region_raw
    ingest -->|fuel records| fuel_raw

    subgraph silver["Silver · typed views; all vintages retained"]
        region_typed["electricity_metrics_hourly_vintages<br/>UTC periods and numeric values"]
        fuel_typed["generation_by_fuel_hourly_vintages<br/>UTC periods and numeric values"]
    end
    region_raw --> region_typed
    fuel_raw --> fuel_typed

    subgraph gold["Gold · analytics views"]
        region_history["electricity_metrics_hourly_history<br/>Per-metric knowledge intervals"]
        region_latest["electricity_metrics_hourly_latest<br/>One row per hour and respondent"]
        fuel_history["generation_by_fuel_hourly_history<br/>Per-fuel knowledge intervals"]
        fuel_latest["generation_by_fuel_hourly_latest<br/>One row per hour and respondent; fuel columns"]
        region_history -->|current versions; pivot metrics| region_latest
        fuel_history -->|current versions; pivot fuels| fuel_latest
    end
    region_typed --> region_history
    fuel_typed --> fuel_history

    classDef default fill:#eaf6e9,stroke:#2e6b35,color:#193a1d
    classDef shared fill:#e8f1ff,stroke:#1e4e8c,color:#12385c
    classDef external fill:#f1f3f5,stroke:#59636e,color:#24292f
    class download,ingest shared
    class api external
```

- **Bronze:** download each dataset and append changed versions in one transaction.
- **Silver:** validate the source and build typed staging views from `dbt/models/staging/eia930/`.
- **Gold:** build the dataset's history and dashboard views from `dbt/models/marts/`.

There is one active DAG run at a time. Each downstream task requires its own
upstream task to succeed. Gold does not rebuild Silver.

Unchanged polls add no rows; A → B → A retains all three versions. History keeps
`observed_at`, `inserted_at`, and `ingestion_run_id`. Silver and Gold are views,
so committed source changes are visible immediately; dbt builds validate data
and update view definitions.

### Gold marts

| View in `gold` | Grain |
| --- | --- |
| `electricity_metrics_hourly_history` | Hour × respondent × metric × version |
| `electricity_metrics_hourly_latest` | Hour × respondent; demand, forecast, generation, and interchange columns |
| `generation_by_fuel_hourly_history` | Hour × respondent × fuel × version |
| `generation_by_fuel_hourly_latest` | Hour × respondent; generation and unit columns per fuel |

Use a point-in-time query to retrieve the versions observed as of a UTC cutoff:

```sql
SELECT period, respondent, metric, value, value_units
FROM gold.electricity_metrics_hourly_history
WHERE known_from <= TIMESTAMPTZ '2026-11-24 00:00:00+00'
  AND (known_until > TIMESTAMPTZ '2026-11-24 00:00:00+00'
       OR known_until IS NULL);
```

The cutoff uses collection time, not EIA publication time. Latest marts pivot
current versions, keeping missing values as NULL; detailed provenance stays in history.

## Tech stack and Docker services

| Technology | Role |
| --- | --- |
| Python, HTTPX, psycopg | Reusable API downloads and changed-only vintage ingestion |
| Airflow LocalExecutor | Scheduling, Python tasks, and dbt subprocesses |
| dbt | Tested Silver and Gold views |
| PostgreSQL 17 | Warehouse and separate Airflow metadata database |
| FastAPI + Tabulator | Read-only data explorer; refreshes every 60 seconds |
| Cloudflare Tunnel | Production HTTPS access without a public VM IP |
| Docker Compose | Services, networks, initialization, and persistent storage |

### How services connect

```mermaid
flowchart LR
    browser["Browser"]
    cloudflare["Cloudflare HTTPS"]
    eia["EIA API"]
    subgraph docker["Docker Compose host"]
        tunnel["cloudflared · production only"]
        dashboard["dashboard · FastAPI + Tabulator"]
        airflow["Airflow API · scheduler · DAG processor"]
        tasks["LocalExecutor · Python + dbt"]
        subgraph postgres["Postgres · port 5432"]
            warehouse["warehouse · Bronze / Silver / Gold"]
            metadata["airflow · metadata"]
        end
    end
    browser -->|"production HTTPS"| cloudflare
    cloudflare -->|"outbound encrypted tunnel"| tunnel
    tunnel -->|"internal HTTP 8080"| dashboard
    browser -->|"local HTTP · localhost:8080"| dashboard
    browser -->|"local / IAP tunnel · localhost:8000"| airflow
    airflow --> metadata
    airflow --> tasks
    tasks -->|"HTTPS"| eia
    tasks -->|"writer"| warehouse
    dashboard -->|"read-only"| warehouse
    classDef service fill:#e8f1ff,stroke:#1e4e8c,color:#12385c
    classDef database fill:#eaf6e9,stroke:#2e6b35,color:#193a1d
    class tunnel,dashboard,airflow,tasks service
    class warehouse,metadata database
    style docker fill:#111e2e,stroke:#1e4e8c,color:#e4ebf4
    style postgres fill:#1b2b3c,stroke:#2e6b35,color:#e4ebf4
```

No separate Airflow worker is needed. The optional `dbt` container reuses the
Airflow image for manual commands and exits afterward. Credentials stay on the server.

### Networks and ports

| Network | Members | Purpose |
| --- | --- | --- |
| `explorer` | Dashboard locally; cloudflared in production | Local port publication or outbound tunnel connection |
| `tunnel` | cloudflared, dashboard (production only) | Internal HTTP; only the connector may supply visitor IPs |
| `warehouse` | Dashboard, Postgres | Internal read-only database connection |
| `pipeline` | Airflow, dbt, database initialization, Postgres | Warehouse writes, metadata, and outbound EIA calls |

Production dashboard containers have no internet route and share no network with
Airflow. The connector has no direct database connection. Containers use service
names such as `dashboard:8080` and `postgres:5432`.

| Host endpoint | Local | Production |
| --- | --- | --- |
| `127.0.0.1:8080` | Dashboard | Unpublished; use the Cloudflare hostname |
| `127.0.0.1:8000` | Airflow | Airflow through an IAP SSH tunnel |
| `127.0.0.1:5000` | Postgres | Postgres through an IAP SSH tunnel |

### Images, startup, and storage

Two custom images contain Airflow/dbt and the dashboard. Postgres and cloudflared
use pinned official images. Local development mounts `src/`, `dags/`, and `dbt/`;
production uses files baked into images. Compose passes only the configured
variables to each service; `.env` is never baked into an image.

Postgres starts first. `database-init` prepares accounts and viewer grants;
`airflow-init` prepares Airflow metadata and the UI account. Both initializers exit.
The dashboard, Airflow services, and production tunnel then start.

| Named volume | Container path | Persists |
| --- | --- | --- |
| `warehouse_pgdata` | `/var/lib/postgresql/data` | Warehouse and Airflow databases |
| `airflow_logs` | `/opt/airflow/logs` | Task logs |

Volumes survive container recreation and `docker compose down`; `down -v` deletes
them. Existing certificate volumes from earlier setups are left untouched.

## Development setup and tests

Install [uv](https://docs.astral.sh/uv/) and sync the local environment:

```sh
uv sync --locked --extra dev --python 3.12
uv run --locked pytest -m 'not integration'
```

Include dashboard dependencies to run its API and security tests:

```sh
uv sync --locked --extra dev --extra dashboard --group dbt
uv run --locked --extra dev --extra dashboard pytest -m 'not integration'
```

Python, DAG, and dbt files are mounted into containers. Rebuild after changing
dependencies or the Dockerfile. Generated dbt artifacts stay under `/tmp/dbt`
in containers; local `dbt/target/` and `dbt/logs/` are Git-ignored and safe to delete.

**Database and dbt fixture tests:** use a disposable Postgres instance. The
application database deliberately rejects the temporary accounts used by tests.

```sh
docker run --rm -d --name data-platform-tests \
  -e POSTGRES_DB=warehouse -e POSTGRES_USER=test_admin \
  -e POSTGRES_PASSWORD=test-only-password -p 127.0.0.1:55432:5432 postgres:17
until docker exec data-platform-tests pg_isready -U test_admin -d warehouse; do sleep 1; done
TEST_DATABASE_URL='postgresql://test_admin:test-only-password@127.0.0.1:55432/warehouse' \
  .venv/bin/pytest -m integration
docker stop data-platform-tests
```

**Airflow DAG tests:** run inside the built image.

```sh
docker compose run --rm --no-deps \
  --volume ./tests:/opt/data-platform/tests:ro \
  --volume ./dags:/opt/data-platform/dags:ro \
  airflow-scheduler python -m unittest discover \
  -s /opt/data-platform/tests -p test_airflow_dag.py
```

## Security

A single `database-init` step prepares runtime accounts, schema ownership, and
viewer grants. Postgres has separate administrator, warehouse writer, Airflow,
and read-only viewer accounts. Airflow and dashboard containers do not receive
database administrator credentials.
In production, the dashboard cannot directly reach Airflow and has no internet
access. All published ports bind to localhost. Keep `.env` private (`chmod 600 .env`).
Locally, FastAPI serves `localhost:8080` directly. In production, Cloudflare Tunnel
provides HTTPS without publishing the dashboard port. Only the connector's fixed
Docker address may supply Cloudflare visitor headers; arbitrary forwarded headers
are ignored. The application accepts request bodies up to 1 KiB with a 10-second
body timeout.
The API permits 120 requests/minute/IP with a burst of 30, 16 KiB query strings,
and offsets up to 10,000 rows. Narrow filters to browse beyond that limit.
Bronze/Silver/Gold are public schemas in the explorer, including future tables.
Backups contain database credentials and data; keep them private.

Use `docker compose build --no-cache` to refresh OS updates. Airflow dependencies
are pinned with hashes in `docker/airflow.lock`; update that lock from
`docker/airflow.in` with `uv pip compile` when applying Python security updates.

## Deployment

GitHub Actions tests pushes to `master`, builds two images, and deploys through
GCP IAP. Cloudflare Tunnel exposes only the read-only dashboard. The VM keeps its
private IP; Airflow and Postgres remain localhost-only.

See [GCP and Cloudflare setup](docs/deployment.md) for one-time configuration,
required GitHub variables, deployment checks, and rollback. The 2 GB VM uses
one Airflow task and one dbt thread at a time; swap provides limited headroom.
