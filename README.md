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

| Technology | Service / runtime | Role |
| --- | --- | --- |
| Python, HTTPX, psycopg | Airflow Python tasks | Download EIA data with retries and ingest changed vintages |
| Airflow | `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor` | Serve the UI/execution API, schedule tasks, and parse DAG definitions |
| dbt | Airflow tasks or the optional `dbt` container | Build and test Silver/Gold views using SQL model references |
| PostgreSQL 17 | `postgres` | Host the `warehouse` database and a separate `airflow` metadata database |
| FastAPI, Tabulator | `dashboard` | Serve the read-only API and browser table explorer; the browser polls every 60 seconds |
| Caddy | `dashboard-proxy` | Forward browser requests; manage HTTPS certificates when a domain is configured |
| Docker Compose | All services | Build images, connect networks, order startup, and manage persistent volumes |

### How services connect

```mermaid
flowchart LR
    browser["Browser"]
    eia["EIA API"]

    subgraph docker["Docker Compose host"]
        caddy["dashboard-proxy · Caddy"]
        dashboard["dashboard · FastAPI + Tabulator"]
        airflow["Airflow<br/>API server · scheduler · DAG processor"]
        tasks["LocalExecutor tasks<br/>Python ingestion · dbt"]
        subgraph postgres["postgres · one container · port 5432"]
            warehouse["warehouse database<br/>Bronze · Silver · Gold"]
            metadata["airflow database<br/>DAG/task state · UI accounts"]
        end
    end

    browser -->|"localhost:8080 · HTTP"| caddy
    caddy -->|"proxy network · HTTP 8080"| dashboard
    dashboard -->|"warehouse network · read-only SQL"| warehouse
    browser -->|"localhost:8000 · Airflow UI"| airflow
    airflow -->|"pipeline network · metadata SQL"| metadata
    airflow -->|"scheduler starts local task processes"| tasks
    eia -->|"HTTPS responses"| tasks
    tasks -->|"pipeline network · ingestion and dbt SQL"| warehouse

    classDef service fill:#e8f1ff,stroke:#1e4e8c,color:#12385c
    classDef database fill:#eaf6e9,stroke:#2e6b35,color:#193a1d
    class caddy,dashboard,airflow,tasks service
    class warehouse,metadata database
    style docker fill:#111e2e,stroke:#1e4e8c,color:#e4ebf4
    style postgres fill:#1b2b3c,stroke:#2e6b35,color:#e4ebf4
```

Airflow's **LocalExecutor** runs tasks under the scheduler; no separate worker
service is needed. dbt runs as a subprocess in those tasks, using its own Python
environment inside the Airflow image. The optional `dbt` service reuses that
image for manual commands and exits when finished. Browsers access data through
FastAPI; database credentials stay on the server.

### Networks and ports

| Docker network | Members | Access |
| --- | --- | --- |
| `explorer` | Caddy | Gateway network for published ports and outbound certificate requests |
| `proxy` | Caddy, dashboard | Internal HTTP connection; only Caddy's address is trusted for forwarded client IPs |
| `warehouse` | Dashboard, Postgres | Internal database connection for the read-only viewer |
| `pipeline` | Airflow, dbt, database initialization, Postgres | Database access and outbound EIA requests |

The dashboard has no internet route and shares no network with Airflow. Caddy
has no direct connection to Postgres. Containers use service names such as
`dashboard:8080` and `postgres:5432`, rather than host-published ports.

| Host endpoint (defaults) | Container endpoint | Purpose |
| --- | --- | --- |
| `127.0.0.1:8080` | Caddy `8080` | Local HTTP dashboard |
| `127.0.0.1:8443` | Caddy `8443` | HTTPS when configured; local HTTP mode has no TLS listener |
| `127.0.0.1:8000` | Airflow API server `8080` | Airflow UI/API |
| `127.0.0.1:5000` | Postgres `5432` | Local database clients |

The dashboard's own port `8080` and Caddy's health port `8081` are internal only.
`.env` controls host mappings. Public HTTPS uses host ports 80/443 as described
in [HTTPS with Caddy](#https-with-caddy).

### Images, startup, and storage

`Dockerfile.airflow`, `Dockerfile.dashboard`, and `Dockerfile.caddy` build the
application images; Postgres uses a pinned official image. Airflow mounts
`src/`, `dags/`, and `dbt/` read-only for development. Dashboard code/assets are
built into its image, while Caddy's configuration is mounted read-only.
Compose reads `.env` and passes each service only its configured variables;
the file is not baked into images.

Postgres becomes healthy before `database-init` prepares accounts, ownership,
and viewer grants. The dashboard then starts, followed by Caddy. Separately,
`airflow-init` prepares metadata and the UI account before Airflow services start.
Both initialization services exit after completion.

| Named volume | Container path | Persists |
| --- | --- | --- |
| `warehouse_pgdata` | `/var/lib/postgresql/data` | Both Postgres databases |
| `airflow_logs` | `/opt/airflow/logs` | Airflow task logs |
| `caddy_data` | `/data` | Certificates, private keys, and certificate account state |

Named volumes live in Docker-managed storage, not repository folders. They
survive container recreation and `docker compose down`; `down -v` deletes them.

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
The dashboard cannot directly reach Airflow and has no internet access. By default,
all published ports bind to localhost. Keep `.env` private (`chmod 600 .env`).
Caddy (`dashboard-proxy`) serves localhost:8080 while the dashboard stays on
internal networks. Only Caddy is trusted to forward client IPs.

The API permits 120 requests/minute/IP with a burst of 30, 16 KiB query strings,
and offsets up to 10,000 rows. Narrow filters to browse beyond that limit.
Bronze/Silver/Gold are public schemas in the explorer, including future tables.
Backups contain database credentials and data; keep them private.

Use `docker compose build --no-cache` to refresh OS updates. Airflow dependencies
are pinned with hashes in `docker/airflow.lock`; update that lock from
`docker/airflow.in` with `uv pip compile` when applying Python security updates.

## HTTPS with Caddy

Local access stays HTTP by default. On a server with a public domain, set:

```dotenv
DASHBOARD_ADDRESS=data.example.com
DASHBOARD_BIND_IP=0.0.0.0
DASHBOARD_PORT=80
DASHBOARD_HTTPS_PORT=443
```

Point the domain's DNS to the server and allow inbound TCP 80/443, then run
`docker compose up -d --build --wait dashboard-proxy`. Caddy redirects HTTP to
HTTPS and obtains/renews certificates automatically. Certificates and account
keys persist in the private `caddy_data` volume; do not delete it during updates.
Airflow and Postgres remain localhost-only. No Certbot or renewal cron job is needed.

`Dockerfile.caddy` builds the pinned Caddy release with a patched network library
until the official image includes that security fix.
