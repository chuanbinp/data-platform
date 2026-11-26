# Data Platform

A reusable Python ingestion platform with Airflow, dbt, Docker, and Postgres.

**Live dashboard:** [dataplatform.chuanbinphoe.site](https://dataplatform.chuanbinphoe.site/)

## Contents

- [Data sources](#data-sources)
- [Tech stack](#tech-stack)
- [Local setup](#local-setup)
- [Architecture](#architecture)
- [Usage](#usage)
- [CI/CD](#cicd)
- [Development setup and tests](#development-setup-and-tests)
- [Connect to deployed services](#connect-to-deployed-services)

## Data sources

| Source | Data | Airflow DAG | Schedule |
| --- | --- | --- | --- |
| [EIA-930 electricity](docs/data/eia930.md) | Demand, forecasts, generation, and interchange | `eia930_ingestion` | Every 15 minutes |
| [NWS weather](docs/data/weather.md) | Station observations and hourly forecasts | `nws_ingestion` | Every 15 minutes |

## Tech stack

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)
![PostgreSQL 17](https://img.shields.io/badge/PostgreSQL_17-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)
![Tabulator](https://img.shields.io/badge/Tabulator-2563EB?style=flat-square)
![Docker Compose](https://img.shields.io/badge/Docker_Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Cloudflare Tunnel](https://img.shields.io/badge/Cloudflare_Tunnel-F38020?style=flat-square&logo=cloudflare&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?style=flat-square&logo=googlecloud&logoColor=white)

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
flowchart TB
    visitor["PUBLIC ACCESS<br/>Dashboard visitors"]
    edge["Cloudflare edge<br/>HTTPS · dataplatform.chuanbinphoe.site"]
    admin["PRIVATE ACCESS<br/>Administrator · laptop"]
    iap["Google Cloud IAP<br/>Authenticated SSH tunnel"]
    sources["Source APIs<br/>Electricity and weather"]
    backups[("PRIVATE GCS BUCKET<br/>data-platform-pg-backups<br/>Last 7 full backups")]

    subgraph vm["GOOGLE CLOUD VM · Private IP only"]
        subgraph services["DOCKER COMPOSE · Application services"]
            connector["cloudflared<br/>Outbound tunnel connector"]
            dashboard["DATA EXPLORER<br/>FastAPI + Tabulator · :8080"]
            api["AIRFLOW UI / API<br/>Container :8080 · host 127.0.0.1:8000"]
            scheduler["AIRFLOW SCHEDULER<br/>LocalExecutor · Python + dbt tasks"]
            processor["AIRFLOW DAG PROCESSOR<br/>Discover and serialize DAGs"]
        end
        subgraph database["POSTGRESQL 17 · One container · :5432"]
            warehouse[("WAREHOUSE<br/>Bronze → Silver → Gold")]
            metadata[("AIRFLOW METADATA<br/>DAGs · runs · task state")]
        end
        storage["PERSISTENT DOCKER VOLUMES<br/>warehouse_pgdata · airflow_logs"]
        backup["HOST BACKUP JOB · systemd<br/>Daily at 3 a.m. New York"]
    end

    visitor -->|HTTPS · 443| edge
    connector <-->|Encrypted tunnel · initiated from VM| edge
    connector -->|Internal HTTP · 8080| dashboard
    admin --> iap
    iap -->|SSH forwarding · host port 8000| api
    iap -->|SSH forwarding · host port 5000| warehouse
    dashboard -->|Read-only viewer| warehouse
    api --> metadata
    processor -->|DAG definitions| metadata
    scheduler -->|Scheduling state| metadata
    scheduler -->|HTTPS · download| sources
    scheduler -->|Ingest + transform · writer| warehouse
    database --- storage
    database -->|pg_dumpall output via docker exec| backup
    backup -->|HTTPS · compressed SQL| backups
    scheduler -. Task logs .-> storage

    classDef external fill:#f1f5f9,stroke:#94a3b8,color:#0f172a
    classDef access fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
    classDef app fill:#dbeafe,stroke:#3b82f6,color:#1e3a8a
    classDef data fill:#dcfce7,stroke:#16a34a,color:#14532d
    classDef disk fill:#fef3c7,stroke:#d97706,color:#78350f
    class visitor,admin,sources external
    class edge,iap,connector access
    class dashboard,api,scheduler,processor,backup app
    class warehouse,metadata data
    class storage,backups disk
    style vm fill:#0f172a,stroke:#475569,color:#f8fafc
    style services fill:#1e293b,stroke:#64748b,color:#f8fafc
    style database fill:#1e293b,stroke:#16a34a,color:#f8fafc
```

The diagram shows production access. Locally, open the dashboard at `localhost:8080`
and Airflow at `localhost:8000`.

<details>
<summary>Docker details</summary>

No separate Airflow worker is needed. The optional `dbt` container reuses the
Airflow image for manual commands and exits afterward.

### Networks and ports

| Network | Members | Purpose |
| --- | --- | --- |
| `explorer` | Dashboard locally; cloudflared in production | Local port publication or outbound tunnel connection |
| `tunnel` | cloudflared, dashboard (production only) | Internal HTTP; only the connector may supply visitor IPs |
| `warehouse` | Dashboard, Postgres | Internal read-only database connection |
| `pipeline` | Airflow, dbt, database initialization, Postgres | Warehouse writes, metadata, and outbound source API calls |

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

One Postgres container hosts the warehouse and a separate Airflow metadata database.
Postgres starts first. `database-init` prepares accounts and viewer grants;
`airflow-init` prepares Airflow metadata and the UI account. Both initializers exit.
The dashboard, Airflow services, and production tunnel then start.

| Named volume | Container path | Persists |
| --- | --- | --- |
| `warehouse_pgdata` | `/var/lib/postgresql/data` | Warehouse and Airflow databases |
| `airflow_logs` | `/opt/airflow/logs` | Task logs |

Volumes survive container recreation and `docker compose down`; `down -v` deletes
them.

Production supports [daily backups and recovery](docs/backups.md), retaining the
latest seven full backups in a private GCS bucket.

</details>

## Local setup

Requires Docker Compose. Run commands from the project directory.

```sh
cp .env.example .env
```

Set `POSTGRES_PASSWORD`, `WAREHOUSE_PASSWORD`, `AIRFLOW_DB_PASSWORD`, a separate random `VIEWER_PASSWORD`,
`AIRFLOW_ADMIN_PASSWORD`, and a random `AIRFLOW_JWT_SECRET`.
Configure source credentials as described in the [data source guides](#data-sources).

```sh
chmod 600 .env
docker compose up -d --build --wait
```

- **Airflow:** <http://localhost:8000>; use the admin credentials in `.env`.
- **Postgres:** `127.0.0.1:5000`, database `warehouse`; credentials are in `.env`.
- **Data explorer:** <http://localhost:8080>; read-only Bronze, Silver, and Gold tables and views.
- Enable the desired ingestion DAGs listed under [data sources](#data-sources). Progress and errors appear in task logs.

Ports and schemas are configured in `.env`.
Changing `.env` does not update existing database or Airflow account passwords.

After successful ingestion, you can also run dbt manually:

```sh
docker compose run --rm dbt debug
docker compose run --rm dbt build
```

## Architecture

### Reusable platform

| Component | Purpose |
| --- | --- |
| `src/data_platform/downloader.py` | HTTP retries, exponential backoff, and jitter |
| `src/data_platform/ingestor.py` | Atomic Postgres loads and changed-only vintage history |
| `compose.yaml` / `Dockerfile.airflow` | Shared infrastructure and separate Airflow/dbt Python environments |
| `docker/airflow_init.py` / `dbt/profiles.yml` | Airflow initialization and dbt warehouse connection |
| `src/data_platform/dashboard/` / `Dockerfile.dashboard` | Reusable, read-only data explorer and viewer provisioning |

### Pipeline flow

Each dataset follows **Bronze → Silver → Gold**. Source clients use the shared
downloader and ingestor; dbt validates the source and builds typed Silver views,
then downstream Gold history views.

```mermaid
flowchart LR
    api["Source API"] --> download["Downloader"]
    download --> client["Source client"]
    client --> ingest["PostgresIngestor"]
    ingest --> bronze["Bronze · source records"]
    bronze --> silver["Silver · typed views"]
    silver --> gold["Gold · history views"]
```

Each DAG permits one active run, with independent dataset branches. A downstream
task runs only after its upstream task succeeds. Gold does not rebuild Silver.

Unchanged polls add no rows; A → B → A retains all three versions. Silver and Gold
are views, so committed source changes are visible immediately; dbt builds
validate data and update view definitions.

### Adding a data source

All sources use the same platform structure:

| Layer | Responsibility |
| --- | --- |
| `sources/<name>.py` | Declare datasets and tables, prepare source schemas, and fetch/map records |
| `sources.prepare_sources()` | Initialize every registered source before dbt builds |
| `ingestor.py` | Create common revision columns/indexes and append changed records transactionally |
| `orchestration.py` | Create dbt tasks with consistent logging and isolated output directories |
| `dags/<name>_ingestion.py` | Set source parameters and connect Bronze → Silver → Gold |
| `dbt/models/staging/<name>/` | Type and validate source fields; Gold views expose revision history |

Clients expose `iter_records()`, `ingest()`, and `close()`, and support `with`.
Both accept an injected downloader and ingestor. Authentication, pagination,
source fields, and collection windows belong in the source client. Each source
declares `DATASETS` and implements `source_table_sql(schema, dataset)` for table
setup. Register new sources in `sources.prepare_sources()`; database initialization
needs no source-specific imports.

The shared downloader sets the platform User-Agent for all sources. Clients add
API-specific headers, such as NWS's response format and forecast feature flags.

Airflow supplies run IDs containing the DAG, run, and attempt. Unchanged retries
add no rows; a later attempt can preserve newly corrected values without colliding
with records committed by an earlier attempt.

## Usage

Use the read-only explorer to browse Bronze, Silver, and Gold. The
[data source guides](#data-sources) list tables, views, units, and query examples.

Gold history views retain revisions with `observed_at`, `inserted_at`, and
`ingestion_run_id`. For point-in-time queries, `known_from` is inclusive and
`known_until` is exclusive. These intervals use our collection time rather than
the source's publication or measurement time.

For example, retrieve electricity demand versions known at a UTC cutoff:

```sql
SELECT period, respondent, value, value_units
FROM gold.electricity_demand_hourly_history
WHERE known_from <= TIMESTAMPTZ '2026-11-24 00:00:00+00'
  AND (known_until > TIMESTAMPTZ '2026-11-24 00:00:00+00'
       OR known_until IS NULL);
```

`WHERE known_until IS NULL` selects the latest stored revision for each observation;
missing values remain null. The dashboard shows all revisions by default.

## CI/CD

[GitHub Actions](.github/workflows/deploy.yml) automates testing and deployment:

| Trigger | Action |
| --- | --- |
| Pull request targeting `master` | Run tests against disposable Postgres and check deployment script syntax |
| Push or merge to `master` | Run tests, build images, and deploy if checks pass |
| Manual run on `master` | Run the same test and deployment workflow |

```mermaid
flowchart LR
    change["Push / merge to master"] --> tests["Test Python + database<br/>Check shell syntax"]
    tests --> build["Build Airflow/dbt image<br/>Test DAGs · build dashboard image"]
    build --> registry["Artifact Registry<br/>Images pinned by digest"]
    registry --> deploy["Private GCP VM<br/>Transfer release through IAP"]
    deploy --> prepare["Pause ingestion · drain tasks<br/>Stop Airflow · back up databases"]
    prepare --> restart["Initialize · start services<br/>Check health · restore scheduling"]
    classDef default fill:#dbeafe,stroke:#3b82f6,color:#1e3a8a
```

Actions authenticates to GCP through Workload Identity Federation; application
secrets remain on the VM. Deployments run one at a time and preserve database
volumes and each ingestion DAG's paused state.

For Postgres backups, CI/CD also:

- Tests backup and restore in a disposable Postgres database.
- Copies the backup and restore scripts to the VM.
- Installs `jq` if missing and checks bucket access before changing services.
- Installs and enables the daily backup timer after application health checks pass.

Create the bucket and grant the VM access once using the GCP setup script. See the
[backup guide](docs/backups.md) for setup and recovery commands.

Service replacement causes brief downtime. Failed deployments retain backups and
previous release files; rollback is manual. View progress and errors in the
repository's **Actions** tab. See the [deployment guide](docs/deployment.md) for
recovery commands.

## Development setup and tests

Install [uv](https://docs.astral.sh/uv/) and sync the local environment:

```sh
uv sync --locked --extra dev --extra dashboard --group dbt --python 3.12
uv run --no-sync pytest -m 'not integration'
```

Rebuild containers after changing dependencies or the Dockerfile. Generated dbt artifacts stay under `/tmp/dbt`
in containers; local `dbt/target/` and `dbt/logs/` are Git-ignored and safe to delete.

<details>
<summary>Database and dbt integration tests</summary>

Use a disposable Postgres instance. The
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

</details>

<details>
<summary>Airflow DAG tests</summary>

Run inside the built image.

```sh
docker compose run --rm --no-deps \
  --volume ./tests:/opt/data-platform/tests:ro \
  --volume ./dags:/opt/data-platform/dags:ro \
  airflow-scheduler python -m unittest discover \
  -s /opt/data-platform/tests -p test_airflow_dag.py
```

</details>

## Connect to deployed services

Connect to deployed Airflow and Postgres through IAP SSH. Run on your laptop
with [gcloud CLI](https://cloud.google.com/sdk/docs/install):

```sh
gcloud auth login
gcloud compute ssh data-platform \
  --project=hpml-cp3541 \
  --zone=us-east1-c \
  --tunnel-through-iap \
  -- -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
  -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:8888:127.0.0.1:8000 \
  -L 127.0.0.1:5433:127.0.0.1:5000
```

| Service | Local connection | Credentials from the VM's `.env` |
| --- | --- | --- |
| Airflow | <http://localhost:8888> | `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` |
| Postgres | `127.0.0.1:5433`, database `warehouse` | `VIEWER_USER` / `VIEWER_PASSWORD` (read-only) |

Keep the SSH session open. Type `exit` to release the ports; rerun the SSH command
if disconnected. With `psql` installed, connect from another laptop terminal:

```sh
psql -h 127.0.0.1 -p 5433 -U warehouse_viewer -d warehouse -W
```

Replace `warehouse_viewer` if you changed `VIEWER_USER`.
