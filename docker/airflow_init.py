"""Create Airflow's database in the shared Postgres server, then initialize it."""

import logging
import os

import psycopg

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

logger.info("Checking Airflow metadata database")
with psycopg.connect(host="postgres", dbname="postgres", autocommit=True) as connection:
    if not connection.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'").fetchone():
        connection.execute("CREATE DATABASE airflow")
        logger.info("Created Airflow metadata database")

# The official entrypoint initializes Airflow tables and creates the UI account.
logger.info("Initializing Airflow tables and UI account")
os.execv("/entrypoint", ["/entrypoint", "airflow", "version"])
