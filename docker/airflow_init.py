"""Initialize Airflow metadata and its UI account with the runtime credentials."""

import logging
import os

from data_platform.database import require_secret

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)
require_secret("AIRFLOW__API_AUTH__JWT_SECRET")
require_secret("_AIRFLOW_WWW_USER_PASSWORD")

# The official entrypoint initializes Airflow tables and creates the UI account.
logger.info("Initializing Airflow tables and UI account")
os.execv("/entrypoint", ["/entrypoint", "airflow", "version"])
