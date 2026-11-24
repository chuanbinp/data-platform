"""Read-only browser for warehouse tables and views."""

import os


def schemas():
    """Return configured warehouse schemas in Bronze, Silver, Gold order."""
    return tuple(
        dict.fromkeys(
            os.getenv(f"DBT_{layer}_SCHEMA", layer.lower())
            for layer in ("BRONZE", "SILVER", "GOLD")
        )
    )
