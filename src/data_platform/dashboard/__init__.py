"""Read-only browser for warehouse tables and views."""

import os

SOURCES = (
    {
        "id": "eia930",
        "label": "Electricity (EIA930)",
        "prefixes": ("eia930_", "electricity_", "generation_by_fuel_"),
    },
    {"id": "nws", "label": "Weather (NWS)", "prefixes": ("nws_", "weather_")},
    {"id": "other", "label": "Other", "prefixes": ()},
)


def relation_source(name):
    """Classify discovered relations without hiding unfamiliar datasets."""
    for source in SOURCES:
        if name.startswith(source["prefixes"]):
            return source["id"]
    return "other"


def schemas():
    """Return configured warehouse schemas in Bronze, Silver, Gold order."""
    return tuple(
        dict.fromkeys(
            os.getenv(f"DBT_{layer}_SCHEMA", layer.lower())
            for layer in ("BRONZE", "SILVER", "GOLD")
        )
    )
