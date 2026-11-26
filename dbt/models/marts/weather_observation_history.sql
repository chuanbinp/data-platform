select
    location,
    location_name,
    station_id,
    latitude,
    longitude,
    period,
    temperature_c,
    temperature_quality_code,
    dewpoint_c,
    dewpoint_quality_code,
    relative_humidity_pct,
    relative_humidity_quality_code,
    wind_speed_ms,
    wind_speed_quality_code,
    wind_gust_ms,
    wind_gust_quality_code,
    wind_direction_degrees,
    wind_direction_quality_code,
    precipitation_last_hour_mm,
    precipitation_last_hour_quality_code,
    observed_at,
    inserted_at,
    ingestion_run_id,
    observed_at as known_from,
    lead(observed_at) over (
        partition by location, station_id, period
        order by observed_at, inserted_at, ingestion_run_id
    ) as known_until
from {{ ref('stg_nws_observation_vintages') }}
