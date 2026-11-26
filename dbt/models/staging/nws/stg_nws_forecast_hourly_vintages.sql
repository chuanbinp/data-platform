{{ config(alias='weather_forecast_hourly_vintages') }}

select
    location,
    location_name,
    station_id,
    latitude,
    longitude,
    period,
    period_end::timestamptz as period_end,
    source_updated_at::timestamptz as source_updated_at,
    wind_direction,
    {{ nws_quantity('temperature', 'temperature') }} as temperature_c,
    temperature_quality_code,
    {{ nws_quantity('dewpoint', 'temperature') }} as dewpoint_c,
    dewpoint_quality_code,
    {{ nws_quantity('relative_humidity', 'percent') }} as relative_humidity_pct,
    relative_humidity_quality_code,
    {{ nws_quantity('wind_speed', 'speed') }} as wind_speed_ms,
    wind_speed_quality_code,
    {{ nws_quantity('wind_gust', 'speed') }} as wind_gust_ms,
    wind_gust_quality_code,
    {{ nws_quantity('precipitation_probability', 'percent') }} as precipitation_probability_pct,
    precipitation_probability_quality_code,
    observed_at,
    inserted_at,
    ingestion_run_id
from {{ source('nws', 'nws_forecast_hourly_vintages') }}
