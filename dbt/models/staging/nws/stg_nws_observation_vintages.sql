{{ config(alias='weather_observation_vintages') }}

select
    location,
    location_name,
    station_id,
    latitude,
    longitude,
    period,
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
    {{ nws_quantity('wind_direction', 'angle') }} as wind_direction_degrees,
    wind_direction_quality_code,
    {{ nws_quantity('precipitation_last_hour', 'rain') }} as precipitation_last_hour_mm,
    precipitation_last_hour_quality_code,
    observed_at,
    inserted_at,
    ingestion_run_id
from {{ source('nws', 'nws_observation_vintages') }}
