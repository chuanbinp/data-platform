{{ config(alias='electricity_fuel_hourly_vintages') }}

select
    (period || ':00:00+00')::timestamptz as period,
    respondent,
    respondent_name,
    fueltype,
    type_name,
    nullif(trim(value), '')::numeric as value,
    value_units,
    observed_at,
    inserted_at,
    ingestion_run_id
from {{ source('eia930', 'eia930_fuel_type_data') }}
