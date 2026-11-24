select
    period,
    respondent,
    respondent_name,
    fueltype,
    type_name as fuel_name,
    value as generation,
    value_units as generation_units,
    observed_at,
    inserted_at,
    ingestion_run_id,
    observed_at as known_from,
    lead(observed_at) over (
        partition by period, respondent, fueltype
        order by observed_at, inserted_at, ingestion_run_id
    ) as known_until
from {{ ref('stg_eia930_fuel_type_data') }}
