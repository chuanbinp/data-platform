select
    period,
    respondent,
    respondent_name,
    value,
    value_units,
    observed_at,
    inserted_at,
    ingestion_run_id,
    observed_at as known_from,
    lead(observed_at) over (
        partition by period, respondent
        order by observed_at, inserted_at, ingestion_run_id
    ) as known_until
from {{ ref('stg_eia930_region_hourly_vintages') }}
where type = 'TI'
