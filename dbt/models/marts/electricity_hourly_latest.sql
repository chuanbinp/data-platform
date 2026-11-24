with latest as (
    select *
    from {{ ref('electricity_metrics_history') }}
    where known_until is null
),

names as (
    select distinct on (period, respondent)
        period, respondent, respondent_name
    from latest
    where respondent_name is not null
    order by period, respondent, observed_at desc, inserted_at desc, ingestion_run_id desc
)

select
    latest.period,
    latest.respondent,
    names.respondent_name,
    max(value) filter (where metric = 'D') as demand,
    max(value_units) filter (where metric = 'D') as demand_units,
    max(value) filter (where metric = 'DF') as demand_forecast,
    max(value_units) filter (where metric = 'DF') as demand_forecast_units,
    max(value) filter (where metric = 'NG') as net_generation,
    max(value_units) filter (where metric = 'NG') as net_generation_units,
    max(value) filter (where metric = 'TI') as total_interchange,
    max(value_units) filter (where metric = 'TI') as total_interchange_units,
    max(observed_at) as last_observed_at,
    max(inserted_at) as last_inserted_at
from latest
left join names using (period, respondent)
group by latest.period, latest.respondent, names.respondent_name
