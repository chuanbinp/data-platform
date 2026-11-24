with latest as (
    select *
    from {{ ref('generation_by_fuel_history') }}
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
    -- Latest has one version per fuel; MAX extracts that value, not a historical peak.
    {% for code, name in var('fuel_columns').items() %}
    max(generation) filter (where fueltype = '{{ code }}') as {{ name }}_generation,
    max(generation_units) filter (where fueltype = '{{ code }}') as {{ name }}_units,
    {% endfor %}
    max(observed_at) as last_observed_at,
    max(inserted_at) as last_inserted_at
from latest
left join names using (period, respondent)
group by latest.period, latest.respondent, names.respondent_name
