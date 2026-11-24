select period, respondent, fueltype, ingestion_run_id
from {{ source('eia930', 'eia930_fuel_type_hourly_vintages') }}
where not pg_input_is_valid(period || ':00:00+00', 'timestamptz')
   or not pg_input_is_valid(nullif(trim(value), ''), 'numeric')
