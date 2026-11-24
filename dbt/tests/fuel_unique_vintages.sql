select period, respondent, fueltype, ingestion_run_id
from {{ source('eia930', 'eia930_fuel_type_hourly_vintages') }}
group by period, respondent, fueltype, ingestion_run_id
having count(*) > 1
