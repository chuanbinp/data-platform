select period, respondent, type, ingestion_run_id
from {{ source('eia930', 'eia930_region_hourly_vintages') }}
group by period, respondent, type, ingestion_run_id
having count(*) > 1
