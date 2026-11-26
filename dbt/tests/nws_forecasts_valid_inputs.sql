{% set fields = {
    'temperature': 'temperature',
    'dewpoint': 'temperature',
    'relative_humidity': 'percent',
    'wind_speed': 'speed',
    'wind_gust': 'speed',
    'precipitation_probability': 'percent'
} %}

{% for field, kind in fields.items() %}
select location, station_id, period, '{{ field }}' as field
from {{ source('nws', 'nws_forecast_hourly_vintages') }}
where nullif(trim({{ field }}_value), '') is not null
  and (not pg_input_is_valid({{ field }}_value, 'numeric')
       or {{ field }}_units is null
       or {{ field }}_units not in ({{ nws_units(kind) }}))
{% if not loop.last %}union all{% endif %}
{% endfor %}
union all
select location, station_id, period, 'forecast_timestamps' as field
from {{ source('nws', 'nws_forecast_hourly_vintages') }}
where not pg_input_is_valid(source_updated_at, 'timestamptz')
   or not pg_input_is_valid(period_end, 'timestamptz')
