{% set fields = {
    'temperature': 'temperature',
    'dewpoint': 'temperature',
    'relative_humidity': 'percent',
    'wind_speed': 'speed',
    'wind_gust': 'speed',
    'wind_direction': 'angle',
    'precipitation_last_hour': 'rain'
} %}

{% for field, kind in fields.items() %}
select location, station_id, period, '{{ field }}' as field
from {{ source('nws', 'nws_observation_vintages') }}
where nullif(trim({{ field }}_value), '') is not null
  and (not pg_input_is_valid({{ field }}_value, 'numeric')
       or {{ field }}_units is null
       or {{ field }}_units not in ({{ nws_units(kind) }}))
{% if not loop.last %}union all{% endif %}
{% endfor %}
