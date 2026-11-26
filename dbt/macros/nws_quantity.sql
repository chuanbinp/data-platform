{% macro nws_units(kind) -%}
    {% set units = {
        'temperature': ['wmoUnit:degC', 'wmoUnit:degF'],
        'speed': ['wmoUnit:m_s-1', 'wmoUnit:km_h-1', 'wmoUnit:mi_h-1', 'wmoUnit:kn'],
        'percent': ['wmoUnit:percent'],
        'angle': ['wmoUnit:degree_(angle)'],
        'rain': ['wmoUnit:mm', 'wmoUnit:m', 'wmoUnit:in']
    } %}
    '{{ units[kind] | join("', '") }}'
{%- endmacro %}

{% macro nws_quantity(column, kind) -%}
    {% set value = "nullif(trim(" ~ column ~ "_value), '')::numeric" %}
    case {{ column }}_units
    {% if kind == 'temperature' %}
        when 'wmoUnit:degC' then {{ value }}
        when 'wmoUnit:degF' then ({{ value }} - 32) * 5 / 9
    {% elif kind == 'speed' %}
        when 'wmoUnit:m_s-1' then {{ value }}
        when 'wmoUnit:km_h-1' then {{ value }} / 3.6
        when 'wmoUnit:mi_h-1' then {{ value }} * 0.44704
        when 'wmoUnit:kn' then {{ value }} * 1852 / 3600
    {% elif kind == 'rain' %}
        when 'wmoUnit:mm' then {{ value }}
        when 'wmoUnit:m' then {{ value }} * 1000
        when 'wmoUnit:in' then {{ value }} * 25.4
    {% else %}
        when {{ nws_units(kind) }} then {{ value }}
    {% endif %}
    end
{%- endmacro %}
