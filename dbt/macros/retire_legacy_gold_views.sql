{% macro retire_legacy_gold_views() %}
    {% set gold = env_var('DBT_GOLD_SCHEMA', 'gold') %}
    {% for name in [
        'electricity_demand_hourly_history',
        'electricity_demand_forecast_hourly_history',
        'electricity_net_generation_hourly_history',
        'electricity_total_interchange_hourly_history',
        'generation_by_fuel_hourly_history'
    ] %}
        {% set relation = adapter.get_relation(database=target.database, schema=gold, identifier=name) %}
        {% if relation is none or relation.type != 'view' %}
            {{ exceptions.raise_compiler_error('Build and test the replacement views first: missing view ' ~ gold ~ '.' ~ name) }}
        {% endif %}
    {% endfor %}

    {# One transaction: a dependent object must leave remaining legacy views intact. #}
    {% call statement('retire_legacy_gold_views', auto_begin=True) %}
        drop view if exists {{ adapter.quote(gold) }}.electricity_metrics_hourly_latest;
        drop view if exists {{ adapter.quote(gold) }}.generation_by_fuel_hourly_latest;
        drop view if exists {{ adapter.quote(gold) }}.electricity_metrics_hourly_history;
    {% endcall %}
    {% do adapter.commit() %}
    {{ log('Legacy Gold views retired; source data retained.', info=True) }}
{% endmacro %}
