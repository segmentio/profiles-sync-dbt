{# Query for latest build time of id_graph #}
{%- set get_max_ts %} SELECT CAST(MAX(etl_ts) AS {{datetime_univ()}}) FROM {{ ref('id_graph') }} {% endset -%}
{% set results = run_query(get_max_ts) %}
{%- if execute %}
    {% set ts = results.columns[0].values()[0] %}
{% else %}
    {% set ts = [] %}
{% endif -%}

select {{ts}} as timereturn