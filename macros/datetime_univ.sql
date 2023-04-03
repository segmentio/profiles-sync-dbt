{# /*Azure/Synapse prefers to use "datetime2" instead of "datetime" data types (more flexible casting), so add some warehouse-specific handling*/ #}

{% macro datetime_univ() -%}
    {{ return(adapter.dispatch('datetime_univ')()) }}
{%- endmacro %}


{% macro default__datetime_univ() -%}
    datetime
{%- endmacro %}


{% macro postgres__datetime_univ() -%}
    timestamp
{%- endmacro %}

{% macro synapse__datetime_univ() %}
    datetime2
{%- endmacro %}