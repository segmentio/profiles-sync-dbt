{# /* dbt's dateadd macro is MOSTLY ok, but we run into problems with synapse. Add universal handling */ #}


{% macro dateadd2(datepart, interval, from_date_or_timestamp) %}
  {{ return(adapter.dispatch('dateadd2')(datepart, interval, from_date_or_timestamp)) }}
{% endmacro %}


{% macro default__dateadd2(datepart, interval, from_date_or_timestamp) %}
    {{ dbt.dateadd(datepart, interval, from_date_or_timestamp) }}
{% endmacro %}


{% macro synapse__dateadd2(datepart, interval, from_date_or_timestamp) %}
        dateadd(
        {{ datepart }},
        {{ interval }},
        CAST({{ from_date_or_timestamp }} as datetime2)
        )
{% endmacro %}