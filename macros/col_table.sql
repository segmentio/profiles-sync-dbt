{% macro col_table(schema) -%}
    {{ return(adapter.dispatch('col_table')(schema)) }}
{%- endmacro %}


{% macro default__col_table(schema) -%}
    information_schema.columns
{%- endmacro %}


{% macro bigquery__col_table(fields) %}
    {{schema}}.INFORMATION_SCHEMA.COLUMNS
{% endmacro %}