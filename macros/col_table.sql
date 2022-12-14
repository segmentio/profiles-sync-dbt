{% macro col_table(schema_1) -%}
    {{ return(adapter.dispatch('col_table')(schema_1)) }}
{%- endmacro %}


{% macro default__col_table(schema_1) -%}
    information_schema.columns
{%- endmacro %}


{% macro bigquery__col_table(schema_1) %}
    {{ schema_1 }}.INFORMATION_SCHEMA.COLUMNS
{% endmacro %}