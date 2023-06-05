/* PROFILE TRAITS

Materialization of Segment inputs, pt. 3: Full list of traits (i.e. what's passed in as identify calls) for each profile

Every profile will be represented in this table, even if that profile has not had an identify message.

Table is schema-adaptive - as new traits are added to the "identifies" table, they will also be appended to profile_traits.
Redshift, BigQuery, and Snowflake all maintain a table called INFORMATION_SCHEMA.COLUMNS that we leverage.


Traits will be sequenced in timestamp order (the most recent is prioritized) - future traits are not omitted.
*/

{{ config(unique_key='canonical_segment_id') }}

{# Define and execute query for col names (traits) in identifies table #}
{%- set get_trait_columns %}
    SELECT 
        column_name
    FROM {{ col_table(var("schema_name")) }} 
    WHERE LOWER(table_schema) = LOWER('{{ var("schema_name") }}')
        AND LOWER(table_name) = 'identifies'
        --AND LOWER(table_name) = 'profile_traits_updates'
        AND NOT LOWER(column_name) IN ('user_id','anonymous_id','id','canonical_segment_id','merged_to','sent_at','received_at')
        AND NOT LOWER(column_name) LIKE  'context_%'
        AND LEFT(column_name,1) <> '_'
    ORDER BY 1
{% endset -%}
{% set results = run_query(get_trait_columns) %}
{%- if execute %}
    {% set column_names = results.columns[0].values() %}
{% else %}
    {% set column_names = [] %}
    {% set ts = [] %}
{% endif -%}


    
-- - - IIa. Build profiles (incremental) - - - - - - - - 
-- Outer join between:
-- 1) List of merged-away + newly-observed profiles
--     (Add tombstone for merged-away profiles - the segment_id that it merged into)
-- 2) List of profile updates (if an identify came in on a profile that was later merged, that is also accounted for)
-- 
-- Left join back to original table to ensure we don't overwrite any traits with NULL
-- - - - - - - - - - - - - - - - - - - - - - - - - - -


{% if is_incremental() %}

{# Define and execute query for (a) col names (traits) in existing profile_traits table and (b) latest landing time of an event #}
{%- set existing_cols_orig = adapter.get_columns_in_relation(this) -%}
{% set existing_cols = [] %}
{%- for col in existing_cols_orig  %}
{{- existing_cols.append( col.name ) or "" -}} 
{% endfor -%}

{%- set get_max_ts %} SELECT CAST(MAX(uuid_ts) AS {{datetime_univ()}}) FROM {{ this }} {% endset -%}
{% set results2 = run_query(get_max_ts) %}
{%- if execute %}
    {% set ts = results2.columns[0].values()[0] %}
{% else %}
    {% set ts = [] %}
{% endif -%}



WITH id_graph AS (
    SELECT * FROM {{ ref('id_graph') }} 
    WHERE CAST(etl_ts as {{datetime_univ()}}) >= {{ dateadd2('hour', -var('etl_overlap'), '\'' ~ ts ~ '\'') }}
),


updates as (
    SELECT 
        COALESCE(id_graph.canonical_segment_id,updates.segment_id) as canonical_segment_id,
        {% for col in column_names %}
            {{ last_observed_profile_trait(col) }},
            {% endfor %}
        row_number() OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,updates.segment_id)
            ORDER BY CASE WHEN updates.seq IS NULL THEN '0' ELSE updates.seq END DESC) AS rn
    FROM {{ var("schema_name") }}.identifies AS updates
    --FROM {{ var("schema_name") }}.profile_traits_updates AS updates
    LEFT JOIN id_graph
        ON id_graph.segment_id = updates.segment_id
    WHERE CAST(updates.uuid_ts as {{datetime_univ()}})  > (SELECT MAX(timestamp) FROM {{ this }})
)


SELECT 
    updates.canonical_segment_id as canonical_segment_id,
    {% for col in column_names %}
          {%- if col in existing_cols %}
             COALESCE(updates.{{col}}, orig.{{ col }}) AS {{ col }},
          {%- else %}
             updates.{{col}} AS {{ col }},
          {% endif %}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
FROM updates
where rn = 1

{% else %}

WITH last_update as (
    SELECT 
        COALESCE(id_graph.canonical_segment_id,updates.segment_id) as canonical_segment_id,
        {% for col in column_names %}
            {{ last_observed_profile_trait(col) }},
            {% endfor %}
        {{ current_timestamp() }} AS etl_ts
        row_number() OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,updates.segment_id)
            ORDER BY CASE WHEN updates.seq IS NULL THEN '0' ELSE updates.seq END DESC) AS rn
    FROM {{ var("schema_name") }}.identifies AS updates
    --FROM {{ var("schema_name") }}.profile_traits_updates AS updates
    FULL OUTER JOIN {{ ref('id_graph') }} AS id_graph
        ON id_graph.segment_id = updates.segment_id
)

select *
from last_update
where rn = 1

{% endif %}