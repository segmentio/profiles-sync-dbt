
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

merges AS (
    SELECT -- net_new profiles - may be identified, maybe not
        canonical_segment_id,
        NULL as merged_to
    FROM id_graph WHERE segment_id = canonical_segment_id
    UNION ALL
    SELECT -- merged-out profiles
        segment_id,
        canonical_segment_id as merged_to
    FROM id_graph WHERE segment_id <> canonical_segment_id

),

updates AS (
    SELECT DISTINCT
        COALESCE(id_graph.canonical_segment_id,events.segment_id) as canonical_segment_id,
        {% for col in column_names %}
        {{ last_observed_trait(col) }}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ var("schema_name") }}.identifies AS events
    LEFT JOIN id_graph
        ON id_graph.segment_id = events.segment_id
    WHERE events.timestamp > (SELECT MAX(timestamp) FROM {{ this }})
)

SELECT 
    COALESCE(merges.canonical_segment_id, updates.canonical_segment_id) as canonical_segment_id,
    {% for col in column_names %}
          {%- if col in existing_cols %}
             COALESCE(updates.{{col}}, orig.{{ col }}) AS {{ col }},
          {%- else %}
             updates.{{col}} AS {{ col }},
          {% endif %}
        {% endfor %}
    merges.merged_to
FROM merges
FULL OUTER JOIN updates
    ON merges.canonical_segment_id = updates.canonical_segment_id
LEFT JOIN {{ this }} as orig
    ON orig.canonical_segment_id =  COALESCE(merges.canonical_segment_id,updates.canonical_segment_id)


-- IIb. Build profiles (non-incremental)  - - - - - - - - - - - - - -
-- Outer join with newly-observed profiles (to ensure every profile is captured, even if it has never had an identify msg)
-- Cycle columns, take last-seen value for trait across entire identifies list
-- We are overwriting the table and destroying any now-merged profiles so "merged_to" will be universally NULL
-- - - - - - - - - - - - - - - - - - - - -
{% else %}
SELECT DISTINCT
    COALESCE(id_graph.canonical_segment_id,events.segment_id) as canonical_segment_id,
    {% for col in column_names %}
    {{ last_observed_trait(col) }},
    {% endfor %}
    '' as merged_to
FROM {{ var("schema_name") }}.identifies AS events
FULL OUTER JOIN {{ ref('id_graph') }} AS id_graph
    ON id_graph.segment_id = events.segment_id
{% endif %}
