
/* EXTERNAL_ID_MAPPING

Materialization of Segment inputs, pt. 2: External ID <> canonical ID lookup.

A profile is defined by having one or more external IDs, so every profile will be represented at least once.

- default config: incremental build (pull in recently-associated external IDs, or 
   associations that changed by virtue of segment ID being remapped).
*/

{{ config(unique_key='external_id_hash') }}

{# Query for latest build time of id_graph #}
{%- set get_max_ts %} SELECT CAST(MAX(etl_ts) AS datetime) FROM {{ ref('id_graph') }} {% endset -%}
{% set results = run_query(get_max_ts) %}
{%- if execute %}
    {% set ts = results.columns[0].values()[0] %}
{% else %}
    {% set ts = [] %}
{% endif -%}

SELECT 
    all_maps.canonical_segment_id, 
    all_maps.external_id_value, 
    all_maps.external_id_type, 
    all_maps.external_id_hash, 
    all_maps.timestamp, 
    all_maps.uuid_ts
FROM (
    SELECT
            COALESCE(id_graph.canonical_segment_id,ids.segment_id) as canonical_segment_id,
            ids.external_id_type,
            ids.external_id_value,
            ids.external_id_hash,
            ids.timestamp,
            ids.uuid_ts,
            ROW_NUMBER() OVER (PARTITION BY ids.external_id_hash ORDER BY ids.timestamp DESC) AS rn
    FROM {{ var("schema_name") }}.external_id_mapping_updates as ids
    LEFT JOIN {{ ref('id_graph') }} AS id_graph
        ON id_graph.segment_id = ids.segment_id
    {% if is_incremental() -%}
        AND CAST(id_graph.etl_ts AS datetime) >= {{ dateadd('hour', -var('etl_overlap'), '\'' ~ ts ~ '\'')}}
    WHERE ids.uuid_ts > (SELECT MAX(uuid_ts) FROM {{ this }})
        OR id_graph.canonical_segment_id IS NOT NULL
    {%- endif %}
) AS all_maps
WHERE rn = 1



