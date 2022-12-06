/* ID_GRAPH

    Materialization of Segment inputs, pt. 1: full list of associations between segment_id and canonical_segment_id
    
    - default config: incremental build (add any newly-landed profiles/updated associations from id_graph_updates).

    - adds "etl_ts", a column to indicate most recently-materialized rows (which we need need to efficiently filter
      when joining to other tables)

    - performs one layer of checking upstream to ensure we don't accidentially include profiles that are now 
      actually merged away (can happen rarely, if merges happen in rapid succession but land in the DWH 
      slightly out of order) - will only look at last 2 hrs
*/

{{ config(unique_key='segment_id') }}

SELECT 
    all_updates.segment_id, 
    all_updates.canonical_segment_id, 
    all_updates.uuid_ts, 
    all_updates.etl_ts, 
    all_updates.timestamp,
    all_updates.seq
FROM (
    SELECT 
        updates.segment_id,
        updates.canonical_segment_id,
        updates.uuid_ts,
        updates.timestamp,
        updates.seq,
        {{ current_timestamp() }} AS etl_ts,
        row_number() OVER(PARTITION BY updates.segment_id 
            ORDER BY updates2.canonical_segment_id IS NOT NULL, updates.seq DESC NULLS FIRST) AS rn
    FROM {{ var("schema_name") }}.id_graph_updates as updates
    LEFT JOIN {{ var("schema_name") }}.id_graph_updates as updates2
        ON updates2.segment_id = updates.canonical_segment_id
        AND CAST(updates.uuid_ts AS datetime) < {{ dbt.dateadd('hour', 2, 'updates2.uuid_ts') }}
        AND updates2.canonical_segment_id <> updates2.segment_id
        AND updates2.canonical_segment_id <> updates.canonical_segment_id 
    {% if is_incremental() -%}
    WHERE CAST(updates.uuid_ts AS datetime) > CAST((SELECT MAX(uuid_ts) FROM {{ this }}) as datetime)
    {%- endif %}
    ) AS all_updates
WHERE rn = 1
