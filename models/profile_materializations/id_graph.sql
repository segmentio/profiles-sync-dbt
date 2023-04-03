/* ID_GRAPH

    Materialization of Segment inputs, pt. 1: full list of associations between segment_id and canonical_segment_id
    
    - default config: incremental build (add any newly-landed profiles/updated associations from id_graph_updates).

    - adds "etl_ts", a column to indicate most recently-materialized rows (which we need need to efficiently filter
      when joining this result to other models)

    - performs one layer of checking upstream to ensure we don't accidentially include profiles that are now 
      actually merged away (can happen rarely, if merges happen in rapid succession but land in the DWH 
      slightly out of order). Updates that resolve to a known-merged-away ID are ignored during incremental materialization.
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
            ORDER BY CASE WHEN updates.seq IS NULL THEN '0' ELSE updates.seq END DESC) AS rn
    FROM {{ var("schema_name") }}.id_graph_updates as updates
    {% if is_incremental() -%}
    LEFT JOIN {{ this }}
        ON {{ this }}.segment_id = updates.canonical_segment_id
        AND {{ this }}.canonical_segment_id <> {{ this }}.segment_id
    WHERE CAST(updates.uuid_ts AS {{datetime_univ()}}) > CAST((SELECT MAX(uuid_ts) FROM {{ this }}) as {{datetime_univ()}})
        AND {{ this }}.canonical_segment_id IS NULL
    {%- endif %}
    ) AS all_updates
WHERE rn = 1
