    SELECT 
        column_name
    FROM {{ col_table(var("schema_name")) }} 
    WHERE LOWER(table_schema) = LOWER('{{ var("schema_name") }}')
        AND LOWER(table_name) = 'identifies'
        AND NOT LOWER(column_name) IN ('user_id','anonymous_id','id','canonical_segment_id','merged_to','sent_at','received_at')
        AND NOT LOWER(column_name) LIKE  'context_%'
        AND LEFT(column_name,1) <> '_'
    ORDER BY 1