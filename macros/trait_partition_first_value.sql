{% macro trait_partition_first_value(col) %}
  {{ return(adapter.dispatch('trait_partition_first_value')(col)) }}
{% endmacro %}

{% macro default__trait_partition_first_value(col) %}
    FIRST_VALUE({{ col }}) 
        OVER(PARTITION BY canonical_segment_id, {{ col ~ '_partition'}} ORDER BY seq) AS {{ col }} 
{% endmacro %}

{% macro postgres__trait_partition_first_value(col) %}
    FIRST_VALUE({{ col }}) 
    --find_last_ignore_nulls(updates.{{ col }}) 
        OVER(PARTITION BY canonical_segment_id, {{ col ~ '_partition'}} ORDER BY seq) AS {{ col }} 
{% endmacro %}

{% macro redshift__trait_partition_first_value(col) %}
    FIRST_VALUE({{ col }}) 
        OVER(PARTITION BY canonical_segment_id, {{ col ~ '_partition'}} ORDER BY seq) AS {{ col }}  
{% endmacro %}


{% macro synapse__trait_partition_first_value(col) %}
    FIRST_VALUE({{ col }})  
        OVER(PARTITION BY canonical_segment_id, {{ col ~ '_partition'}} ORDER BY seq) AS {{ col }}  
{% endmacro %}