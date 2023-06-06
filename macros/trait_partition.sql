{% macro trait_partition(col) %}
  {{ return(adapter.dispatch('trait_partition')(col)) }}
{% endmacro %}

{% macro default__trait_partition(col) %}
    COUNT(updates.{{ col }}) 
        OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,updates.segment_id) ORDER BY updates.seq) AS {{ col ~ '_partition'}} 
{% endmacro %}

{% macro postgres__trait_partition(col) %}
    COUNT(updates.{{ col }}) 
        OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,updates.segment_id) ORDER BY updates.seq) AS {{ col ~ '_partition'}} 
{% endmacro %}


{% macro redshift__trait_partition(col) %}
    COUNT(updates.{{ col }}) 
        OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,updates.segment_id) ORDER BY updates.seq) AS {{ col ~ '_partition'}} 
{% endmacro %}


{% macro synapse__trait_partition(col) %}
    COUNT(updates.{{ col }}) 
        OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,updates.segment_id) ORDER BY updates.seq) AS {{ col ~ '_partition'}} 
{% endmacro %}