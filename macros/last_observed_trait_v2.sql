{# /* postgres (and older versions of Synapse) lack "LAST VALUE" functionality - need a workaround*/ #}


{% macro last_observed_profile_trait(col) %}
  {{ return(adapter.dispatch('last_observed_profile_trait')(col)) }}
{% endmacro %}


{% macro default__last_observed_profile_trait(col) %}
    LAST_VALUE(updates.{{ col }} IGNORE NULLS) 
        OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,updates.segment_id) ORDER BY updates.seq 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{ col }}
{% endmacro %}

{% macro redshift__last_observed_profile_trait(col) %}
    LAST_VALUE(updates.{{ col }} IGNORE NULLS) 
        OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,updates.segment_id) ORDER BY updates.seq 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{ col }}
{% endmacro %}


{% macro synapse__last_observed_profile_trait(col) %}
    LAST_VALUE(updates.{{ col }} ) IGNORE NULLS
        OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,updates.segment_id) ORDER BY updates.seq ) AS {{ col }}
{% endmacro %}

{% macro postgres__last_observed_profile_trait(col) %}
    LAST_VALUE(updates.{{ col }}) 
    --find_last_ignore_nulls(updates.{{ col }}) 
        OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,updates.segment_id) ORDER BY updates.seq 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{ col }}
{% endmacro %}