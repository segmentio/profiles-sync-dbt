{# /* postgres (and older versions of Synapse) lack "LAST VALUE" functionality - need a workaround*/ #}


{% macro last_observed_trait(col) %}
  {{ return(adapter.dispatch('last_observed_trait')(col)) }}
{% endmacro %}


{% macro default__last_observed_trait(col) %}
    LAST_VALUE(events.{{ col }} IGNORE NULLS) 
        OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,events.segment_id) ORDER BY events.timestamp 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{ col }}
{% endmacro %}

{% macro redshift__last_observed_trait(col) %}
    LAST_VALUE(events.{{ col }} IGNORE NULLS) 
        OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,events.segment_id) ORDER BY events.timestamp 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{ col }}
{% endmacro %}


{% macro synapse__last_observed_trait(col) %}
    LAST_VALUE(events.{{ col }} ) IGNORE NULLS
        OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,events.segment_id) ORDER BY events.timestamp ) AS {{ col }}
{% endmacro %}

{% macro postgres__last_observed_trait(col) %}
    find_last_ignore_nulls(events.{{ col }}) 
        OVER(PARTITION BY COALESCE(id_graph.canonical_segment_id,events.segment_id) ORDER BY events.timestamp 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{ col }}
{% endmacro %}