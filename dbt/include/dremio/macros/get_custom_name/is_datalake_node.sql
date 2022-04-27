{% macro is_datalake_node(node) -%}
  {{ return(node.resource_type in ['test', 'seed']
    or (node.resource_type == 'model' and node.config.materialized not in ['view', 'reflection'])) }}
{%- endmacro %}
