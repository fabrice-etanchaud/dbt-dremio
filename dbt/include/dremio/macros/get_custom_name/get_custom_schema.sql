{% macro dremio__generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema if not is_datalake_node(node)
    else target.root_path -%}
  {%- set custom_schema_name = custom_schema_name if not is_datalake_node(node)
    else node.config.root_path -%}
  {{ generate_schema_name_impl(default_schema, custom_schema_name, node) }}
{%- endmacro %}

{% macro generate_schema_name_impl(default_schema, custom_schema_name=none, node=none) -%}
  {%- if custom_schema_name is none -%}

      {{ default_schema }}

  {%- else -%}

      {{ custom_schema_name }}

  {%- endif -%}
{%- endmacro %}
