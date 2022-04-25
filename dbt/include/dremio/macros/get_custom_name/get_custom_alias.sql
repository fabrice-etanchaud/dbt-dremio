{% macro dremio__generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- set custom_alias_name = custom_alias_name if not is_datalake_node(node)
    else node.config.file -%}
  {{ generate_alias_name_impl(node.name, custom_alias_name, node) }}
{%- endmacro %}

{% macro generate_alias_name_impl(default_alias, custom_alias_name=none, node=none) -%}
  {%- if custom_alias_name is none -%}

      {{ default_alias }}

  {%- else -%}

      {{ custom_alias_name | trim }}

  {%- endif -%}
{%- endmacro %}
