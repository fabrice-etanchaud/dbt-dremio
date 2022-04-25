{% macro dremio__generate_database_name(custom_database_name=none, node=none) -%}
  {%- set default_database = target.database if not is_datalake_node(node)
    else target.datalake -%}
  {%- set custom_database_name = custom_database_name if not is_datalake_node(node)
    else node.config.datalake -%}
  {{ generate_database_name_impl(default_database, custom_database_name, node) }}
{%- endmacro %}

{% macro generate_database_name_impl(default_database, custom_database_name=none, node=none) -%}
  {%- if custom_database_name is none -%}

      {{ default_database }}

  {%- else -%}

      {{ custom_database_name }}

  {%- endif -%}
{%- endmacro %}
