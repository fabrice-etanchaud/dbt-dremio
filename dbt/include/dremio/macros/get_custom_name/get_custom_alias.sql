{% macro dremio__generate_alias_name(custom_alias_name=none, node=none) -%}
<<<<<<< HEAD
  {%- set custom_alias_name = custom_alias_name
    if node.config.materialized in ['view', 'raw_reflection', 'aggregation_reflection']
    else node.config.file -%}
  {{ generate_alias_name_impl(node.name, custom_alias_name, node) }}
{%- endmacro %}

{% macro generate_alias_name_impl(default_alias, custom_alias_name=none, node=none) -%}
  {%- if custom_alias_name is none -%}

      {{ default_alias }}

  {%- else -%}

      {{ custom_alias_name | trim }}

  {%- endif -%}
=======

    {%- if custom_alias_name is none -%}

        {{ node.name }}

    {%- else -%}

        {{ custom_alias_name | trim }}

    {%- endif -%}

>>>>>>> e8b196d307d9e0471f88722c45fdb43ac33c63dc
{%- endmacro %}
