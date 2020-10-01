{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {%- set default_database = target.database -%}
    {%- if target.name == 'managed' %}
      {%- if custom_database_name is not none -%}
        {{ custom_database_name | trim }}
      {%- elif default_database is not none -%}
        {{ default_database }}
      {%- else -%}
        {{ target.profile_name }}
      {%- endif -%}
    {%- else -%}
      {%- if default_database is not none %}
        {{ default_database }}
      {% else %}
        {{ get_user_database() }}
      {%- endif -%}
    {%- endif -%}
{%- endmacro %}
