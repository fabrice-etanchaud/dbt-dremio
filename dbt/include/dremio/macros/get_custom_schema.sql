{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if target.name == 'managed' and custom_schema_name is not none -%}
      {{ custom_schema_name | trim }}
    {%- else -%}
      {{ default_schema }}
    {%- endif -%}
{%- endmacro %}
