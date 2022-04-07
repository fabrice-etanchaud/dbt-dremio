{% macro dremio__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create or replace view {{ relation }} as (
    {{ sql }}
  )
{%- endmacro %}
