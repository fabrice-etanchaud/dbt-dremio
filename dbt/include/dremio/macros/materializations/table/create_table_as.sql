{% macro dremio__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create table {{ relation }}
  {{ store_as_clause() }}
  {{ single_writer_clause() }}
  as (
    {{ sql }}
  )
{%- endmacro -%}

{%- macro single_writer_clause() -%}
  {%- set single_writer = config.get('single_writer', validator=validation.any[boolean]) -%}
  {%- if single_writer is not none and single_writer -%}
    with single writer
  {%- endif -%}
{%- endmacro -%}
