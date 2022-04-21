<<<<<<< HEAD
{#
/**
 * Parses a CTAS statement.
 * CREATE TABLE tblname [ (field1, field2, ...) ]
 *       [ (STRIPED, HASH, ROUNDROBIN) PARTITION BY (field1, field2, ..) ]
 *       [ DISTRIBUTE BY (field1, field2, ..) ]
 *       [ LOCALSORT BY (field1, field2, ..) ]
 *       [ STORE AS (opt1 => val1, opt2 => val3, ...) ]
 *       [ WITH SINGLE WRITER ]
 *       [ AS select_statement. ]
 */
#}

=======
>>>>>>> e8b196d307d9e0471f88722c45fdb43ac33c63dc
{% macro dremio__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create table {{ relation }}
<<<<<<< HEAD
  {{ partition_method() }} {{ config_cols("partition by") }}
  {{ config_cols("distribute by") }}
  {{ config_cols("localsort by") }}
=======
>>>>>>> e8b196d307d9e0471f88722c45fdb43ac33c63dc
  {{ store_as_clause() }}
  {{ single_writer_clause() }}
  as (
    {{ sql }}
  )
{%- endmacro -%}

<<<<<<< HEAD
{% macro config_cols(label) %}
  {%- set cols = config.get(label | replace(" ", "_"), validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}

{% macro partition_method() %}
  {%- set method = config.get('partition_method', validator=validation.any[basestring]) -%}
  {%- if method is not none -%}
   {{ method }}
  {%- endif %}
{%- endmacro -%}

{% macro store_as_clause() -%}
  {%- set options = format_options() -%}
  {%- if options is not none -%}
  store as ( {{ options }} )
  {%- endif %}
{%- endmacro -%}

=======
>>>>>>> e8b196d307d9e0471f88722c45fdb43ac33c63dc
{%- macro single_writer_clause() -%}
  {%- set single_writer = config.get('single_writer', validator=validation.any[boolean]) -%}
  {%- if single_writer is not none and single_writer -%}
    with single writer
  {%- endif -%}
{%- endmacro -%}
