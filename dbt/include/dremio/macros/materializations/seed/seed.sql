{% macro dremio__create_csv_table(model, agate_table) %}
  {{ exceptions.raise_not_implemented(
    'create_csv_table macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro dremio__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
  {{ exceptions.raise_not_implemented(
    'reset_csv_table macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro dremio__load_csv_rows(model, agate_table) %}
  {{ exceptions.raise_not_implemented(
    'load_csv_rows macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro dremio_select_csv_rows(model, agate_table) %}
{%- set column_override = model['config'].get('column_types', {}) -%}
{%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
{%- set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) -%}
  select
    {% for col_name in agate_table.column_names -%}
      {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
      {%- set type = column_override.get(col_name, inferred_type) -%}
      {%- set column_name = (col_name | string) -%}
      cast({{ adapter.quote_seed_column(column_name, quote_seed_column) }} as {{ type }})
        as {{ adapter.quote_seed_column(column_name, quote_seed_column) }}{%- if not loop.last -%}, {%- endif -%}
    {% endfor %}
  from
    (values
      {% for row in agate_table.rows %}
        ({%- for value in row -%}
          {% if value is not none %}
            {{ "'" ~ (value | string | replace("'", "''")) ~ "'" }}
          {% else %}
            cast(null as varchar)
          {% endif %}
          {%- if not loop.last%},{%- endif %}
        {%- endfor -%})
        {%- if not loop.last%},{%- endif %}
      {% endfor %}) temp_table ( {{ cols_sql }} )
{% endmacro %}

{% materialization seed, adapter='dremio' %}
  {%- set materialization_database = config.get('materialization_database', default='$scratch') %}
  {%- set identifier = model['alias'] -%}
  {%- set full_refresh_mode = True -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {% set target_relation = this.incorporate(type='view') %}
  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', status='OK', agate_table=agate_table) -%}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% if exists_as_table %}
    {{ exceptions.raise_compiler_error("Cannot seed to '{}', it is a table".format(old_relation)) }}
  {% endif %}
  {% set num_rows = (agate_table.rows | length) %}
  {% set sql = dremio_select_csv_rows(model, agate_table) %}
  {% set old_table, target_table = dremio_get_old_and_target_tables(target_relation, materialization_database) %}
  {{ drop_relation_if_exists(target_table) }}
  {% call statement('main') %}
    {{ create_table_as(False, target_table, sql) }}
  {% endcall %}
  {% call statement('create view') %}
    {{ create_view_as(target_relation, 'select * from ' ~ target_table) }}
  {% endcall %}
  {{ drop_relation_if_exists(old_table) }}
  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  -- `COMMIT` happens here
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
