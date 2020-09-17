{% materialization incremental, adapter='dremio' %}
{%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
{% set unique_key = config.get('unique_key') %}
{%- set materialization_database = config.rquire('materialization_database') %}
{%- set materialization_schema = config.require('materialization_schema') %}
{%- set identifier = model['alias'] -%}
{%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
{%- set target_relation = this.incorporate(type='view') %}
{%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
{%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
{{ run_hooks(pre_hooks, inside_transaction=False) }}
-- `BEGIN` happens here:
{{ run_hooks(pre_hooks, inside_transaction=True) }}
{% if exists_as_table %}
  {{ exceptions.raise_compiler_error("Cannot create virtual dataset '{}', there is already a physical dataset named the same".format(old_relation)) }}
{% endif %}
{% set old_table, target_table = dremio_get_old_and_target_tables(target_relation, materialization_database, materialization_schema) %}
{% if full_refresh_mode or old_relation is none %}
  {% set build_sql = sql %}
{% else %}
  {% set build_sql %}
    with increment as (
      {{ sql }}
    )
    select *
    from increment
    union all
    select *
    from {{ old_table }}
    {%- if unique_key is not none %}
    where {{ unique_key }} not in (
      select {{ unique_key }}
      from increment
    )
    {% endif %}
  {% endset %}
{% endif %}
{{ drop_relation_if_exists(target_table) }}
{% call statement('main') %}
  {{ create_table_as(False, target_table, build_sql) }}
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
