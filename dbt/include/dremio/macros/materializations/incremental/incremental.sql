{% materialization incremental, adapter='dremio' %}
{%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
{% set unique_key = config.get('unique_key') %}
{%- set materialization_database = config.get('materialization_database', default='$scratch') %}
{%- set materialization_schema = config.get('materialization_schema', default=target.environment) %}
{% set partition = config.get('partition') %}
{% set sort = config.get('sort') %}
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
{{ log(old_table, info=true) }}
{{ log(target_table, info=true) }}
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
{{ log("drop target table " ~ target_table, info=true) }}
{% call statement('main') %}
  {{ dremio_create_table_as(target_table, build_sql, partition, sort) }}
{% endcall %}
{{ log("create table " ~ target_table, info=true) }}
{% call statement('create view') %}
  {{ create_view_as(target_relation, 'select * from ' ~ target_table) }}
{% endcall %}
{{ log("create view on " ~ target_table, info=true) }}
{{ drop_relation_if_exists(old_table) }}
{{ log("drop old table " ~ old_table, info=true) }}
{% do persist_docs(target_relation, model) %}
{{ run_hooks(post_hooks, inside_transaction=True) }}
-- `COMMIT` happens here
{{ adapter.commit() }}
{{ run_hooks(post_hooks, inside_transaction=False) }}
{{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
