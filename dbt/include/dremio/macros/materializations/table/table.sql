{% materialization table, adapter='dremio' %}
  {%- set materialization_database = config.get('materialization_database', '$scratch') %}
  {%- set materialization_schema = config.get('materialization_schema', target.environment) %}
  {% set partition = config.get('partition') %}
  {% set sort = config.get('sort') %}
  {%- set identifier = model['alias'] -%}
  {%- set full_refresh_mode = True -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = this.incorporate(type='view') %}
  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% if exists_as_table %}
    {{ exceptions.raise_compiler_error("Cannot create virtual dataset '{}', there is already a physical dataset named the same".format(old_relation)) }}
  {% endif %}
  {% set old_table, target_table = dremio_get_old_and_target_tables(target_relation, materialization_database, materialization_schema) %}
  {{ drop_relation_if_exists(target_table) }}
  {% call statement('main') %}
    {{ dremio_create_table_as(False, target_table, sql, partition, sort) }}
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
