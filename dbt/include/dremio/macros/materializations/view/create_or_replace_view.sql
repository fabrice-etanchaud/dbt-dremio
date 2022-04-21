{% macro create_or_replace_view() %}
  {%- set datalake = config.get('datalake', default=target.datalake) %}
  {%- set root_path = config.get('root_path', default=target.root_path) %}
  {%- set identifier = model['alias'] -%}
  {%- set file = config.get('file', default=identifier) %}

  {%- set target_view = api.Relation.create(database=database, schema=schema, identifier=identifier, type='view') -%}
  {%- set target_table = api.Relation.create(database=datalake, schema=root_path, identifier=file, type='table') -%}

  {{ run_hooks(pre_hooks) }}

  -- setup: in case the model was materialized before, drop the table
  {{ adapter.drop_relation(target_table) }}

  -- build model
  {% call statement('main') -%}
    {{ get_create_view_as_sql(target_view, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_view]}) }}

{% endmacro %}
