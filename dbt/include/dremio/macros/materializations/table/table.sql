{% materialization table, adapter = 'dremio' %}

<<<<<<< HEAD
  {%- set identifier = model['alias'] -%}
  {%- set twin_strategy = config.get('twin_strategy', validator=validation.any[basestring]) or 'clone' -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}
  {{ run_hooks(pre_hooks) }}

  -- setup: if the target relation already exists, drop it
  -- in case if the existing and future table is delta, we want to do a
  -- create or replace table instead of dropping, so we don't have the table unavailable
  {% if old_relation is not none -%}
    {{ adapter.drop_relation(old_relation) }}
  {%- endif %}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {%- endcall %}

  {% call statement('refresh_metadata') -%}
    {%- if config.get('type') == 'parquet' -%}
      {{ alter_table_refresh_metadata(target_relation) }}
    {%- else -%}
      {{ alter_pds(target_relation, avoid_promotion=false, lazy_update=false) }}
    {%- endif -%}
  {%- endcall %}

  {{ table_twin_strategy(twin_strategy, target_relation) }}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}
=======
  {{ return(common_table(sql)) }}
>>>>>>> e8b196d307d9e0471f88722c45fdb43ac33c63dc

{% endmaterialization %}
