{% materialization seed, adapter = 'dremio' %}

  {%- set identifier = model['alias'] -%}
  {%- set format = config.get('format', validator=validation.any[basestring]) or 'iceberg' -%}
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

  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}
  {%- set num_rows = (agate_table.rows | length) -%}
  {%- set sql = select_csv_rows(model, agate_table) -%}

  -- build model
  {% call statement('effective_main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {%- endcall %}

  {% call noop_statement('main', 'CREATE ' ~ num_rows, 'CREATE', num_rows) %}
    {{ sql }}
  {% endcall %}

  {{ refresh_metadata(target_relation, format) }}

  {{ apply_twin_strategy(target_relation) }}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}
