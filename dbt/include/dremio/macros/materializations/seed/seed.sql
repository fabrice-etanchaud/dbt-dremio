<<<<<<< HEAD
{% materialization seed, adapter = 'dremio' %}

  {%- set identifier = model['alias'] -%}

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

=======
{% materialization seed, adapter='dremio' %}
>>>>>>> e8b196d307d9e0471f88722c45fdb43ac33c63dc
  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}
  {%- set num_rows = (agate_table.rows | length) -%}
  {%- set sql = select_csv_rows(model, agate_table) -%}
<<<<<<< HEAD

  -- build model
  {% call statement('effective_main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {%- endcall %}

  {% call statement('refresh_metadata') -%}
    {%- if config.get('type') == 'parquet' -%}
      {{ alter_table_refresh_metadata(target_relation) }}
    {%- else -%}
      {{ alter_pds(target_relation, avoid_promotion=false, lazy_update=false) }}
    {%- endif -%}
  {%- endcall %}

  {% call noop_statement('main', 'CREATE ' ~ num_rows, 'CREATE', num_rows) %}
    {{ sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

=======
  {%- set result = common_table(sql, 'seed') -%}
  {% call noop_statement('main', 'CREATE ' ~ num_rows, 'CREATE', num_rows) %}
    {{ sql }}
  {% endcall %}
  {{ return(result) }}
>>>>>>> e8b196d307d9e0471f88722c45fdb43ac33c63dc
{% endmaterialization %}
