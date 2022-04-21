{% materialization incremental, adapter='dremio' -%}

  {%- set identifier = model['alias'] -%}
  {%- set twin_strategy = config.get('twin_strategy', validator=validation.any[basestring]) or 'clone' -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set tmp_relation = adapter.get_relation(database=database, schema=schema, identifier=tmp_identifier) -%}
  {%- set target_tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {%- set raw_strategy = config.get('incremental_strategy', validator=validation.any[basestring]) or 'append' -%}
  {%- set raw_file_format = config.get('type', validator=validation.any[basestring]) or 'iceberg' -%}
  {%- set file_format = dbt_dremio_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_dremio_validate_get_incremental_strategy(raw_strategy, file_format) -%}
  {%- set unique_key = config.get('unique_key', validator=validation.any[list, basestring]) -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set raw_on_schema_change = config.get('on_schema_change', validator=validation.any[basestring]) or 'ignore' -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(raw_on_schema_change) -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {{ run_hooks(pre_hooks) }}

  {% if old_relation is none or full_refresh_mode %}
    {% if full_refresh_mode %}
      {{ adapter.drop_relation(old_relation) }}
    {% endif %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {% if tmp_relation is not none %}
      {{ adapter.drop_relation(tmp_relation) }}
    {% endif %}
    {{ run_query(create_table_as(True, target_tmp_relation, sql)) }}
    {{ process_schema_changes(on_schema_change, target_tmp_relation, old_relation) }}
    {% set build_sql = dbt_dremio_get_incremental_sql(strategy, target_tmp_relation, target_relation, unique_key) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {% do persist_docs(target_relation, model) %}

  {% if not(old_relation is none or full_refresh_mode) %}
    {{ adapter.drop_relation(target_tmp_relation) }}
  {% endif %}

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

{%- endmaterialization %}
