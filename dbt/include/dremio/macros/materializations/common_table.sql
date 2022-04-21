{%- macro common_table(sql, statement_name='main') -%}

{%- set datalake = config.get('datalake', default=target.datalake) %}
{%- set root_path = config.get('root_path', default=target.root_path) %}
{%- set identifier = model['alias'] -%}
{%- set file = config.get('file', default=identifier) %}

{%- set target_view = api.Relation.create(database=database, schema=schema, identifier=identifier, type='view') -%}
{%- set target_table = api.Relation.create(database=datalake, schema=root_path, identifier=file, type='table') -%}

{{ run_hooks(pre_hooks) }}

-- setup: if the target relation already exists, drop it
{{ adapter.drop_relation(target_table) }}

-- build model
{% call statement(statement_name) -%}
  {{ create_table_as(False, target_table, sql) }}
{%- endcall %}

{%- set wrapper_view -%}
  select *
  from {{ render_with_format_options(target_table) }}
{%- endset -%}

{% call statement('wrapper_view') -%}
  {{ create_view_as(target_view, wrapper_view) }}
{%- endcall %}

{% do persist_docs(target_view, model) %}

{{ run_hooks(post_hooks) }}

{{ return({'relations': [target_table, target_view] }) }}

{%- endmacro -%}
