{% macro dremio_create_or_replace_view(run_outside_transaction_hooks=True) %}
  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema, database=database,
      type='view') -%}
  {% if run_outside_transaction_hooks %}
      -- no transactions on BigQuery
      {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {% endif %}
  -- `BEGIN` happens here on Snowflake
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {%- if old_relation is not none and old_relation.is_table -%}
    {{ handle_existing_table(flags.FULL_REFRESH, old_relation) }}
  {%- endif -%}
  -- build model
  {% call statement('main') -%}
    {{ create_view_as(target_relation, sql) }}
  {%- endcall %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {% if run_outside_transaction_hooks %}
      -- No transactions on BigQuery
      {{ run_hooks(post_hooks, inside_transaction=False) }}
  {% endif %}
  {{ return({'relations': [target_relation]}) }}
{% endmacro %}

{% materialization view, adapter='dremio' -%}
    {{ return(dremio_create_or_replace_view()) }}
{%- endmaterialization %}
