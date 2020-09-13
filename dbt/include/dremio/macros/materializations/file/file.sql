{% materialization file, adapter='dremio' %}
  {%- set identifier = model['alias'] -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
    -- cleanup
  {{ drop_relation_if_exists(target_relation) }}
  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {%- endcall %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  -- `COMMIT` happens here
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
