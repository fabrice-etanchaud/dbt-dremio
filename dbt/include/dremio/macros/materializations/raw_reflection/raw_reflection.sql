{% macro create_raw_reflection(dataset, reflection, display, sort=None, partition=None, distribute=None) %}
  alter dataset {{ dataset }}
    create raw reflection {{ reflection.include(database=False, schema=False) }}
      using display( {{ display | map('tojson') | join(', ') }} )
      {% if partition is not none %}
        partition by ( {{ partition | map('tojson') | join(', ') }} )
      {% endif %}
      {% if sort is not none %}
        localsort by ( {{ sort | map('tojson') | join(', ') }} )
      {% endif %}
      {% if distribute is not none %}
        distribute by ( {{ distribute | map('tojson') | join(', ') }} )
      {% endif %}
{% endmacro %}

{% materialization raw_reflection, adapter='dremio' %}
  {% set dataset = config.require('dataset') %}
  {% set display = config.get('display') %}
  {% set partition = config.get('partition') %}
  {% set sort = config.get('sort') %}
  {% set distribute = config.get('distribute') %}
  {% set dataset = ref(dataset) %}
  {% set identifier = model['alias'] %}
  {%- set old_relation = adapter.get_relation(database=dataset.database, schema=dataset.schema, identifier=identifier) -%}
  {%- set target_relation = this.incorporate(database=dataset.database, schema=dataset.schema, type='materializedview') %}
  {% if display is none %}
    {% set display = adapter.get_columns_in_relation(dataset) | map(attribute='name') | list %}
  {% endif %}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
    -- cleanup
  {{ drop_reflection_if_exists(dataset, old_relation) }}
  -- build model
  {% call statement('main') -%}
    {{ create_raw_reflection(dataset, target_relation, display, sort, partition, distribute) }}
  {%- endcall %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  -- `COMMIT` happens here
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
