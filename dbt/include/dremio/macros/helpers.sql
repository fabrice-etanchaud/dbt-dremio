{% macro create_raw_reflection(view, reflection, display, sort=None, partition=None, distribute=None) %}
  alter dataset {{ view }}
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

{% macro create_aggregation_reflection(view, reflection, dimensions, measures, sort=None, partition=None, distribute=None) %}
  alter dataset {{ view }}
    create aggregate reflection {{ reflection.include(database=False, schema=False) }}
      using dimensions( {{ dimensions | map('tojson') | join(', ') }} )
      measures( {{ measures | map('tojson') | join(', ') }} )
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

{% macro drop_reflection_if_exists(view, reflection) %}
  {% if reflection is not none %}
    {% call statement('drop reflection') -%}
      alter dataset {{ view }} drop reflection {{ reflection.include(database=False, schema=False) }}
    {%- endcall %}
  {% endif %}
{% endmacro %}

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

{% macro dremio_get_old_and_target_tables(view, table_database) %}
  {% set table_schema = (['dbt', 'internal', view.database] + ([ view.schema ] if view.schema != 'no_schema' else [])) | join ('.') %}
  {% set blue_table = api.Relation.create(database=table_database, schema=table_schema, identifier=(view.identifier ~ '_blue'), type='table') %}
  {% set green_table = api.Relation.create(database=table_database, schema=table_schema, identifier=(view.identifier ~ '_green'), type='table') %}
  {% set blue_table_exists = load_relation(blue_table) is not none %}
  {% set green_table_exists = load_relation(green_table) is not none %}
  {% set old_table = none %}
  {% set target_table = none %}
  {% if not (green_table_exists and blue_table_exists) %}
      {% if not green_table_exists %}
      {% set old_table = blue_table %}
      {% set target_table = green_table %}
    {% else %}
      {% set old_table = green_table %}
      {% set target_table = blue_table %}
    {% endif %}
    {% else %}
    {% set definition = dremio_get_view_definition(view) %}
      {% if blue_table.render() in definition %}
      {% set old_table = blue_table %}
      {% set target_table = green_table %}
    {% else %}
      {% set old_table = green_table %}
      {% set target_table = blue_table %}
    {% endif %}
  {% endif %}
  {{ return([old_table, target_table]) }}
{% endmacro %}

{% macro dremio_get_view_definition(relation) %}

  {% call statement('get_view_definition', fetch_result=True) -%}
    with t(table_catalog, table_name, table_schema, view_definition) as (
    select lower(case when position('.' in table_schema) > 0
            then substring(table_schema, 1, position('.' in table_schema) - 1)
            else table_schema
        end)
        ,lower(table_name)
        ,lower(case when position('.' in table_schema) > 0
            then substring(table_schema, position('.' in table_schema) + 1)
            else 'no_schema'
        end)
        ,view_definition
    from information_schema.views
    )

    select view_definition
    from t
    where ilike(table_catalog, '{{ relation.database.strip('"') }}')
      and ilike(table_schema, '{{ relation.schema.strip('"') }}')
      and ilike(table_name, '{{ relation.identifier.strip('"') }}')
  {% endcall %}
  {% set result = load_result('get_view_definition').table %}
  {{ return(result.rows[0].view_definition if result.rows | count > 0 else none ) }}
{% endmacro %}
