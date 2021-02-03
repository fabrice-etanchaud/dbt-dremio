{% macro dremio_create_table_as(relation, sql, partition=none, sort=none) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create table
    {{ relation }}
  {% if partition is not none %}
    hash partition by ( {{ partition | map('tojson') | join(', ') }} )
  {% endif %}
  {% if sort is not none %}
    localsort by ( {{ sort | map('tojson') | join(', ') }} )
  {% endif %}
  as (
    {{ sql }}
  )
{% endmacro %}

{% macro get_user_database() %}
  {{ '@' ~ target.user | trim }}
{% endmacro %}

{% macro drop_reflection_if_exists(view, reflection) %}
  {% if reflection is not none %}
    {% call statement('drop reflection') -%}
      alter dataset {{ view }} drop reflection {{ reflection.include(database=False, schema=False) }}
    {%- endcall %}
  {% endif %}
{% endmacro %}

{% macro old_dremio_get_old_and_target_tables(view, materialization_database, materialization_schema) %}
  {% set materialization_schema = materialization_schema if materialization_schema != 'no_schema' else ([view.database] + ([ view.schema ] if view.schema != 'no_schema' else [])) | join ('.') %}
  {% set color_table = api.Relation.create(database=materialization_database, schema=materialization_schema, identifier=view.identifier, type='table') %}
  {{ return([color_table, color_table]) }}
{% endmacro %}

{% macro dremio_get_old_and_target_tables(view, materialization_database, materialization_schema) %}
  {% set materialization_schema = materialization_schema if materialization_schema != 'no_schema' else ([view.database] + ([ view.schema ] if view.schema != 'no_schema' else [])) | join ('.') %}
  {% set blue_table = api.Relation.create(database=materialization_database, schema=materialization_schema, identifier=(view.identifier ~ '_blue'), type='table') %}
  {% set green_table = api.Relation.create(database=materialization_database, schema=materialization_schema, identifier=(view.identifier ~ '_green'), type='table') %}
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
