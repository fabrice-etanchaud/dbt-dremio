{% macro dremio__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    with cols as (
      select lower(case when position('.' in table_schema) > 0
              then substring(table_schema, 1, position('.' in table_schema) - 1)
              else table_schema
          end) as table_catalog
          ,lower(case when position('.' in table_schema) > 0
              then substring(table_schema, position('.' in table_schema) + 1)
              else 'no_schema'
          end) as table_schema
          ,lower(table_name) as table_name
          ,lower(column_name) as column_name
          ,lower(data_type) as data_type
          ,character_maximum_length
          ,numeric_precision
          ,numeric_scale
          ,ordinal_position
      from information_schema.columns
      union all
      select
          lower(case when position('.' in table_schema) > 0
                  then substring(table_schema, 1, position('.' in table_schema) - 1)
                  else table_schema
              end)
          ,lower(case when position('.' in table_schema) > 0
                  then substring(table_schema, position('.' in table_schema) + 1)
                  else 'no_schema'
              end)
          ,lower(reflection_name)
          ,lower(column_name)
          ,lower(data_type)
          ,character_maximum_length
          ,numeric_precision
          ,numeric_scale
          ,ordinal_position
      from sys.reflections
      join information_schema.columns
          on (columns.table_schema || '.' || columns.table_name = replace(dataset_name, '"', '')
              and (strpos(',' || replace(display_columns, ' ', '') || ',', ',' || column_name || ',') > 0
                  or strpos(',' || replace(dimensions, ' ', '') || ',', ',' || column_name || ',') > 0
                  or strpos(',' || replace(measures, ' ', '') || ',', ',' || column_name || ',') > 0))
    )
    select column_name
      ,data_type
      ,character_maximum_length
      ,numeric_precision
      ,numeric_scale
    from cols
    where ilike(table_catalog, '{{ relation.database.strip("\"") }}')
      and ilike(table_schema, '{{ relation.schema.strip("\"") }}')
      and ilike(table_name, '{{ relation.identifier.strip("\"") }}')
    order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro dremio__alter_column_type(relation, column_name, new_column_type) -%}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} alter column {{ adapter.quote(column_name) }} {{ adapter.quote(column_name) }} {{ new_column_type }}
  {% endcall %}

{% endmacro %}

{% macro dremio__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}
  {% if remove_columns is none %}
    {% set remove_columns = [] %}
  {% endif %}

  {% if add_columns | length > 0 %}
    {% set sql -%}
       alter {{ relation.type }} {{ relation }} add columns (

              {% for column in add_columns %}
                 {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
              {% endfor %}
        )
    {%- endset -%}
    {% do run_query(sql) %}
  {% endif %}

  {% if remove_columns | length > 0 %}
    {% for column in remove_columns %}
      {% set sql -%}
         alter {{ relation.type }} {{ relation }} drop column {{ column.name }}
      {%- endset -%}
      {% do run_query(sql) %}
    {% endfor %}
  {% endif %}

{% endmacro %}
