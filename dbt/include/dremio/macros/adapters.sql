{% macro dremio__create_schema(relation) -%}
  {{ log('create_schema macro (' + relation.render() + ') not implemented yet for adapter ' + adapter.type(), info=True) }}
{% endmacro %}

{% macro dremio__drop_schema(relation) -%}
{{ exceptions.raise_not_implemented(
  'drop_schema macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro dremio__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create table
    {{ relation }}
  as (
    {{ sql }}
  )
{% endmacro %}

{% macro dremio__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create or replace view {{ relation }} as (
    {{ sql }}
  )
{% endmacro %}

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
          ,lower(name)
          ,lower(column_name)
          ,lower(data_type)
          ,character_maximum_length
          ,numeric_precision
          ,numeric_scale
          ,ordinal_position
      from sys.reflections
      join information_schema.columns
          on (columns.table_schema || '.' || columns.table_name = replace(dataset, '"', '')
              and (strpos(',' || replace(displayColumns, ' ', '') || ',', ',' || column_name || ',') > 0
                  or strpos(',' || replace(dimensions, ' ', '') || ',', ',' || column_name || ',') > 0
                  or strpos(',' || replace(measures, ' ', '') || ',', ',' || column_name || ',') > 0))
    )
    select column_name
      ,data_type
      ,character_maximum_length
      ,numeric_precision
      ,numeric_scale
    from cols
    where ilike(table_catalog, '{{ relation.database.strip('"') }}')
      and ilike(table_schema, '{{ relation.schema.strip('"') }}')
      and ilike(table_name, '{{ relation.identifier.strip('"') }}')
    order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro dremio__alter_column_comment(relation, column_dict) -%}
  {{ exceptions.raise_not_implemented(
    'alter_column_comment macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro dremio__alter_relation_comment(relation, relation_comment) -%}
  {{ exceptions.raise_not_implemented(
    'alter_relation_comment macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro dremio__alter_column_type(relation, column_name, new_column_type) -%}
  {{ exceptions.raise_not_implemented(
    'alter_column_type macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro dremio__drop_relation(relation) -%}
  {% call statement('drop_relation', fetch_result=False, auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro dremio__truncate_relation(relation) -%}
  {{ exceptions.raise_not_implemented(
    'truncate_relation macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro dremio__rename_relation(from_relation, to_relation) -%}
  {{ exceptions.raise_not_implemented(
    'rename_relation macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{% macro dremio__information_schema_name(database) -%}
    information_schema
{%- endmacro %}

{% macro dremio__list_schemas(database) -%}
  {% set sql %}
    with schemata as (
        select lower(case when position('.' in schema_name) > 0
                then substring(schema_name, 1, position('.' in schema_name) - 1)
                else schema_name
            end) as catalog_name
            ,lower(case when position('.' in schema_name) > 0
                then substring(schema_name, position('.' in schema_name) + 1)
                else 'no_schema'
            end) as schema_name
        from information_schema.schemata
    )
    select distinct schema_name
    from schemata
    where ilike(catalog_name, '{{ database.strip('"') }}')
      -- and schema_name <> 'no_schema'
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro dremio__check_schema_exists(information_schema, schema) -%}
  {% set sql -%}
    with schemata as (
        select lower(case when position('.' in schema_name) > 0
                then substring(schema_name, 1, position('.' in schema_name) - 1)
                else schema_name
            end) as catalog_name
            ,lower(case when position('.' in schema_name) > 0
                then substring(schema_name, position('.' in schema_name) + 1)
                else 'no_schema'
            end) as schema_name
        from information_schema.schemata
    )
    select count(*)
    from schemata
    where catalog_name = lower('{{ information_schema.database.strip('"') }}')
      and schema_name = lower('{{ schema.strip('"') }}')
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro dremio__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    with t1(table_catalog, table_name, table_schema, table_type) as (
    select lower(case when position('.' in table_schema) > 0
            then substring(table_schema, 1, position('.' in table_schema) - 1)
            else table_schema
        end)
        ,lower(table_name)
        ,lower(case when position('.' in table_schema) > 0
            then substring(table_schema, position('.' in table_schema) + 1)
            else 'no_schema'
        end)
        ,lower(table_type)
    from information_schema."tables"
    )
    ,r1(identifier_position, database_end_position, dataset, name, type) as (
        select
            case when "RIGHT"(dataset, 1) = '"'
                then length(dataset) - strpos(substr(reverse(dataset), 2), '"')
                else length(dataset) - strpos(reverse(dataset), '.') + 2
            end
            ,case when "LEFT"(dataset, 1) = '"'
                then strpos(substr(dataset, 2), '"') + 1
                else strpos(dataset, '.') - 1
            end
            ,dataset
            ,name
            ,type
        from sys.reflections
    )
    ,r2(table_catalog, table_name, table_schema, table_type) as (
    select
        lower(replace(substr(dataset, 1, database_end_position), '"', ''))
        ,lower(name)
        ,case when identifier_position - database_end_position > 2
            then lower(replace(substr(dataset, database_end_position + 2, identifier_position - database_end_position - 3), '"', ''))
            else 'no_schema'
        end
        -- ,lower(type) || 'reflection'
        ,'materializedview'
--        ,replace(substr(dataset, identifier_position), '"', '') as identifier
    from r1
    )
    ,u(table_catalog, table_name, table_schema, table_type) as (
      select *
      from t1
      union all
      select *
      from r2
    )

    select *
    from u
    where ilike(table_catalog, '{{ schema_relation.database.strip('"') }}')
      and ilike(table_schema, '{{ schema_relation.schema.strip('"') }}')
      and table_type <> 'system_table'
  {% endcall %}
  {% set t = load_result('list_relations_without_caching').table %}
  {{ return(t) }}
{% endmacro %}

{% macro dremio__current_timestamp() -%}
  CURRENT_TIMESTAMP
{%- endmacro %}

{% macro dremio__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {% set tmp_relation = base_relation.incorporate(
                                path={"identifier": tmp_identifier}) -%}

    {% do return(tmp_relation) %}
{% endmacro %}
