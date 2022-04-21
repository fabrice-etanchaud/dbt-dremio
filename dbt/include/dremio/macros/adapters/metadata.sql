{% macro dremio__get_catalog(information_schema, schemas) -%}

  {%- call statement('catalog', fetch_result=True) -%}
  with cols(table_database,
    table_schema,
    table_name,
    table_type,
    table_comment,
    column_name,
    column_index,
    column_type,
    column_comment,
    table_owner) as (
  select lower(case when position('.' in columns.table_schema) > 0
        then substring(columns.table_schema, 1, position('.' in columns.table_schema) - 1)
        else columns.table_schema
    end)
    ,lower(case when position('.' in columns.table_schema) > 0
        then substring(columns.table_schema, position('.' in columns.table_schema) + 1)
        else 'no_schema'
    end)
    ,lower(columns.table_name)
    ,lower(t.table_type)
    ,cast(null as varchar)
    ,lower(column_name)
    ,ordinal_position
    ,lower(data_type)
    ,cast(null as varchar)
    ,cast(null as varchar)
  from information_schema.columns
      join information_schema."tables" as t
          on (t.table_schema = columns.table_schema
              and t.table_name = columns.table_name)
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
    ,'materializedview'
    ,initcap(type) || ' Reflection'
    ,lower(column_name)
    ,ordinal_position
    ,lower(data_type)
    ,case
        when strpos(',' || replace(display_columns, ' ', '') || ',', ',' || column_name || ',') > 0 then 'Display'
        when strpos(',' || replace(dimensions, ' ', '') || ',', ',' || column_name || ',') > 0 then 'Dimension'
        when strpos(',' || replace(measures, ' ', '') || ',', ',' || column_name || ',') > 0 then 'Measure'
    end
    || case
        when strpos(',' || replace(sort_columns, ' ', '') || ',', ',' || column_name || ',') > 0 then ', Sort'
        else ''
    end
    || case
        when strpos(',' || replace(partition_columns, ' ', '') || ',', ',' || column_name || ',') > 0 then ', Partition'
        else ''
    end
    || case
        when strpos(',' || replace(distribution_columns, ' ', '') || ',', ',' || column_name || ',') > 0 then ', Distribute'
        else ''
    end
    ,cast(null as varchar)
  from sys.reflections
  join information_schema.columns
      on (columns.table_schema || '.' || columns.table_name = replace(dataset_name, '"', '')
          and (strpos(',' || replace(display_columns, ' ', '') || ',', ',' || column_name || ',') > 0
              or strpos(',' || replace(dimensions, ' ', '') || ',', ',' || column_name || ',') > 0
              or strpos(',' || replace(measures, ' ', '') || ',', ',' || column_name || ',') > 0))
  )
  select *
  from cols
  where table_type <> 'SYSTEM_TABLE'
    and table_database = lower('{{ information_schema.database.strip("\"") }}')
    and (
        {%- for schema in schemas -%}
          table_schema = lower('{{ schema.strip("\"") }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
      )
  order by
      table_schema,
      table_name,
      column_index

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}


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
        from {{ information_schema_name(database) }}.SCHEMATA
    )
    select distinct schema_name
    from schemata
    where ilike(catalog_name, '{{ database.strip("\"") }}')
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
        from {{ information_schema.replace(information_schema_view='SCHEMATA') }}
    )
    select count(*)
    from schemata
    where ilike(catalog_name, '{{ information_schema.database.strip("\"") }}')
      and ilike(schema_name, '{{ schema }}')
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
            case when "RIGHT"(dataset_name, 1) = '"'
                then length(dataset_name) - strpos(substr(reverse(dataset_name), 2), '"')
                else length(dataset_name) - strpos(reverse(dataset_name), '.') + 2
            end
            ,case when "LEFT"(dataset_name, 1) = '"'
                then strpos(substr(dataset_name, 2), '"') + 1
                else strpos(dataset_name, '.') - 1
            end
            ,dataset_name
            ,reflection_name
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
    where ilike(table_catalog, '{{ schema_relation.database.strip("\"") }}')
      and ilike(table_schema, '{{ schema_relation.schema.strip("\"") }}')
      and table_type <> 'system_table'
  {% endcall %}
  {% set t = load_result('list_relations_without_caching').table %}
  {{ return(t) }}
{% endmacro %}
