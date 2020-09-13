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
    ,lower(name)
    ,'materializedview'
    ,initcap(type) || ' Reflection'
    ,lower(column_name)
    ,ordinal_position
    ,lower(data_type)
    ,case
        when strpos(',' || replace(displayColumns, ' ', '') || ',', ',' || column_name || ',') > 0 then 'Display'
        when strpos(',' || replace(dimensions, ' ', '') || ',', ',' || column_name || ',') > 0 then 'Dimension'
        when strpos(',' || replace(measures, ' ', '') || ',', ',' || column_name || ',') > 0 then 'Measure'
    end
    || case
        when strpos(',' || replace(sortColumns, ' ', '') || ',', ',' || column_name || ',') > 0 then ', Sort'
        else ''
    end
    || case
        when strpos(',' || replace(partitionColumns, ' ', '') || ',', ',' || column_name || ',') > 0 then ', Partition'
        else ''
    end
    || case
        when strpos(',' || replace(distributionColumns, ' ', '') || ',', ',' || column_name || ',') > 0 then ', Distribute'
        else ''
    end
    ,cast(null as varchar)
  from sys.reflections
  join information_schema.columns
      on (columns.table_schema || '.' || columns.table_name = replace(dataset, '"', '')
          and (strpos(',' || replace(displayColumns, ' ', '') || ',', ',' || column_name || ',') > 0
              or strpos(',' || replace(dimensions, ' ', '') || ',', ',' || column_name || ',') > 0
              or strpos(',' || replace(measures, ' ', '') || ',', ',' || column_name || ',') > 0))
  )
  select *
  from cols
  where table_type <> 'SYSTEM_TABLE'
    and table_database = lower('{{ information_schema.database.strip('"') }}')
    and (
        {%- for schema in schemas -%}
          table_schema = lower('{{ schema.strip('"') }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
      )
  order by
      table_schema,
      table_name,
      column_index

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
