{% macro dremio__information_schema_name(database) -%}
    INFORMATION_SCHEMA
{%- endmacro %}

{% macro dremio__list_schemas(database) -%}
{{ log("database:" ~ database, info=True) }}
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

{% macro dremio__list_relations_without_caching(schema_relation) %}
{{ log("schema_relation:" ~ schema_relation, info=True) }}
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
