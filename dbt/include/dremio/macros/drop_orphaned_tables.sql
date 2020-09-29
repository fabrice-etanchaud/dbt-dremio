{%- macro drop_cascade_orphaned_tables(materialization_database, materialization_schema) -%}
  {%- set path = ([materialization_database] + ([materialization_schema] if materialization_schema != 'no_schema' else [])) | join('.')  -%}
  {% call statement('list_orphan_tables', fetch_result=True) %}
    select
        lower(case when position('.' in table_schema) > 0
                then substring(table_schema, 1, position('.' in table_schema) - 1)
                else table_schema
            end) as table_catalog
        ,lower(case when position('.' in table_schema) > 0
                then substring(table_schema, position('.' in table_schema) + 1)
                else 'no_schema'
            end) as table_schema
        ,lower(table_name) as table_name
    from information_schema."tables" as t
    where table_type = 'TABLE'
    and table_schema like concat('{{ path }}', '.%')
    and not exists (
        select 1
        from information_schema."views" as v
        where v.table_schema = substr(t.table_schema, length('{{ path }}') + 2)
        and v.table_name = t.table_name
    )
  {%- endcall -%}
  {%- set table = load_result('list_orphan_tables').table -%}
  {%- for row in table.rows -%}
    {% set relation = api.Relation.create(database=row[0], schema=row[1], identifier=row[2], type='table') %}
    {{ log(relation, info=True) }}
    {{ adapter.drop_relation(relation) }}
  {%- endfor -%}

{%- endmacro -%}
