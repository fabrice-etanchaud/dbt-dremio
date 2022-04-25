{%- macro ref(model_name) -%}
  {%- set relation = builtins.ref(model_name) -%}
  {%- if execute -%}
    {%- set model = graph.nodes.values() | selectattr("name", "equalto", model_name) | list | first -%}
    {%- set format_type = model.config.type if
      model.config.materialized not in ['view', 'raw_reflection', 'aggregation_reflection']
      and model.config.type is defined
      else none -%}
    {%- set format_clause = format_clause_from_node(model.config) if format_type is not none else none -%}
    {%- set relation2 = api.Relation.create(database=relation.database, schema=relation.schema, identifier=relation.identifier, format_type=format_type, format_clause=format_clause) -%}
    {{ return (relation2) }}
  {%- else -%}
    {{ return (relation) }}
  {%- endif -%}
{%- endmacro -%}

{%- macro source(source_name, table_name) -%}
  {%- set relation = builtins.source(source_name, table_name) -%}
  {%- if execute -%}
    {%- set source = graph.sources.values() | selectattr("source_name", "equalto", source_name) | selectattr("name", "equalto", table_name) | list | first -%}
    {%- set format_type = source.external.type if
      source.external is defined
      and source.external.type is defined
      else none -%}
    {%- set format_clause = format_clause_from_node(source.external) if format_type is not none else none -%}
    {%- set relation2 = api.Relation.create(database=relation.database, schema=relation.schema, identifier=relation.identifier, format_type=format_type, format_clause=format_clause) -%}
    {{ return (relation2) }}
  {%- else -%}
    {{ return (relation) }}
  {%- endif -%}
{%- endmacro -%}
