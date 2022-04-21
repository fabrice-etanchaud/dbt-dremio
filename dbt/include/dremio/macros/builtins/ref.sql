{%- macro ref(model_name) -%}
  {%- set relation = builtins.ref(model_name) -%}
  {%- if execute -%}
    {%- set model = graph.nodes.values() | selectattr("name", "equalto", model_name) | list | first -%}
    {%- set format_type = model.config.type if model.config.materialized != 'view' and model.config.type is defined else none -%}
    {%- set format_clause = format_clause_from_node(model) -%}
    {%- set relation2 = api.Relation.create(database=relation.database, schema=relation.schema, identifier=relation.identifier, format_type=format_type, format_clause=format_clause) -%}
    {{ return (relation2) }}
  {%- else -%}
    {{ return (relation) }}
  {%- endif -%}
{%- endmacro -%}

{%- macro format_clause_from_node(model) -%}

  {%- if model.config.materialized != 'view' and model.config.type is defined -%}
    {%- set options = [] -%}
    {%- set key = 'type' -%}
    {%- set type = model.config[key] -%}
    {%- if type is defined and type is string and type in ['text', 'json', 'arrow'] -%}
      {%- do options.append(key ~ "=>'" ~ type ~ "'") -%}
    {%- endif -%}
    {%- if model.config.type == 'text' -%}
      {%- for key in ['fieldDelimiter', 'lineDelimiter', 'quote', 'comment', 'escape'] -%}
        {%- set value = model.config[key] -%}
        {%- if value is defined and value is string -%}
          {%- do options.append(key ~ "=>'" ~ value ~ "'") -%}
        {%- endif -%}
      {%- endfor -%}
      {%- for key in ['skipFirstLine', 'extractHeader', 'trimHeader', 'autoGenerateColumnNames'] -%}
        {%- set value = model.config[key] -%}
        {%- if value is defined and value is boolean -%}
          {%- do options.append(key ~ "=>" ~ value) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- elif type == 'json' -%}
      {%- set key = 'prettyPrint' -%}
      {%- set value = model.config[key] -%}
      {%- if value is defined and value is boolean -%}
        {%- do options.append(key ~ "=>" ~ value) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
  {{ return((options | join(', ')) if options | length > 0 else none) }}
{%- endmacro -%}
