
{#
"type": String ['text', 'json', 'arrow', 'parquet', 'iceberg']
for 'text' :
"fieldDelimiter": String,
"lineDelimiter": String,
"quote": String,
"comment": String,
"escape": String,
"skipFirstLine": Boolean,
"extractHeader": Boolean,
"trimHeader": Boolean,
"autoGenerateColumnNames": Boolean
for 'json' :
"prettyPrint" : Boolean
#}

{% macro format_clause_from_config() -%}
  {%- set options = [] -%}
  {%- set key = 'type' -%}
  {%- set type = config.get(key, validator=validation.any[basestring]) or 'iceberg' -%}
  {%- if type in ['text', 'json', 'arrow', 'parquet'] -%}
    {%- do options.append(key ~ "=>'" ~ type ~ "'") -%}
    {%- if type == 'text' -%}
      {%- for key in ['fieldDelimiter', 'lineDelimiter', 'quote', 'comment', 'escape'] -%}
        {%- set value = config.get(key, validator=validation.any[basestring]) -%}
        {%- if value is not none -%}
          {%- do options.append(key ~ "=>'" ~ value ~ "'") -%}
        {%- endif -%}
      {%- endfor -%}
      {%- for key in ['skipFirstLine', 'extractHeader', 'trimHeader', 'autoGenerateColumnNames'] -%}
        {%- set value = config.get(key, validator=validation.any[boolean]) -%}
        {%- if value is not none -%}
          {%- do options.append(key ~ "=>" ~ value) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- elif type == 'json' -%}
      {%- set key = 'prettyPrint' -%}
      {%- set value = config.get(key, validator=validation.any[boolean]) -%}
      {%- if value is not none -%}
        {%- do options.append(key ~ "=>" ~ value) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
  {{ return((options | join(', ')) if options | length > 0 else none) }}
{%- endmacro -%}

{%- macro format_clause_from_node(config) -%}
  {%- set options = [] -%}
  {%- set key = 'type' -%}
  {%- set type = config[key] -%}
  {%- if type is defined and type is string and type in ['text', 'json', 'arrow', 'parquet'] -%}
    {%- do options.append(key ~ "=>'" ~ type ~ "'") -%}
  {%- endif -%}
  {%- if config.type == 'text' -%}
    {%- for key in ['fieldDelimiter', 'lineDelimiter', 'quote', 'comment', 'escape'] -%}
      {%- set value = config[key] -%}
      {%- if value is defined and value is string -%}
        {%- do options.append(key ~ "=>'" ~ value ~ "'") -%}
      {%- endif -%}
    {%- endfor -%}
    {%- for key in ['skipFirstLine', 'extractHeader', 'trimHeader', 'autoGenerateColumnNames'] -%}
      {%- set value = config[key] -%}
      {%- if value is defined and value is boolean -%}
        {%- do options.append(key ~ "=>" ~ value) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- elif type == 'json' -%}
    {%- set key = 'prettyPrint' -%}
    {%- set value = config[key] -%}
    {%- if value is defined and value is boolean -%}
      {%- do options.append(key ~ "=>" ~ value) -%}
    {%- endif -%}
  {%- endif -%}
  {{ return((options | join(', ')) if options | length > 0 else none) }}
{%- endmacro -%}

{% macro render_with_format_clause(target_table) %}
  {%- set options = format_clause_from_config() -%}
  {% if options is not none -%}
    table(
  {%- endif %}
  {{ target_table }}
  {%- if options is not none -%}
    ( {{ options }} ))
  {%- endif -%}
{% endmacro %}
