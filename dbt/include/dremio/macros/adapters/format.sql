
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

{% macro format_options() -%}
  {%- set options = [] -%}
  {%- set key = 'type' -%}
  {%- set type = config.get(key, validator=validation.any[basestring]) -%}
<<<<<<< HEAD
  {%- if type in ['text', 'json', 'arrow', 'parquet'] -%}
=======
  {%- if type in ['text', 'json', 'arrow', 'parquet', 'iceberg'] -%}
>>>>>>> e8b196d307d9e0471f88722c45fdb43ac33c63dc
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

<<<<<<< HEAD
=======
{% macro store_as_clause() -%}
  {%- set options = format_options() -%}
  {%- if options is not none -%}
  store as (
    {{ options }}
  )
  {%- endif -%}
{%- endmacro -%}

>>>>>>> e8b196d307d9e0471f88722c45fdb43ac33c63dc
{% macro render_with_format_options(target_table) %}
  {%- set options = format_options() -%}
  {% if options is not none -%}
    table(
  {%- endif %}
  {{ target_table }}
  {%- if options is not none -%}
    ( {{ options }} ))
  {%- endif -%}
{% endmacro %}
