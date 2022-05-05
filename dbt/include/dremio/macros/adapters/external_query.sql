{%- macro external_query(sql) -%}
  {%- set source = validate_external_query() -%}
  {%- if source is not none -%}
    {%- set escaped_sql = sql | replace("'", "''") -%}
    {%- set result -%}
      select *
      from table({{ builtins.source(source[0], source[1]).include(schema=false, identifier=false) }}.external_query('{{ escaped_sql }}'))
    {%- endset -%}
  {%- else -%}
    {%- set result = sql -%}
  {%- endif -%}
  {{ return(result) }}
{%- endmacro -%}

{%- macro validate_external_query() -%}
  {%- set external_query = config.get('external_query', validator=validation.any[boolean]) or false -%}
  {%- if external_query -%}
    {%- if model.refs | length == 0 and model.sources | length > 0 -%}
      {%- set source_names = [] -%}
      {%- for source in model.sources -%}
        {%- do source_names.append(source[0]) if source[0] not in source_names -%}
      {% endfor %}
      {%- if source_names | length == 1 -%}
        {{ return(model.sources[0]) }}
      {%- else -%}
        {% do exceptions.raise_compiler_error("Invalid external query configuration: awaiting one single source name among all source dependencies") %}
      {%- endif -%}
    {%- else -%}
      {% do exceptions.raise_compiler_error("Invalid external query: awaiting only source dependencies") %}
    {%- endif -%}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}
{%- endmacro -%}
