{% macro config_cols(label, default_cols=none) %}
  {%- set cols = config.get(label | replace(" ", "_"), validator=validation.any[list, basestring]) or default_cols -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ adapter.quote(item) }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}

{% macro partition_method() %}
  {%- set method = config.get('partition_method', validator=validation.any[basestring]) -%}
  {%- if method is not none -%}
   {{ method }}
  {%- endif %}
{%- endmacro -%}

{%- macro join_using(left_table, right_table, left_columns, right_columns=none) -%}
  {%- for column_name in left_columns -%}
    {{ left_table }}.{{ column_name }} = {{ right_table }}.{{ right_columns[loop.index0] if right_columns else column_name }}
    {% if not loop.last %} and {% endif -%}
  {%- endfor -%}
{%- endmacro -%}
