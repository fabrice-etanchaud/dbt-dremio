{% macro select_csv_rows(model, agate_table) %}
{%- set column_override = model['config'].get('column_types', {}) -%}
{%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
{%- set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) -%}
  select
    {% for col_name in agate_table.column_names -%}
      {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
      {%- set type = column_override.get(col_name, inferred_type) -%}
      {%- set column_name = (col_name | string) -%}
      cast({{ adapter.quote_seed_column(column_name, quote_seed_column) }} as {{ type }})
        as {{ adapter.quote_seed_column(column_name, quote_seed_column) }}{%- if not loop.last -%}, {%- endif -%}
    {% endfor %}
  from
    (values
      {% for row in agate_table.rows %}
        ({%- for value in row -%}
          {% if value is not none %}
            {{ "'" ~ (value | string | replace("'", "''")) ~ "'" }}
          {% else %}
            cast(null as varchar)
          {% endif %}
          {%- if not loop.last%},{%- endif %}
        {%- endfor -%})
        {%- if not loop.last%},{%- endif %}
      {% endfor %}) temp_table ( {{ cols_sql }} )
{% endmacro %}
