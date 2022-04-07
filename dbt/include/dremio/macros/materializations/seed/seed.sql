{% materialization seed, adapter='dremio' %}
  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}
  {%- set num_rows = (agate_table.rows | length) -%}
  {%- set sql = select_csv_rows(model, agate_table) -%}
  {%- set result = common_table(sql, 'seed') -%}
  {% call noop_statement('main', 'CREATE ' ~ num_rows, 'CREATE', num_rows) %}
    {{ sql }}
  {% endcall %}
  {{ return(result) }}
{% endmaterialization %}
