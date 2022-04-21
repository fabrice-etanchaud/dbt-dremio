{% macro dremio__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {% set tmp_relation = base_relation.incorporate(
                                path={"identifier": tmp_identifier}) -%}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro dremio__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro materialization_to_relation_type(materialization) -%}
  {{ return('view' if materialization == 'view'
  else 'materializedview' if materialization in ['raw_reflection', 'aggregation_reflection']
  else 'table') }}
{%- endmacro %}
