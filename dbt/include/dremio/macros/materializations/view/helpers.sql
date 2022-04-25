{% macro dremio__handle_existing_table(full_refresh, old_relation) %}
    {{ log("Dropping relation " ~ old_relation ~ " because it is of type " ~ old_relation.type) }}
    {{ exceptions.raise_not_implemented('Inside a dremio home space, a model cannot change from table to view materialization; please drop the table in the UI') }}
{% endmacro %}

{# ALTER VDS <dataset> SET ENABLE_DEFAULT_REFLECTION = TRUE | FALSE #}

{% macro enable_default_reflection() %}
  {%- set enable_default_reflection = config.get('enable_default_reflection', validator=validation.any[boolean]) -%}
  {%- if enable_default_reflection is not none -%}
    {% call statement('enable_default_reflection') -%}
      alter vds {{ this }} set enable_default_reflection = {{ enable_default_reflection }}
    {%- endcall %}
  {%- endif -%}
{% endmacro %}
