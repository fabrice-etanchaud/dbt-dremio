{% macro drop_reflection_if_exists(relation, reflection) %}
  {% if reflection is not none and reflection.type == 'materializedview' %}
    {% call statement('drop reflection') -%}
      alter dataset {{ relation }}
        drop reflection {{ reflection.include(database=False, schema=False) }}
    {%- endcall %}
  {% endif %}
{% endmacro %}

{% macro dbt_dremio_validate_get_reflection_type(raw_reflection_type) %}
  {% set accepted_types = ['raw', 'aggregate', 'external'] %}
  {% set invalid_reflection_type_msg -%}
    Invalid reflection type provided: {{ raw_reflection_type }}
    Expected one of: {{ accepted_types | join(', ') }}
  {%- endset %}
  {% if raw_reflection_type not in accepted_types %}
    {% do exceptions.raise_compiler_error(invalid_reflection_type_msg) %}
  {% endif %}
  {% do return(raw_reflection_type) %}
{% endmacro %}
