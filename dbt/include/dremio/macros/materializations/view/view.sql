{% materialization view, adapter='dremio' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
