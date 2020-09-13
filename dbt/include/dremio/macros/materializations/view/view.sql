{% materialization view, adapter='dremio' -%}
    {{ return(dremio_create_or_replace_view()) }}
{%- endmaterialization %}
