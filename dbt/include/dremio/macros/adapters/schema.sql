{% macro dremio__create_schema(relation) -%}
  {{ log('create_schema macro (' + relation.render() + ') not implemented yet for adapter ' + adapter.type(), info=True) }}
{% endmacro %}

{% macro dremio__drop_schema(relation) -%}
{{ exceptions.raise_not_implemented(
  'drop_schema macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}
