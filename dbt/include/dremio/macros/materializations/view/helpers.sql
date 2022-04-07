{% macro default__handle_existing_table(full_refresh, old_relation) %}
    {{ log("Dropping relation " ~ old_relation ~ " because it is of type " ~ old_relation.type) }}
    {{ exceptions.raise_not_implemented('Inside a dremio home space, a model cannot change from table to view materialization; please drop the table in the UI') }}
{% endmacro %}
