<<<<<<< HEAD
{% macro dremio__handle_existing_table(full_refresh, old_relation) %}
=======
{% macro default__handle_existing_table(full_refresh, old_relation) %}
>>>>>>> e8b196d307d9e0471f88722c45fdb43ac33c63dc
    {{ log("Dropping relation " ~ old_relation ~ " because it is of type " ~ old_relation.type) }}
    {{ exceptions.raise_not_implemented('Inside a dremio home space, a model cannot change from table to view materialization; please drop the table in the UI') }}
{% endmacro %}
