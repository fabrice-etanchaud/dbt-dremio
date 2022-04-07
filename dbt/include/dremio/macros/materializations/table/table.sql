{% materialization table, adapter = 'dremio' %}

  {{ return(common_table(sql)) }}

{% endmaterialization %}
