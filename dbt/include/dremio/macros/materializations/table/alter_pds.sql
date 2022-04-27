{#
ALTER PDS <PHYSICAL-DATASET-PATH> REFRESH METADATA
    [AVOID PROMOTION | AUTO PROMOTION]
    [FORCE UPDATE | LAZY UPDATE]
    [MAINTAIN WHEN MISSING | DELETE WHEN MISSING]

ALTER PDS <PHYSICAL-DATASET-PATH> FORGET METADATA

ALTER TABLE <TABLE> REFRESH METADATA
#}

{% macro refresh_metadata(relation, format='iceberg') -%}
  {%- if format != 'iceberg' -%}
    {% call statement('refresh_metadata') -%}
      {%- if format == 'parquet' -%}
        {{ alter_table_refresh_metadata(relation) }}
      {%- else -%}
        {{ alter_pds(relation, avoid_promotion=false, lazy_update=false) }}
      {%- endif -%}
    {%- endcall %}
  {%- endif -%}
{%- endmacro -%}

{% macro alter_table_refresh_metadata(table_relation) -%}
  alter table {{ table_relation }} refresh metadata
{%- endmacro -%}

{% macro alter_pds(table_relation, avoid_promotion=True, lazy_update=True, delete_when_missing=True, forget_metadata=False) -%}
  alter pds {{ table_relation }} refresh metadata
  {% if forget_metadata %}
    forget metadata
  {%- else -%}
    {%- if avoid_promotion %}
      avoid promotion
    {%- else %}
      auto promotion
    {%- endif %}
    {%- if lazy_update %}
      lazy update
    {%- else %}
      force update
    {%- endif %}
    {%- if delete_when_missing %}
      delete when missing
    {%- else %}
      maintain when missing
    {%- endif -%}
  {%- endif %}
{%- endmacro -%}
