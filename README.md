![dbt-dremio](https://www.dremio.com/img/blog/gnarly-wave-data-lake.png)

> *This project is developed during my spare time, along side my lead dev position at [MAIF-VIE](http://www.maif.fr), and aims to provide a competitive alternative solution for our current ETL stack.*

# dbt-dremio
[dbt](https://www.getdbt.com/)'s adapter for [dremio](https://www.dremio.com/)

If you are reading this documentation, I assume you already know well both dbt and dremio. Please refer to their respective documentation.

# Installation
dbt dependencies :
 - dbt-core>=0.18.0,
 - pyodbc>=4.0.27

dremio dependency :
 - latest dremio's odbc driver

os dependency :
- odbc (unixodbc-dev on linux)

`pip install dbt-dremio`

# Relation types
In dbt's world, A dremio's dataset can be either a `view` or a `table`. A dremio's reflection - a dataset's materialization with a refresh policy - will be mapped to a dbt's `materializedview`.

# Databases
As Dremio is a federation tool, dbt's queries can span locations and so, in dremio's adapter, databases are first class citizens.
There are two kinds of dataset's locations : sources and spaces. Sources are mostly input locations, and spaces output ones, with exceptions :

location|can create table| can drop table |can create/drop view
-|-|-|-
source|if CTAS (`CREATE TABLE AS`) is allowed on this source|if `DROP TABLE` is allowed on this source|no
space|only in the user's home space, and by manually uploading files in the UI|only in the UI|yes
distributed shared storage (`$scratch` source)|yes|yes|no

As you can see, using the SQL-DDL interface, the location's type implies the relation's type, so materialization's implementations do not have to take care of possible relation type mutations.

The UI allows dots in a space's name : **the adapter does not handle that correctly**.

# Schemas
In dremio, schemas are recursive, like filesystem folders : `dbt.internal."my very strange folder's name"`, and dots are not allowed in sub-folder's names. For each database, there is a root schema, known as `no_schema`by the adapter. So, in order to materialize a model at the root folder of the `track17`space, one will configure it as :

    +database: track17
    +schema: no_schema

**Please note that because dremio has no `CREATE SCHEMA` command yet, all schemas must be created before in the UI or via the API (and by the way dremio is loved by all oracle dbas)**

# Rendering of a relation

Because dremio accepts almost any string character in the objects' names, the adapter will double quote each part of the database.schema.identifier tryptic with the following rules concerning schema :

 - if schema is equal to `no_schema`, the schema will not be included, leading to a simple `"database"."identifier"` being rendered
 - if schema spans multiple folders, each folder's name will be double quoted, leading to `"database"."folder"."sub-folder"."sub-sub-folder"."identifier"`.

# Sources

## Environments

A same dremio installation could handle several data environments. In order to group sources by environment, you can use the undocumented `target.profile_name` or the adapter specific `environment` configuration to map environments between dremio and dbt :

 - dremio's side: prefix all the sources' names of a specific environment `prd` with the environment's name, for example : `prd_crm, prd_hr, prd_accounting`
 - dbt's side: prefix all source's database configs like this : `{{target.environment}}_crm` or `{{target.profile_name}}_crm`

That way you can configure seperately input sources and output target `database`.

# Materializations

## Dremio's SQL specificities

Given that :

- tables and views cannot coexist in the same schema
- there are no transactions (at DDL level),
- there is no `ALTER TABLE|VIEW RENAME TO`command
- you can `CREATE OR REPLACE` a view, but only `CREATE` a table,
- data can only be added in tables with a CTAS,

I tried to keep things secure setting up a kind of logical interface between the dbt model and its implementation in dremio. So :

 - almost all materializations create a view as interface, so they could coexist in a same space,
 - each new version of the model's underlying data is first stored in a new table, and then referenced atomically (via a `CREATE OR REPLACE VIEW`) by the interface view. The table containing the old version of the underlying data can then be dropped : a kind of pedantic blue/green deployement at model's level.
 - the coexistence of old and new data versions helps overcoming the lack of SQL-DML commands, see for example the `incremental` implementation.

> This could change in near future, as Dremio's CEO Tomer Shiran posted in discourse that [Apache Iceberg](https://iceberg.apache.org/) could be included by the end of the year, bringing INSERT [OVERWRITE] to dremio, challenging a well known cloud datawarehouse in the same temperatures...

## Seed

adapter's specific configuration|type|required|default
-|-|-|-
materialization_database|CTAS/DROP TABLE allowed source's name|no|`$scratch`
materialization_schema||no|`target.environment` (we don't want the environments to share the same underlying table)

    CREATE TABLE AS
    SELECT *
    FROM VALUES()[,()]

As dremio does not support query's bindings, the python value is converted as string, quoted and casted in the sql type.
## View

    CREATE OR REPLACE VIEW AS
    {{ sql }}

## Table

adapter's specific configuration|type|required|default
-|-|-|-
materialization_database|CTAS/DROP TABLE allowed source's name|no|`$scratch`
materialization_schema||no|`target.environment`
partition| the list of partitioning columns|no|
sort| the list of sorting columns|no|

    CREATE TABLE [HASH PARTITION BY] [LOCALSORT BY] AS
    {{ sq }}

## Incremental

adapter's specific configuration|type|required|default
-|-|-|-
materialization_database|CTAS/DROP TABLE allowed source's name|no|`$scratch`
materialization_schema||no|`target.environment`
partition| the list of partitioning columns|no|
sort| the list of sorting columns|no|

As we still have the old data when new data is created, the new table is filled with :

    {% if full_refresh_mode or old_relation is none %}
	    {% set build_sql = sql %}
    {% else %}
	    {% set build_sql %}
		    with increment as (
			    {{ sql }}
		    )
		    select *
		    from increment
		    union all
		    select *
		    from {{ old_table }}
		    {%- if unique_key is not none %}
			where {{ unique_key }} not in (
			    select {{ unique_key }}
			    from increment
			    )
		    {% endif %}
		{% endset %}
    {% endif %}

## Raw Reflection
A raw reflection is a dataset's materialization, with a refresh policy, handled internally by dremio.

adapter's specific configuration|type|required|default
-|-|-|-
dataset|the related model's name|yes|
display| the list of columns|no|all columns
partition| the list of partitioning columns|no|
sort| the list of sorting columns|no|
distribute| the list of distributing columns|no|

## Aggregate Reflection

An aggregate reflection is a dataset's materialization, containing pre aggregated measures on dimensions, with a refresh policy, handled internally by dremio.

adapter's specific configuration|type|required|default
-|-|-|-
dataset|the related model's name|yes|
dimensions| the list of dimension columns|no|all columns whom type is not in 'float', 'double' and 'decimal'
measures| the list of measurement columns|no|all columns whom type is in 'float', 'double' and 'decimal'
partition| the list of partitioning columns|no|
sort| the list of sorting columns|no|
distribute| the list of distributing columns|no|

## File

This materialization creates a table without a view interface. It's an easy way to automate the export of a dataset (in parquet format).

# Connection
Be careful to provide the right odbc driver's name in the adapter specific `driver` attribute, the one you gave to your dremio's odbc driver installation.

## Managed or unmanaged target
Thanks to [Ronald Damhof's article](https://prudenza.typepad.com/files/english---the-data-quadrant-model-interview-ronald-damhof.pdf), I wanted to have a clear separation between managed environments (prod, preprod...) and unmanaged ones (developers' environments). So there are two distinct targets : managed and unmanaged.

In an unmanaged environment, if no target database is provided, all models are materialized in the user's home space, under the target schema.

In a managed environment, target and custom databases and schemas are used as usual. If no target database is provided,  `target.environment` will be used as the default value.

You will find in [the macros' directory](https://github.com/fabrice-etanchaud/dbt-dremio/tree/master/dbt/include/dremio/macros) an environment aware implementation for custom database and schema names.


    track17:
      outputs:
        unmanaged:
          type: dremio
          threads: 2
          driver: Dremio ODBC Driver 64-bit
          host: veniseverte.fr
          port: 31010
          environment: track17
          schema: dbt
          user: fabrice_etanchaud
          password: fabricesecretpassword
        managed:
          type: dremio
          threads: 2
          driver: Dremio ODBC Driver 64-bit
          host: veniseprovencale.fr
          port: 31010
          environment: track17
          database: '@dremio'
          schema: no_schema
          user: dremio
          password: dremiosecretpassword
      target: unmanaged
