# dbt-dremio
[dbt](https://www.getdbt.com/)'s adapter for [dremio](https://www.dremio.com/)

If you are reading this documentation, I assume you already know well both dbt and dremio. Please refer to the respective documentation. 

# Installation
There is no package yet, you can clone the repository, cd in it, and then type pip3 install -e .

# Relation types
A dremio's relation can be a view or a table. A reflection is a special kind of table : a view's materialization with a refresh policy.

# Databases
As Dremio is a federation tool, dbt's queries can span locations and so, in dremio's adapter, databases are first class citizens.
There are two kinds of dataset's locations : sources and spaces. Sources are mostly input locations, and spaces output ones, with exceptions :

location|can create table| can drop table |can create/drop view
-|-|-|-
source|if CTAS (`CREATE TABLE AS`) is allowed on this source|if `DROP TABLE` is allowed on this source|no
space|only in the user's home space, by uploading files in the UI|only in the UI|yes
distributed shared storage (`$scratch` source)|yes|yes|no

As you can see, using the SQL-DDL interface, the location's type implies the relation's type, so materialization's implementations do not have to take care of possible relation type mutations.

The UI allows dots in a space's name : **the adapter does not handle that correctly**.

# Schemas
In dremio, schemas are recursive, like filesystem folders : `dbt.internal."my very strange folder's name"`, and dots are not allowed in sub-folder's names. For each database, there is a root schema, known as `no_schema`by the adapter. So, in order to materialize a model at the root folder of the `track17`space, one will configure it as :

    +database: track17
    +schema: no_schema

**Please note that because dremio has no CREATE SCHEMA command yet, all schemas must be created before in the UI or via the API**

# Rendering of a relation

Because dremio accepts almost any string character in the objects' names, the adapter will double quote each part of the database.schema.identifier tryptic with the following rules concerning schema :

 - if schema is equal to `no_schema`, the schema will not be included, leading to a simple `"database"."identifier"` being rendered
 - if schema spans multiple folders, each folder's name will be double quoted, leading to `"database"."folder"."sub-folder"."sub-sub-folder"."identifier"`.

# Materializations

## Dremio's SQL specificities

Given that :

- tables and views cannot coexist in the same schema
- there are no transactions (at DDL level),
- you can `CREATE OR REPLACE` a view, but only `CREATE` a table,
- data can only be added in tables with a CTAS,

I tried to keep things secure setting up a kind of logical interface between the dbt model and its implementation in dremio. So :

 - every materialization (except `file`)  has a view as interface, so all kind of materializations could coexist in a same schema,
 - each new version of the model's data is first stored in a new `$scratch` table, and then referenced atomically (via `CREATE OR REPLACE VIEW`) by the interface view. The table containing the old version of the data can then be dropped : a kind of pedantic blue/green deployement at model's level.
 - the coexistence of old and new data versions help overcoming the lack of SQL-DML commands, see for example the `incremental` implementation.

> This could change in near future, as Dremio's CEO Tomer Shiran posted in discourse that [Apache Iceberg](https://iceberg.apache.org/) could be included by the end of the year, bringing INSERT [OVERWRITE] to dremio, challenging a well known cloud datawarehouse in the same temperatures...

## Seeds

    CREATE TABLE AS 
    SELECT * 
    FROM VALUES()[,()]

As dremio does not support query's bindings, the python value is converted as string, quoted and casted in the sql type.
## View

    CREATE OR REPLACE VIEW AS
    {{ sql }}

## Table

    CREATE TABLE AS
    {{ sq }}
     
## Incremental
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

## File

This materialization creates a table without a view interface. It's an easy way to automated the export of a dataset (in parquet format).
