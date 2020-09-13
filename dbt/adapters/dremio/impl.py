from dbt.adapters.sql import SQLAdapter
from dbt.adapters.dremio import DremioConnectionManager
from dbt.adapters.dremio.relation import DremioRelation

from typing import List
from typing import Optional
import dbt.flags
from dbt.adapters.base.relation import BaseRelation
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.adapters.base.meta import available

import agate

class DremioAdapter(SQLAdapter):
    ConnectionManager = DremioConnectionManager
    Relation = DremioRelation

    @classmethod
    def date_function(cls):
        return 'current_date'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "varchar"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "timestamp"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "date"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "boolean"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "decimal" if decimals else "bigint"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time"

    def list_relations(
        self, database: Optional[str], schema: str
    ) -> List[BaseRelation]:
        if self._schema_is_cached(database, schema):
            return self.cache.get_relations(database, schema)

        schema_relation = self.Relation.create(
            database=database,
            schema=schema,
            identifier='',
            quote_policy=self.config.quoting
        ).without_identifier()

        # we can't build the relations cache because we don't have a
        # manifest so we can't run any operations.
        relations = self.list_relations_without_caching(
            schema_relation
        )

        logger.debug('with database={}, schema={}, relations={}'
                     .format(database, schema, relations))
        return relations


    @available
    def cache_added(self, relation: Optional[BaseRelation]) -> str:
        """Cache a new relation in dbt. It will show up in `list relations`."""

        if relation is None:
            name = self.nice_connection_name()
            raise_compiler_error(
                'Attempted to cache a null relation for {}'.format(name)
            )
        if dbt.flags.USE_CACHE:
            self.cache.add(relation)
        # so jinja doesn't render things
        return ''
