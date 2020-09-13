from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy, ComponentName, RelationType
from typing import Optional, Tuple, Iterator

@dataclass
class DremioQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True

@dataclass
class DremioIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True

@dataclass(frozen=True, eq=False, repr=False)
class DremioRelation(BaseRelation):
    quote_policy: DremioQuotePolicy = DremioQuotePolicy()
    include_policy: DremioIncludePolicy = DremioIncludePolicy()
    quote_character: str = '"'
    no_schema = 'no_schema'

    def _render_iterator(
        self
    ) -> Iterator[Tuple[Optional[ComponentName], Optional[str]]]:

        for key in ComponentName:
            path_part: Optional[str] = None
            if self.include_policy.get_part(key):
                tmp_path_part = self.path.get_part(key)
                if not (key == ComponentName.Schema and tmp_path_part == self.no_schema):
                    path_part = tmp_path_part
                if path_part is not None and (self.quote_policy.get_part(key)): # or key == ComponentName.Schema):
                    path_part = self.quoted_by_component(path_part, key)
            yield key, path_part

    def quoted_by_component(self, identifier, componentName):
        if componentName == ComponentName.Schema:
            return '.'.join(
                self.quoted(folder) for folder in identifier.split('.')
            )
        else:
            return self.quoted(identifier)
