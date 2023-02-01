"""GOB Relations."""


import os
from collections import defaultdict

from gobcore.model import GOBModel
from gobcore.parse import json_to_cached_dict

RelationType = dict[str, str]
RelationListType = list[RelationType]


class GOBSources:
    """GOB relations."""

    def __init__(self, model: GOBModel):
        """Initialise GOBSources.

        :param model: GOBModel instance
        """
        data = json_to_cached_dict(os.path.join(os.path.dirname(__file__), "gobsources.json"))
        self.model = model

        self._relations: defaultdict[str, defaultdict[str, RelationListType]] = defaultdict(lambda: defaultdict(list))

        # Extract references for easy access.
        for source_name, source in data.items():
            self._extract_relations(source_name, source)

    def _extract_relations(self, source_name: str, source):
        for catalog_name, catalog in self.model.items():
            for collection_name, collection in catalog["collections"].items():
                for field_name, spec in collection["references"].items():
                    field_relation = self._get_field_relation(source, catalog_name, collection_name, field_name)
                    if field_relation:
                        relation = {
                            "source": source_name,
                            "catalog": catalog_name,
                            "collection": collection_name,
                            "field_name": field_name,
                            "type": spec["type"],
                            **field_relation,
                        }
                        # Store the relation for the catalog - collection
                        self._relations[catalog_name][collection_name].append(relation)

    def _get_field_relation(self, source, catalog_name: str, collection_name: str, field_name: str):
        try:
            relation = source[catalog_name][collection_name][field_name]
        except KeyError:
            return {}
        else:
            return relation

    def get_field_relations(self, catalog_name: str, collection_name: str, field_name: str) -> RelationListType:
        """Get all the relations for the specified field in the given catalog - collection.

        Note that more field relations may exist because multiple applications may
        be source for the field.

        :param catalog_name:
        :param collection_name:
        :param field_name:
        :return:
        """
        try:
            relations = self.get_relations(catalog_name, collection_name)
            return [relation for relation in relations if relation["field_name"] == field_name]
        except KeyError:
            return []

    def get_relations(self, catalog_name: str, collection_name: str) -> RelationListType:
        """Return all the relations for the given catalog - collection."""
        return self._relations[catalog_name][collection_name]
