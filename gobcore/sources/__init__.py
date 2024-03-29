"""GOB Relations."""


import os
from collections import defaultdict
from typing import NewType, Optional

from gobcore.model import GOBModel
from gobcore.parse import json_to_cached_dict
from gobcore.typing import CatalogName, CollectionName

ReferenceName = NewType("ReferenceName", str)
RelationType = dict[str, str]
RelationListType = list[RelationType]

SourceName = NewType("SourceName", str)
SourceType = NewType("SourceType", dict[CatalogName, dict[CollectionName, dict[ReferenceName, RelationType]]])
GobSourcesType = NewType("GobSourcesType", dict[SourceName, SourceType])


class GOBSources:
    """GOB relations."""

    def __init__(self, model: GOBModel) -> None:
        """Initialise GOBSources.

        :param model: GOBModel instance
        """
        data: GobSourcesType = json_to_cached_dict(os.path.join(os.path.dirname(__file__), "gobsources.json"))
        self.model = model

        self._relations: defaultdict[str, defaultdict[str, RelationListType]] = defaultdict(lambda: defaultdict(list))

        # Extract references for easy access.
        for source_name, source in data.items():
            self._extract_relations(source_name, source)

    def _extract_relations(self, source_name: SourceName, source: SourceType) -> None:
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

    def _get_field_relation(
        self,
        source: SourceType,
        catalog_name: CatalogName,
        collection_name: CollectionName,
        field_name: ReferenceName,
    ) -> Optional[dict[str, str]]:
        try:
            relation = source[catalog_name][collection_name][field_name]
        except KeyError:
            return {}
        else:
            return relation

    def get_field_relations(
        self, catalog_name: CatalogName, collection_name: CollectionName, field_name: ReferenceName
    ) -> RelationListType:
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

    def get_relations(self, catalog_name: CatalogName, collection_name: CollectionName) -> RelationListType:
        """Return all the relations for the given catalog - collection."""
        return self._relations[catalog_name][collection_name]
