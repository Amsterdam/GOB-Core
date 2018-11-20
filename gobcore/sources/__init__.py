import json
import os

from collections import defaultdict

from gobcore.model import GOBModel


class GOBSources():
    def __init__(self):
        path = os.path.join(os.path.dirname(__file__), 'gobsources.json')
        with open(path) as file:
            data = json.load(file)

        self._data = data
        self._model = GOBModel()

        self._relations = {}

        # Extract references for easy access in API
        for source_name, source in self._data.items():
            self._relations.update(self._extract_relations(source_name, source))

    def _extract_relations(self, source_name, source):
        relations = defaultdict(lambda: defaultdict(list))
        for catalog_name, catalog in self._model.get_catalogs().items():
            for collection_name, collection in catalog['collections'].items():
                for field_name, spec in collection['references'].items():
                    relation = {
                        'source': source_name,
                        'catalog': catalog_name,
                        'collection': collection_name,
                        'field_name': field_name
                    }

                    relation.update(
                        self._get_field_relation(
                            source,
                            catalog_name,
                            collection_name,
                            field_name
                        )
                    )
                    ref_catalog, ref_collection = spec['ref'].split(':')

                    relations[ref_catalog][ref_collection].append(relation)
        return relations

    def _get_field_relation(self, source, catalog_name, collection_name, field_name):
        try:
            relation = source[catalog_name][collection_name][field_name]
        except KeyError:
            return {}
        else:
            return relation

    def get_relations(self, catalog_name, collection_name):

        return self._relations[catalog_name][collection_name]
