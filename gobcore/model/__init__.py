import os
import json


class GOBModel():
    def __init__(self):
        path = os.path.join(os.path.dirname(__file__), 'gobmodel.json')
        with open(path) as file:
            data = json.load(file)

        if os.getenv('DISABLE_TEST_CATALOGUE', True):
            # Default is to include the test catalogue
            # By setting the DISABLE_TEST_CATALOGUE environment variable
            # the test catalogue can be removed
            del data["test_catalogue"]

        self._data = data

        # Extract references for easy access in API
        for catalog_name, catalog in self._data.items():
            for entity_name, model in catalog['collections'].items():
                model['references'] = self._extract_references(model['attributes'])

                # Add fields to the GOBModel to be used in database creation and lookups
                model['fields'] = {
                    field_name: attributes for field_name, attributes in model['attributes'].items()
                }

    def _extract_references(self, attributes):
        return {field_name: spec for field_name, spec in attributes.items()
                if spec['type'] in ['GOB.Reference', 'GOB.ManyReference']}

    def get_catalog_names(self):
        return self._data.keys()

    def get_catalogs(self):
        return self._data

    def get_catalog(self, catalog_name):
        return self._data[catalog_name] if catalog_name in self._data else None

    def get_collection_names(self, catalog_name):
        catalog = self.get_catalog(catalog_name)
        return catalog['collections'].keys() if 'collections' in catalog else None

    def get_collections(self, catalog_name):
        catalog = self.get_catalog(catalog_name)
        return catalog['collections'] if 'collections' in catalog else None

    def get_collection(self, catalog_name, collection_name):
        collections = self.get_collections(catalog_name)
        return collections[collection_name] if collection_name in collections else None

    def get_table_names(self):
        '''
        Helper function to generate all table names
        '''
        table_names = []
        for catalog_name in self.get_catalog_names():
            for collection_name in self.get_collection_names(catalog_name):
                table_names.append(self.get_table_name(catalog_name, collection_name))
        return table_names

    def get_table_name(self, catalog_name, collection_name):
        return f'{catalog_name}_{collection_name}'
