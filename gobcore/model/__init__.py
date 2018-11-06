import os
import json


class GOBModel():
    def __init__(self):
        path = os.path.join(os.path.dirname(__file__), 'gobmodel.json')
        with open(path) as file:
            data = json.load(file)
        self._data = data

        # Extract references for easy access in API
        for entity_name, model in self._data.items():
            self._data[entity_name]['references'] = self._extract_references(model['attributes'])

            # Add fields to the GOBModel to be used in database creation and lookups
            self._data[entity_name]['fields'] = {field_name: attributes
                                                 for field_name, attributes in model['attributes'].items()}

    def _extract_references(self, attributes):
        return {field_name: spec for field_name, spec in attributes.items()
                if spec['type'] in ['GOB.Reference', 'GOB.ManyReference']}

    def get_model(self, name):
        return self._data[name]

    def get_model_fields(self, name):
        return self._data[name]['fields']

    def get_model_names(self):
        return [k for k, v in self._data.items()]
