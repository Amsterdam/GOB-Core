import os
import json


class GOBModel():
    def __init__(self):
        path = os.path.join(os.path.dirname(__file__), 'gobmodel.json')
        with open(path) as file:
            data = json.load(file)
        self._data = data

        # Add fields to the GOBModel to be used in database creation and lookups
        self._add_fields()

    def _add_fields(self):
        for entity_name, model in self._data.items():
            self._data[entity_name]['fields'] = self._expand_references(model['attributes'])

    def _expand_references(self, attributes):
        fields = {}
        for field_name, attributes in attributes.items():
            if 'ref' in attributes:
                ref = attributes.pop('ref', None)
                # For references add the _text and _id fields with type GOB.String
                fields[f'{field_name}_text'] = attributes
                fields[f'{field_name}_id'] = {
                    'type': attributes['type'],
                    'description': f"Reference ID to {ref}"
                }
            else:
                fields[field_name] = attributes
        return fields

    def get_model(self, name):
        return self._data[name]

    def get_model_fields(self, name):
        return self._data[name]['fields']

    def get_model_names(self):
        return [k for k, v in self._data.items()]
