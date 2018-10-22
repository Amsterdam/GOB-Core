import os
import json


class GOBViews():
    def __init__(self):
        path = os.path.join(os.path.dirname(__file__), 'gobviews.json')
        with open(path) as file:
            data = json.load(file)
        self._data = data

    def get_catalogs(self):
        return list(self._data.keys())

    def get_entities(self, catalog_name):
        return list(self._data[catalog_name].keys())

    def get_views(self, catalog_name, entity_name):
        return self._data[catalog_name][entity_name]
