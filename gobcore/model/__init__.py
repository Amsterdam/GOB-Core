import os
import json


class GOBModel():
    def __init__(self):
        path = os.path.join(os.path.dirname(__file__), 'gobmodel.json')
        with open(path) as file:
            data = json.load(file)
        self._data = data

    def get_model(self, name):
        return self._data[name]

    def get_model_names(self):
        return [k for k, v in self._data.items()]
