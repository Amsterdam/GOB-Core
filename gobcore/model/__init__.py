import os
import json


def get_gobmodel():
    path = os.path.join(os.path.dirname(__file__), 'gobmodel.json')
    with open(path) as file:
        data = json.load(file)
    return data
