"""Parse JSON files."""


import json


def json_to_dict(json_path):
    """Parse JSON file into a dict."""
    with open(json_path, encoding="utf-8") as json_file:
        data = json.load(json_file)
    return data
