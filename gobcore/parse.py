"""Parse JSON files."""


import json
from functools import cache


@cache
def json_to_cached_dict(json_path):
    """Parse JSON file into a cached dict."""
    with open(json_path, encoding="utf-8") as json_file:
        data = json.load(json_file)
    return data
