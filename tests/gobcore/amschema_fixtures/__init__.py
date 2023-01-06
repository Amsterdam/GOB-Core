"""Amsterdam Schema fixtures."""


import json
from pathlib import Path

from gobcore.model.amschema.model import Dataset, Table


def get_dataset(catalog):
    """Get dataset for AMS catalog."""
    path = Path(__file__).parent.joinpath(catalog, "dataset.json")
    with open(path, "r", encoding="utf-8") as f:
        return Dataset.parse_obj(json.load(f))


def get_table(catalog, collection):
    """Get table for AMS catalog collection."""
    path = Path(__file__).parent.joinpath(catalog, f"{collection}.json")
    with open(path, "r", encoding="utf-8") as f:
        return Table.parse_obj(json.load(f))
