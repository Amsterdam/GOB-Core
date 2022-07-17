import json
from pathlib import Path

from gobcore.model.amschema.model import Dataset, Table


def get_dataset():
    path = Path(__file__).parent.joinpath("dataset.json")
    with open(path, 'r') as f:
        return Dataset.parse_obj(json.load(f))


def get_table():
    path = Path(__file__).parent.joinpath("table.json")
    with open(path, 'r') as f:
        return Table.parse_obj(json.load(f))
