from gobcore.model.amschema.model import Dataset, Table
from gobcore.model.amschema.repo import AMSchemaRepository
from gobcore.model.pydantic import Schema
from pydash import snake_case


class LoadSchemaException(Exception):
    pass


def load_schema(schema: Schema):
    """
    Load json schema for the given catalog and connection

    :param schema:
    """
    table, dataset = AMSchemaRepository().get_schema(schema)

    if isinstance(table.schema_.identifier, list):
        if not schema.entity_id:
            raise LoadSchemaException("Please set entity_id explicitly, because AMS Schema identifier is of type list")

    # Convert to GOBModel format
    return {
        "attributes": _to_gob(table, dataset),
        "entity_id": schema.entity_id or table.schema_.identifier,
        "version": f"ams_{table.version}"
    }


def _to_gob(table: Table, dataset: Dataset):
    """
    Transforms the given schema (spec) to a GOB model

    :param table:
    :param dataset:
    :return:
    """

    model = {}

    for key, prop in table.schema_.properties.items():
        if key == "schema":
            continue
        model[snake_case(key)] = prop.gob_representation(dataset)

    return model
