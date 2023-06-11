from pydash import snake_case

from gobcore.model.amschema.model import Dataset, Table
from gobcore.model.amschema.repo import AMSchemaRepository
from gobcore.model.pydantic import Schema


class LoadSchemaException(Exception):
    pass


def _get_entity_id_from_amschema(table: Table) -> str:
    """Return entity_id for the given table (collection).

    :param table:
    """
    if isinstance(table.schema_.identifier, list):
        identifiers = table.schema_.identifier.copy()
        if table.temporal:
            identifiers.remove(table.temporal.identifier)
        if len(identifiers) != 1:
            raise LoadSchemaException("Please set entity_id explicitly, because AMS Schema identifier is ambiguous")
        return identifiers[0]
    return table.schema_.identifier


def load_schema(schema: Schema):
    """Load JSON Schema for the given catalog and collection.

    :param schema:
    """
    table, dataset = AMSchemaRepository().get_schema(schema)

    if not schema.entity_id:
        identifier = _get_entity_id_from_amschema(table)

    # Convert to GOBModel format
    return {
        "attributes": _to_gob(table, dataset),
        "entity_id": schema.entity_id or identifier,
        "has_states": bool(table.temporal),
        "version": f"ams_{table.version}"
    }


def _to_gob(table: Table, dataset: Dataset):
    """Transform the given table schema (spec) to a GOB collection model.

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
