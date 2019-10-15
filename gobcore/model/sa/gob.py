"""GOB

SQLAlchemy GOB Models

"""
import re

from sqlalchemy.ext.declarative import declarative_base

# Import data definitions
from gobcore.model import GOBModel, EVENTS, EVENTS_DESCRIPTION
from gobcore.model.metadata import FIELD
from gobcore.model.metadata import columns_to_fields
from gobcore.sources import GOBSources
from gobcore.typesystem import get_gob_type, is_gob_geo_type, is_gob_json_type

Base = declarative_base()

model = GOBModel()
sources = GOBSources()


def get_column(column_name, column_specification):
    """Utility method to convert GOB type to a SQLAlchemy Column

    Get the SQLAlchemy columndefinition for the gob type as exposed by the gob_type

    :param column: (name, type)
    :return: sqlalchemy.Column
    """
    gob_type = get_gob_type(column_specification["type"])
    column = gob_type.get_column_definition(column_name)
    column.doc = column_specification["description"]
    return column


def columns_to_model(table_name, columns, has_states=False):
    """Create a model out of table_name and GOB column specification

    :param table_name: name of the table
    :param columns: GOB column specification
    :param has_states:
    :return: Model class
    """
    # Convert columns to SQLAlchemy Columns
    columns = {column_name: get_column(column_name, column_spec) for column_name, column_spec in columns.items()}

    # Create model
    return type(table_name, (Base,), {
        "__tablename__": table_name,
        **columns,
        "__has_states__": has_states,
        "__repr__": lambda self: f"{table_name}"
    })


def _derive_models():
    """Derive Models from GOB model specification

    :return: None
    """

    models = {
        # e.g. "meetbouten_rollagen": <BASE>
    }

    # Start with events
    columns_to_model("events", columns_to_fields(EVENTS, EVENTS_DESCRIPTION))

    for catalog_name, catalog in model.get_catalogs().items():
        for collection_name, collection in model.get_collections(catalog_name).items():
            # "attributes": {
            #     "attribute_name": {
            #         "type": "GOB type name, e.g. GOB.String",
            #         "description": "attribute description",
            #         "ref: "collection_name:entity_name, e.g. meetbouten:meetbouten"
            #     }, ...
            # }

            # the GOB model for the specified entity
            table_name = model.get_table_name(catalog_name, collection_name)
            models[table_name] = columns_to_model(table_name, collection['all_fields'],
                                                  has_states=collection.get('has_states', False))

    return models


def _remove_leading_underscore(s: str):
    return re.sub(r'^_', '', s)


def _default_indexes_for_columns(input_columns: list) -> dict:
    """Returns applicable indexes for table with input_columns

    :param input_columns:
    :return:
    """
    default_indexes = [
        (FIELD.SOURCE, FIELD.SOURCE_ID,),
        (FIELD.SEQNR,),
        (FIELD.APPLICATION,),
        (FIELD.ID,),
        (FIELD.GOBID,),
        (FIELD.SOURCE_ID,),
        (FIELD.START_VALIDITY,),
        (FIELD.END_VALIDITY,),
        (FIELD.DATE_DELETED,),
        (FIELD.EXPIRATION_DATE,),
        (f"src{FIELD.ID}",),
        (f"src_{FIELD.SEQNR}",),
        (f"dst{FIELD.ID}",),
        (f"dst_{FIELD.SEQNR}",),
        (FIELD.SOURCE_VALUE,),
    ]
    result = {}
    for columns in default_indexes:
        # Check if all columns defined in index are present
        if all([column in input_columns for column in columns]):
            idx_name = "_".join([_remove_leading_underscore(column) for column in columns])
            result[idx_name] = columns
    return result


def _get_special_column_type(column_type: str):
    """Returns special column type such as 'geo' or 'json', or else None

    :param column_type:
    :return:
    """
    if is_gob_geo_type(column_type):
        return "geo"
    elif is_gob_json_type(column_type):
        return "json"
    else:
        return None


def _relation_indexes_for_collection(catalog_name, collection_name, collection):
    indexes = {}
    table_name = model.get_table_name(catalog_name, collection_name)

    reference_columns = {column: desc['ref'] for column, desc in collection['all_fields'].items() if
                         desc['type'] in ['GOB.Reference', 'GOB.ManyReference']}

    # Search source and destination attributes for relation and define index
    for col, ref in reference_columns.items():
        dst_index_table = model.get_table_name_from_ref(ref)
        dst_collection = model.get_collection_from_ref(ref)
        relations = sources.get_field_relations(catalog_name, collection_name, col)

        for relation in relations:
            src_index_col = f"{relation['source_attribute'] if 'source_attribute' in relation else col}"

            # Source column
            name = f'{table_name}.idx.{_remove_leading_underscore(src_index_col)}'
            indexes[name] = {
                "table_name": table_name,
                "columns": [src_index_col],
            }

            indexes[name]["type"] = _get_special_column_type(collection['all_fields'][src_index_col]['type'])

            # Destination column
            name = f"{dst_index_table}.idx.{_remove_leading_underscore(relation['destination_attribute'])}"
            indexes[name] = {
                "table_name": dst_index_table,
                "columns": [relation['destination_attribute']],
            }

            indexes[name]["type"] = _get_special_column_type(
                dst_collection['all_fields'][relation['destination_attribute']]['type']
            )

    return indexes


def _derive_indexes() -> dict:
    indexes = {}

    # Add indexes to events table
    event_indexes = ['catalogue', 'entity', 'action', 'source', 'application']
    for column in event_indexes:
        indexes[f'events.idx.{column}'] = {
            "columns": [column],
            "table_name": "events"
        }

    # Add entity, timestamp index
    indexes[f'events.idx.entity_timestamp'] = {
        "columns": ['entity', 'timestamp DESC'],
        "table_name": "events",
    }

    for catalog_name, catalog in model.get_catalogs().items():
        for collection_name, collection in model.get_collections(catalog_name).items():
            entity = collection['all_fields']
            table_name = model.get_table_name(catalog_name, collection_name)

            # Generate indexes on default columns
            for idx_name, columns in _default_indexes_for_columns(list(entity.keys())).items():
                indexes[f'{table_name}.idx.{idx_name}'] = {
                    "columns": columns,
                    "table_name": table_name,
                }

            # Add source, last event index
            columns = [FIELD.SOURCE, FIELD.LAST_EVENT + ' DESC']
            idx_name = "_".join([_remove_leading_underscore(column) for column in columns])
            indexes[f'{table_name}.idx.{idx_name}'] = {
                "columns": columns,
                "table_name": table_name,
            }

            # Generate indexes on referenced columns (GOB.Reference and GOB.ManyReference)
            indexes.update(**_relation_indexes_for_collection(catalog_name, collection_name, collection))
    return indexes


models = _derive_models()
indexes = _derive_indexes()
