"""GOB

SQLAlchemy GOB Models

"""
import hashlib

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

TABLE_TYPE_RELATION = 'relation_table'
TABLE_TYPE_ENTITY = 'entity_table'


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


def _default_indexes_for_columns(input_columns: list, table_type: str) -> dict:
    """Returns applicable indexes for table with input_columns

    :param input_columns:
    :return:
    """
    default_indexes = [
        (FIELD.ID,),
        (FIELD.GOBID,),
        (FIELD.DATE_DELETED),
        (FIELD.EXPIRATION_DATE, FIELD.DATE_DELETED),
        (FIELD.ID, FIELD.SEQNR, FIELD.EXPIRATION_DATE),
    ]

    entity_table_indexes = [
        (FIELD.ID, FIELD.SEQNR),
    ]

    relation_table_indexes = [
        (FIELD.GOBID, FIELD.EXPIRATION_DATE),
        (FIELD.ID, f"src{FIELD.ID}", f"src_{FIELD.SEQNR}", f"src_{FIELD.SOURCE}", FIELD.SOURCE_VALUE,
         FIELD.DATE_DELETED, FIELD.APPLICATION),
        (FIELD.APPLICATION, FIELD.START_VALIDITY, FIELD.END_VALIDITY),
        (f"src{FIELD.ID}", f"src_{FIELD.SEQNR}", FIELD.DATE_DELETED),
        (f"dst{FIELD.ID}", f"dst_{FIELD.SEQNR}", FIELD.DATE_DELETED),
    ]

    create_indexes = default_indexes

    if table_type == TABLE_TYPE_ENTITY:
        create_indexes += entity_table_indexes
    elif table_type == TABLE_TYPE_RELATION:
        create_indexes += relation_table_indexes

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


def _hashed_index_name(prefix, index_name):
    return f"{prefix}_{hashlib.md5(index_name.encode()).hexdigest()}"


def _relation_indexes_for_collection(catalog_name, collection_name, collection, idx_prefix):
    indexes = {}
    table_name = model.get_table_name(catalog_name, collection_name)

    reference_columns = {column: desc['ref'] for column, desc in collection['all_fields'].items() if
                         desc['type'] in ['GOB.Reference', 'GOB.ManyReference']}

    # Search source and destination attributes for relation and define index
    for col, ref in reference_columns.items():
        dst_index_table = model.get_table_name_from_ref(ref)
        relations = sources.get_field_relations(catalog_name, collection_name, col)

        for relation in relations:
            src_index_col = f"{relation['source_attribute'] if 'source_attribute' in relation else col}"

            # Source column
            name = _hashed_index_name(idx_prefix, _remove_leading_underscore(src_index_col))
            indexes[name] = {
                "table_name": table_name,
                "columns": [src_index_col],
            }

            indexes[name]["type"] = _get_special_column_type(collection['all_fields'][src_index_col]['type'])

            # Destination column
            name = _hashed_index_name(
                idx_prefix, _remove_leading_underscore(relation['destination_attribute'])
            )
            indexes[name] = {
                "table_name": dst_index_table,
                "columns": [relation['destination_attribute'], FIELD.START_VALIDITY, FIELD.END_VALIDITY,
                            FIELD.DATE_DELETED],
            }

    return indexes


def _derive_indexes() -> dict:
    indexes = {}

    # Add indexes to events table
    event_indexes = ['catalogue', 'entity', 'action', 'source']
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
            is_relation_table = table_name.startswith('rel_')

            if is_relation_table:
                prefix = '_'.join(table_name.split('_')[:3])
            else:
                prefix = f"{catalog['abbreviation']}_{collection['abbreviation']}".lower()

            # Generate indexes on default columns
            for idx_name, columns in _default_indexes_for_columns(
                    list(entity.keys()),
                    TABLE_TYPE_RELATION if is_relation_table else TABLE_TYPE_ENTITY
            ).items():
                indexes[_hashed_index_name(prefix, idx_name)] = {
                    "columns": columns,
                    "table_name": table_name,
                }

            # Add source, last event index
            columns = [FIELD.SOURCE, FIELD.LAST_EVENT + ' DESC']
            idx_name = "_".join([_remove_leading_underscore(column) for column in columns])
            indexes[_hashed_index_name(prefix, idx_name)] = {
                "columns": columns,
                "table_name": table_name,
            }

            # Generate indexes on referenced columns (GOB.Reference and GOB.ManyReference)
            indexes.update(**_relation_indexes_for_collection(catalog_name, collection_name, collection, prefix))

    return indexes


models = _derive_models()
indexes = _derive_indexes()
