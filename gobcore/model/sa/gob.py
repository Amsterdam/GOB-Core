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
from gobcore.typesystem import get_gob_type

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


def _derive_models():
    """Derive Models from GOB model specification

    :return: None
    """

    models = {
        # e.g. "meetbouten_rollagen": <BASE>
    }

    def columns_to_model(table_name, columns):
        """Create a model out of table_name and GOB column specification

        :param table_name: name of the table
        :param columns: GOB column specification
        :return: Model class
        """
        # Convert columns to SQLAlchemy Columns
        columns = {column_name: get_column(column_name, column_spec) for column_name, column_spec in columns.items()}

        # Create model
        return type(table_name, (Base,), {
            "__tablename__": table_name,
            **columns,
            "__repr__": lambda self: f"{table_name}"
        })

    # Start with events
    columns_to_model("events", columns_to_fields(EVENTS, EVENTS_DESCRIPTION))

    for catalog_name, catalog in model.get_catalogs().items():
        for collection_name, collection in model.get_collections(catalog_name).items():
            if collection['version'] != "0.1":
                # No migrations defined yet...
                raise ValueError("Unexpected version, please write a generic migration here or migrate the import")

            # "attributes": {
            #     "attribute_name": {
            #         "type": "GOB type name, e.g. GOB.String",
            #         "description": "attribute description",
            #         "ref: "collection_name:entity_name, e.g. meetbouten:meetbouten"
            #     }, ...
            # }

            # the GOB model for the specified entity
            table_name = model.get_table_name(catalog_name, collection_name)
            models[table_name] = columns_to_model(table_name, collection['all_fields'])

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
        (FIELD.SOURCE_ID,),
        (FIELD.START_VALIDITY,),
        (FIELD.END_VALIDITY,),
        (FIELD.DATE_DELETED,),
    ]
    result = {}
    for columns in default_indexes:
        # Check if all columns defined in index are present
        if all([column in input_columns for column in columns]):
            idx_name = "_".join([_remove_leading_underscore(column) for column in columns])
            result[idx_name] = columns
    return result


def _relation_indexes_for_collection(catalog_name, collection_name, collection):
    indexes = {}
    table_name = model.get_table_name(catalog_name, collection_name)

    reference_columns = {column: desc['ref'] for column, desc in collection['all_fields'].items() if
                         desc['type'] in ['GOB.Reference', 'GOB.ManyReference']}

    # Search source and destination attributes for relation and define index
    for col, ref in reference_columns.items():
        relations = sources.get_field_relations(catalog_name, collection_name, col)

        for relation in relations:
            src_index_col = f"{relation['source_attribute'] if 'source_attribute' in relation else col}"
            dst_index_table = model.table_name_from_ref(ref)

            indexes[f'{table_name}.idx.{_remove_leading_underscore(src_index_col)}'] = {
                "table_name": table_name,
                "columns": [src_index_col],
            }
            name = f"{dst_index_table}.idx.{_remove_leading_underscore(relation['destination_attribute'])}"
            indexes[name] = {
                "table_name": dst_index_table,
                "columns": [relation['destination_attribute']],
            }

    return indexes


def _derive_indexes() -> dict:
    indexes = {}

    for catalog_name, catalog in model.get_catalogs().items():
        for collection_name, collection in model.get_collections(catalog_name).items():
            if collection['version'] != "0.1":
                # No migrations defined yet...
                raise ValueError("Unexpected version, please write a generic migration here or migrate the import")

            entity = collection['all_fields']
            table_name = model.get_table_name(catalog_name, collection_name)

            # Generate indexes on default columns
            for idx_name, columns in _default_indexes_for_columns(list(entity.keys())).items():
                indexes[f'{table_name}.idx.{idx_name}'] = {
                    "columns": columns,
                    "table_name": table_name,
                }

            # Generate indexes on referenced columns (GOB.Reference and GOB.ManyReference)
            indexes.update(**_relation_indexes_for_collection(catalog_name, collection_name, collection))
    return indexes


models = _derive_models()
indexes = _derive_indexes()
