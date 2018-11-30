"""GOB

SQLAlchemy GOB Models

"""
from sqlalchemy import Index
from sqlalchemy.ext.declarative import declarative_base

# Import data definitions
from gobcore.model import GOBModel
from gobcore.model.metadata import FIXED_FIELDS, PRIVATE_META_FIELDS, PUBLIC_META_FIELDS, STATE_FIELDS
from gobcore.model.metadata import columns_to_fields
from gobcore.typesystem import get_gob_type

Base = declarative_base()

EVENTS_DESCRIPTION = {
    "eventid": "Unique identification of the event, numbered sequentially",
    "timestamp": "Datetime when the event as created",
    "catalogue": "The catalogue in which the entity resides",
    "entity": "The entity to which the event need to be applied",
    "version": "The version of the entity model",
    "action": "Add, change, delete or confirm",
    "source": "The source of the entity, e.g. DIVA",
    "source_id": "The id of the entity in the source",
    "contents": "A json object that holds the contents for the action, the full entity for an Add"
}

EVENTS = {
    "eventid": "GOB.PKInteger",
    "timestamp": "GOB.DateTime",
    "catalogue": "GOB.String",
    "entity": "GOB.String",
    "version": "GOB.String",
    "action": "GOB.String",
    "source": "GOB.String",
    "source_id": "GOB.String",
    "contents": "GOB.JSON"
}


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

    model = GOBModel()

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
            fields = {col: desc for col, desc in collection['fields'].items()}

            state_fields = STATE_FIELDS if collection.get('has_states') else {}

            # Collect all columns for this collection
            entity = {
                **FIXED_FIELDS,
                **PRIVATE_META_FIELDS,
                **PUBLIC_META_FIELDS,
                **state_fields,
                **fields
            }

            table_name = model.get_table_name(catalog_name, collection_name)
            models[table_name] = columns_to_model(table_name, entity)
            Index(
                f'{table_name}.idx.source_source_id',
                '_source',
                '_source_id',
                unique=True
            )

    return models


models = _derive_models()
