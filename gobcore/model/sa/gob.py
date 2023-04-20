"""SQLAlchemy GOB Models."""


from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import ForeignKeyConstraint, UniqueConstraint

from gobcore.model import GOBModel

# Import data definitions
from gobcore.model.events import EVENTS, EVENTS_DESCRIPTION
from gobcore.model.metadata import FIELD, columns_to_fields
from gobcore.model.name_compressor import NameCompressor
from gobcore.model.relations import split_relation_table_name
from gobcore.typesystem import get_gob_type

TABLE_TYPE_RELATION = 'relation_table'
TABLE_TYPE_ENTITY = 'entity_table'

Base = declarative_base()


def get_base():
    """Backwards compatibility."""
    return Base


def get_column(column_name, column_specification):
    """Utility method to convert GOB type to a SQLAlchemy Column.

    Get the SQLAlchemy columndefinition for the gob type as exposed by the gob_type.

    :param column_name:
    :param column: (name, type)
    :return: sqlalchemy.Column
    """
    gob_type = get_gob_type(column_specification["type"])
    column = gob_type.get_column_definition(column_name, **column_specification)
    column.doc = column_specification["description"]
    return column


def _create_model_type(table_name, columns, has_states, table_args):
    """Creates model type based on given parameters. Extracted for testability.

    :param table_name:
    :param columns:
    :param has_states:
    :param table_args:
    :return:
    """
    return type(table_name, (get_base(),), {
        '__tablename__': table_name,
        **columns,
        '__has_states__': has_states,
        '__repr__': lambda self: table_name,
        '__table_args__': table_args,
    })


def columns_to_model(model: GOBModel, catalog_name, table_name, columns, has_states=False, constraint_columns=None):
    """Create a model out of table_name and GOB column specification.

    :param model: GOBModel instance
    :param table_name: name of the table
    :param columns: GOB column specification
    :param has_states:
    :return: Model class
    """
    # Convert columns to SQLAlchemy Columns
    columns = {column_name: get_column(column_name, column_spec) for column_name, column_spec in columns.items()}

    unique_constraint_tid = UniqueConstraint(FIELD.TID, name=f"{NameCompressor.compress_name(table_name)}__tid_key")

    if catalog_name == "rel":
        # Add FK constraints from relation table to src and dst tables
        relation_info = split_relation_table_name(table_name)

        src_catalog, src_collection = model.get_catalog_collection_from_abbr(relation_info['src_cat_abbr'],
                                                                             relation_info['src_col_abbr'])
        dst_catalog, dst_collection = model.get_catalog_collection_from_abbr(relation_info['dst_cat_abbr'],
                                                                             relation_info['dst_col_abbr'])

        def fk_constraint(to_catalog: dict, to_collection: dict, srcdst: str) -> ForeignKeyConstraint:
            """Creates FK constraint to given catalog and collection from as src or dst

            Example:
            foreign key from (src_id, src_volgnummer) to (_id, volgnummer), or
            foreign key from (dst_id) to (_id)

            :param to_catalog:
            :param to_collection:
            :param srcdst: src or dst
            :return:
            """
            tablename = model.get_table_name(to_catalog['name'], to_collection['name'])
            has_states = to_collection.get('has_states', False)

            return ForeignKeyConstraint(
                # Source columns (src or dst)
                [f"{srcdst}_{col}" for col in (
                    [FIELD.REFERENCE_ID, FIELD.SEQNR] if has_states else [FIELD.REFERENCE_ID]
                )],
                # Destination columns, prefixed with destination table name
                [f"{tablename}.{col}" for col in ([FIELD.ID, FIELD.SEQNR] if has_states else [FIELD.ID])],
                name=f"{NameCompressor.compress_name(table_name)}_{srcdst[0]}fk"
            )

        unique_constraint_source_id = UniqueConstraint(FIELD.SOURCE_ID,
                                                       name=f"{NameCompressor.compress_name(table_name)}_uniq")

        if relation_info['reference_name'] in src_collection["very_many_references"].keys():
            # Do not create FK constraints for very many references
            table_args = (unique_constraint_source_id, unique_constraint_tid,)
        else:
            table_args = (
                fk_constraint(src_catalog, src_collection, 'src'),
                fk_constraint(dst_catalog, dst_collection, 'dst'),
                unique_constraint_source_id,
                unique_constraint_tid,
            )

    else:
        if not constraint_columns:
            constraint_columns = [FIELD.ID, FIELD.SEQNR] if has_states else [FIELD.ID]

        # Limit the table_name to prevent a key name of more than 63 characters
        constraint_name = f"{table_name[:40]}_{'_'.join(constraint_columns)}_key"

        if FIELD.TID in columns:
            table_args = (UniqueConstraint(*constraint_columns, name=constraint_name), unique_constraint_tid,)
        else:
            table_args = (UniqueConstraint(*constraint_columns, name=constraint_name),)

    # Create model
    return _create_model_type(table_name, columns, has_states, table_args)


def get_sqlalchemy_models(model: GOBModel):
    """Derive Models from GOB model specification.

    :param model: GOBModel instance
    :return: None
    """
    if model.__class__.sqlalchemy_models:
        return model.__class__.sqlalchemy_models

    models = {
        # e.g. "meetbouten_rollagen": <BASE>
    }

    # Start with events
    columns_to_model(
        model,
        None,
        "events",
        columns_to_fields(EVENTS, EVENTS_DESCRIPTION),
        constraint_columns=["eventid"]
    )

    for catalog_name in model:
        for collection_name, collection in model[catalog_name]['collections'].items():
            # "attributes": {
            #     "attribute_name": {
            #         "type": "GOB type name, e.g. GOB.String",
            #         "description": "attribute description",
            #         "ref: "collection_name:entity_name, e.g. meetbouten:meetbouten"
            #     }, ...
            # }

            # the GOB model for the specified entity
            table_name = model.get_table_name(catalog_name, collection_name)
            models[table_name] = columns_to_model(model, catalog_name, table_name, collection['all_fields'],
                                                  has_states=collection.get('has_states', False))

    model.__class__.sqlalchemy_models = models
    return models
