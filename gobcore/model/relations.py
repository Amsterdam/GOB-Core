"""
GOB Relations module

Relations are automatically derived from the GOB Model specification.
"""
from gobcore.model.metadata import FIELD, DESCRIPTION

# Derivation of relation
DERIVATION = {
    "ON_KEY": "key",    # Value is derived on key comparison, eg bronwaarde == code
    "ON_GEO": "geo"     # Value is derived on geometrical comparison, eg point in polygon
}

_startup = True  # Show relation warnings only on startup (first execution)


def _get_relation(name, src_begin_geldigheid, dst_begin_geldigheid):
    """
    Get the relation specification

    :param name: The name of the relation
    :param src_begin_geldigheid: Source validity
    :param dst_begin_geldigheid: Destination validity
    :return: The relation specification
    """

    # Determine date type for validity, default to Date, if any is DateTime then use DateTime
    date_type = "GOB.Date"
    if src_begin_geldigheid:
        date_type = src_begin_geldigheid['type']
    if dst_begin_geldigheid and date_type != "GOB.DateTime":
        date_type = dst_begin_geldigheid['type']

    return {
        "version": "0.1",
        "abbreviation": name,
        "entity_id": "id",
        "attributes": {
            "source": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.SOURCE]
            },
            "id": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.ID]
            },
            f"src{FIELD.SOURCE}": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.SOURCE]
            },
            f"src{FIELD.ID}": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.ID]
            },
            f"src_{FIELD.SEQNR}": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.SEQNR]
            },
            "derivation": {
                "type": "GOB.String",
                "description": "Describes the derivation logic for the relation (e.g. geometric, key compare, ..)"
            },
            FIELD.START_VALIDITY: {
                "type": date_type,
                "description": "Begin relation"
            },
            FIELD.END_VALIDITY: {
                "type": date_type,
                "description": "End relation"
            },
            f"dst{FIELD.SOURCE}": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.SOURCE]
            },
            f"dst{FIELD.ID}": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.ID]
            },
            f"dst_{FIELD.SEQNR}": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.SEQNR]
            }
        }
    }


def _get_destination(model, dst_catalog_name, dst_collection_name):
    """
    Get the destination catalog and collection given their names

    :param model: GOBModel instance
    :param dst_catalog_name:
    :param dst_collection_name:
    :return:
    """
    try:
        dst_catalog = model.get_catalog(dst_catalog_name)
        dst_collection = model.get_collection(dst_catalog_name, dst_collection_name)
        return {
            "catalog": dst_catalog,
            "catalog_name": dst_catalog_name,
            "collection": dst_collection,
            "collection_name": dst_collection_name
        }
    except (TypeError, KeyError):
        return None


def _get_relation_name(src, dst, reference_name):
    """
    Get the name of the relation. This name can be used as table name

    :param src: source {catalog, catalog_name, collection, collection_name}
    :param dst: destination {catalog, catalog_name, collection, collection_name}
    :param reference_name: name of the reference attribute
    :return:
    """
    try:
        # Relations may exist to not yet existing other entities, catch exceptions
        return (f"{src['catalog']['abbreviation']}_{src['collection']['abbreviation']}_" +
                f"{dst['catalog']['abbreviation']}_{dst['collection']['abbreviation']}_" +
                f"{reference_name}").lower()
    except (TypeError, KeyError):
        return None


def get_relation_name(model, catalog_name, collection_name, reference_name):
    """
    Get the name of the relation. This name can be used as table name

    :param catalog_name:
    :param collection_name:
    :param reference_name:
    :return:
    """
    catalog = model.get_catalog(catalog_name)
    collection = model.get_collection(catalog_name, collection_name)
    reference = [reference for name, reference in collection['attributes'].items() if name == reference_name][0]
    dst_catalog_name, dst_collection_name = reference['ref'].split(':')

    src = {
        "catalog": catalog,
        "catalog_name": catalog_name,
        "collection": collection,
        "collection_name": collection_name
    }
    dst = _get_destination(model, dst_catalog_name, dst_collection_name)
    return _get_relation_name(src=src,
                              dst=dst,
                              reference_name=reference_name)


def get_relations(model):
    """
    Get the relation specs for all references within the GOB model

    :param model: The GOBModel instance
    :return: The relation specifications
    """
    global _startup
    relations = {
        "version": "0.1",
        "abbreviation": "REL",
        "description": "GOB Relations",
        "collections": {}
    }
    for src_catalog_name, src_catalog in model._data.items():
        for src_collection_name, src_collection in src_catalog['collections'].items():
            references = model._extract_references(src_collection['attributes'])
            for reference_name, reference in references.items():
                dst_catalog_name, dst_collection_name = reference['ref'].split(':')
                src = {
                    "catalog": src_catalog,
                    "catalog_name": src_catalog_name,
                    "collection": src_collection,
                    "collection_name": src_collection_name
                }
                dst = _get_destination(model, dst_catalog_name, dst_collection_name)
                name = _get_relation_name(src=src,
                                          dst=dst,
                                          reference_name=reference_name)
                if not (dst and name):
                    if _startup:
                        # Show warnings only on startup
                        print(f"Skip {src_catalog_name}.{src_collection_name}.{reference_name} => " +
                              f"{dst_catalog_name}.{dst_collection_name}")
                    continue
                src_begin_geldigheid = src['collection']['attributes'].get('begin_geldigheid')
                dst_begin_geldigheid = dst['collection']['attributes'].get('begin_geldigheid')
                relations["collections"][name] = _get_relation(name, src_begin_geldigheid, dst_begin_geldigheid)
    _startup = False
    return relations


def create_relation(src, validity, dst, derivation):
    """
    Create a relation for the given specification items

    :param src:
    :param validity:
    :param dst:
    :param derivation:
    :return:
    """

    src_id = src['id']
    if src[FIELD.SEQNR] is not None:
        src_id = f"{src_id}.{src[FIELD.SEQNR]}"

    dst_id = dst.get('id')
    if dst[FIELD.SEQNR] is not None:
        dst_id = f"{dst_id}.{dst[FIELD.SEQNR]}"

    return {
        "source": f"{src['source']}.{dst.get('source')}",
        "id": f"{src_id}.{dst_id}",

        f"src{FIELD.SOURCE}": src["source"],
        f"src{FIELD.ID}": src["id"],
        f"src_{FIELD.SEQNR}": src[FIELD.SEQNR],

        "derivation": derivation,

        FIELD.START_VALIDITY: validity[FIELD.START_VALIDITY],
        FIELD.END_VALIDITY: validity[FIELD.END_VALIDITY],

        f"dst{FIELD.SOURCE}": dst["source"],
        f"dst{FIELD.ID}": dst["id"],
        f"dst_{FIELD.SEQNR}": dst[FIELD.SEQNR]
    }
