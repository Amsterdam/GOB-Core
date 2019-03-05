"""
GOB Relations module

Relations are automatically derived from the GOB Model specification.
"""
from gobcore.model.metadata import FIELD, DESCRIPTION

_startup = True  # Show relation warnings only on startup (first execution)


def _get_relation(name, src_begin_geldigheid, dst_begin_geldigheid):
    """
    Get the relation specification

    If the source or destination of the relation has states, additional attributes are added

    :param name: The name of the relation
    :param src_has_states: Tells if the source has states
    :param dst_has_states: Tells if the destination has states
    :return: The relation specification
    """
    date_type = None
    # Source sequence number is added for sources with states
    src_seqnr = {}
    if src_begin_geldigheid:
        date_type = src_begin_geldigheid['type']
        src_seqnr = {
            FIELD.SEQNR: {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.SEQNR]
            }
        }

    # Destination sequence number is afdded for destinations with states
    dst_seqnr = {}
    if dst_begin_geldigheid:
        if date_type != "GOB.DateTime":
            date_type = dst_begin_geldigheid['type']
        dst_seqnr = {
            f"dst_{FIELD.SEQNR}": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.SEQNR]
            }
        }

    start_end_validity = {
        FIELD.START_VALIDITY: {
            "type": date_type,
            "description": "Begin relation"
        },
        FIELD.END_VALIDITY: {
            "type": date_type,
            "description": "End relation"
        }
    } if src_begin_geldigheid or dst_begin_geldigheid else {}

    return {
        "version": "0.1",
        "abbreviation": name,
        "entity_id": "id",
        "attributes": {
            "id": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.ID]
            },
            **src_seqnr,
            "derivation": {
                "type": "GOB.String",
                "description": "Describes the derivation logic for the relation (e.g. geometric, reference, ..)"
            },
            **start_end_validity,
            f"dst{FIELD.SOURCE}": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.SOURCE]
            },
            f"dst{FIELD.ID}": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.ID]
            },
            **dst_seqnr
        }
    }


def _get_destination(model, dst_catalog_name, dst_collection_name):
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


def _get_relation_name(model, src, dst, reference_name):
    try:
        # Relations may exist to not yet existing other entities, catch exceptions
        return f"{src['catalog']['abbreviation']}_{src['collection']['abbreviation']}_" + \
               f"{dst['catalog']['abbreviation']}_{dst['collection']['abbreviation']}_" + \
               f"{reference_name}"
    except (TypeError, KeyError):
        return None


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
                name = _get_relation_name(model=model,
                                          src=src,
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
