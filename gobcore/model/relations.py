"""
GOB Relations module

Relations are automatically derived from the GOB Model specification.
"""
from gobcore.model.metadata import FIELD, DESCRIPTION

_startup = True  # Show relation warnings only on startup (first execution)


def _get_relation(name, src_has_states, dst_has_states):
    """
    Get the relation specification

    If the source or destination of the relation has states, additional attributes are added

    :param name: The name of the relation
    :param src_has_states: Tells if the source has states
    :param dst_has_states: Tells if the destination has states
    :return: The relation specification
    """
    # Source sequence number is added for sources with states
    src_seqnr = {
        FIELD.SEQNR: {
            "type": "GOB.String",
            "description": DESCRIPTION[FIELD.SEQNR]
        }
    } if src_has_states else {}

    # Destination sequence number is afdded for destinations with states
    dst_seqnr = {
        f"dst_{FIELD.SEQNR}": {
            "type": "GOB.String",
            "description": DESCRIPTION[FIELD.SEQNR]
        }
    } if dst_has_states else {}

    # Relation start and end dates are added if any of the source or destination has states
    start_end_validity = {
        FIELD.START_VALIDITY: {
            "type": "GOB.Date",
            "description": "Begin relation"
        },
        FIELD.END_VALIDITY: {
            "type": "GOB.Date",
            "description": "End relation"
        }
    } if src_has_states or dst_has_states else {}

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
                try:
                    # Relations may exist to not yet existing other entities, catch exceptions
                    dst_catalog = model.get_catalog(dst_catalog_name)
                    dst_collection = model.get_collection(dst_catalog_name, dst_collection_name)
                    name = f"{src_catalog['abbreviation']}_{src_collection['abbreviation']}_" + \
                           f"{dst_catalog['abbreviation']}_{dst_collection['abbreviation']}_" + \
                           f"{reference_name}"
                except (TypeError, KeyError):
                    if _startup:
                        # Show warnings only on startup
                        print(f"Skip {src_catalog_name}.{src_collection_name}.{reference_name} => " +\
                              f"{dst_catalog_name}.{dst_collection_name}")
                    continue
                src_has_states = model.has_states(src_catalog_name, src_collection_name)
                dst_has_states = model.has_states(dst_catalog_name, dst_collection_name)
                relations["collections"][name] = _get_relation(name, src_has_states, dst_has_states)
    _startup = False
    return relations
