"""GOB Relations module.

Relations are automatically derived from the GOB Model specification.
"""

from collections import defaultdict

from gobcore.model.metadata import FIELD, DESCRIPTION
from gobcore.model.name_compressor import NameCompressor
from gobcore.exceptions import GOBException

# Derivation of relation
DERIVATION = {
    "ON_KEY": "key",    # Value is derived on key comparison, eg bronwaarde == code
    "ON_GEO": "geo"     # Value is derived on geometrical comparison, eg point in polygon
}

_startup = True  # Show relation warnings only on startup (first execution)


def _get_relation(name):
    """Get the relation specification.

    :param name: The name of the relation
    :return: The relation specification
    """
    return {
        "version": "0.1",
        "abbreviation": name,
        "entity_id": "id",
        "attributes": {
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
                "type": "GOB.Integer",
                "description": DESCRIPTION[FIELD.SEQNR]
            },
            f"{FIELD.SOURCE_VALUE}": {
                "type": "GOB.String",
                "description": DESCRIPTION[FIELD.SOURCE_VALUE]
            },
            "derivation": {
                "type": "GOB.String",
                "description": "Describes the derivation logic for the relation (e.g. geometric, key compare, ..)"
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
                "type": "GOB.Integer",
                "description": DESCRIPTION[FIELD.SEQNR]
            },
            FIELD.START_VALIDITY: {
                "type": "GOB.DateTime",
                "description": DESCRIPTION[FIELD.START_VALIDITY],
            },
            FIELD.END_VALIDITY: {
                "type": "GOB.DateTime",
                "description": DESCRIPTION[FIELD.END_VALIDITY],
            },
            FIELD.LAST_SRC_EVENT: {
                "type": "GOB.Integer",
                "description": DESCRIPTION[FIELD.LAST_SRC_EVENT],
            },
            FIELD.LAST_DST_EVENT: {
                "type": "GOB.Integer",
                "description": DESCRIPTION[FIELD.LAST_DST_EVENT],
            },
        }
    }


def _get_destination(model, dst_catalog_name, dst_collection_name):
    """Get the destination catalog and collection given their names.

    :param model: GOBModel class/instance
    :param dst_catalog_name:
    :param dst_collection_name:
    :return:
    """
    try:
        # model.data covers GOBModel initialisation.
        dst_catalog = model.data[dst_catalog_name]
        dst_collection = dst_catalog['collections'][dst_collection_name]
        return {
            "catalog": dst_catalog,
            "catalog_name": dst_catalog_name,
            "collection": dst_collection,
            "collection_name": dst_collection_name
        }
    # hr.nietnatuurlijkepersonen, hr.rechtspersoon
    except KeyError:
        return None


def _get_relation_name(src, dst, reference_name):
    """Get the name of the relation. This name can be used as table name.

    :param src: source {catalog, catalog_name, collection, collection_name}
    :param dst: destination {catalog, catalog_name, collection, collection_name}
    :param reference_name: name of the reference attribute
    :return:
    """
    try:
        # Relations may exist to not yet existing other entities, catch exceptions
        name = (f"{src['catalog']['abbreviation']}_{src['collection']['abbreviation']}_" +
                f"{dst['catalog']['abbreviation']}_{dst['collection']['abbreviation']}_" +
                f"{reference_name}").lower()
        return NameCompressor.compress_name(name)
    # brk.kadastralesubjecten.heeft_rsin_voor => hr.nietnatuurlijkepersonen
    # hr.sbiactiviteiten.heeft_als_rechtspersoon => hr.rechtspersoon
    except TypeError:
        return None


def split_relation_table_name(table_name: str):
    table_name = NameCompressor.uncompress_name(table_name)
    split = table_name.split('_')

    if len(split) < 6:
        raise GOBException("Invalid table name")

    # Example: rel_brk_tng_brk_sjt_van_kadastraalsubject
    #          0   1   2   3   4   5 ......

    return {
        'src_cat_abbr': split[1],
        'src_col_abbr': split[2],
        'dst_cat_abbr': split[3],
        'dst_col_abbr': split[4],
        'reference_name': "_".join(split[5:]),
    }


def get_reference_name_from_relation_table_name(table_name: str):
    return split_relation_table_name(table_name)['reference_name']


def get_relation_name(model, catalog_name, collection_name, reference_name):
    """Get the name of the relation. This name can be used as table name.

    :param model: The GOBModel instance
    :param catalog_name:
    :param collection_name:
    :param reference_name:
    :return:
    """
    catalog = model[catalog_name]
    collection = model[catalog_name]['collections'][collection_name]
    reference = [reference for name, reference in collection['attributes'].items()
                 if name == reference_name][0]
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
    """Get the relation specs for all references within GOBModel data.

    :param model: GOBModel class
    :return: The relation specifications
    """
    global _startup
    relations = {
        "version": "0.1",
        "abbreviation": "REL",
        "description": "GOB Relations",
        "collections": {}
    }
    # model.data covers GOBModel initialisation.
    for src_catalog_name, src_catalog in model.data.items():
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
                relations["collections"][name] = _get_relation(name)
    _startup = False
    return relations


def get_fieldnames_for_missing_relations(model):
    """Add missing references.

    Returns the field names in a catalog -> collection -> [fieldnames] dict for which no relation
    is defined, for example in case a collection is referenced that doesn't exist yet.

    :param model: The GOBModel instance
    :return:
    """
    result = {}
    for src_catalog_name, src_catalog in model.items():
        result[src_catalog_name] = {}
        for src_collection_name, src_collection in src_catalog['collections'].items():
            result[src_catalog_name][src_collection_name] = []
            references = model._extract_references(src_collection['attributes'])
            for reference_name, reference in references.items():
                src = {
                    "catalog": src_catalog,
                    "catalog_name": src_catalog_name,
                    "collection": src_collection,
                    "collection_name": src_collection_name
                }
                dst_catalog_name, dst_collection_name = reference['ref'].split(':')
                dst = _get_destination(model, dst_catalog_name, dst_collection_name)
                name = _get_relation_name(src=src, dst=dst, reference_name=reference_name)

                if not (dst and name):
                    result[src_catalog_name][src_collection_name].append(reference_name)
    return result


def get_inverse_relations(model):
    """Returns a list of inverse relations for each collection, grouped by owning collection.

    For example, when brk:tenaamstellingen has a relation heeft_zrt with brk:zakelijkerechten,
    the result of this function would be:

    brk: {
      zakelijkerechten: { # Dict of all collections that reference brk:zakelijkerechten
        brk: {
            tenaamstellingen: [ # List of the names of all references in brk:tenaamstellingen to brk:zakelijkerechten
                heeft_zrt,
                ..,
            ]
        }
      },
      ..,
      kadastraleobjecten: [ .. ] # List of all collections that reference brk:kadastraleobjecten
    }

    :param model: The GOBModel instance
    :return:
    """
    inverse_relations = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    for src_cat_name, src_catalog in model.items():
        for src_col_name, src_collection in src_catalog['collections'].items():
            references = model._extract_references(src_collection['attributes'])

            for reference_name, reference in references.items():
                dst_cat_name, dst_col_name = reference['ref'].split(':')

                try:
                    inverse_relations[dst_cat_name][dst_col_name][src_cat_name][src_col_name]
                except KeyError:
                    inverse_relations[dst_cat_name][dst_col_name][src_cat_name][src_col_name] = []

                inverse_relations[dst_cat_name][dst_col_name][src_cat_name][src_col_name].append(reference_name)
    return inverse_relations


def get_relations_for_collection(model, catalog_name, collection_name):
    """Return a dictionary with all relations and the table_name for a specified collection.

    :param model: The GOBModel instance
    :param catalog_name:
    :param collection_name:
    :return:
    """
    collection = model[catalog_name]['collections'][collection_name]
    references = model._extract_references(collection['attributes'])
    table_names = {reference_name: get_relation_name(model, catalog_name, collection_name, reference_name)
                   for reference_name in references.keys()}
    return table_names


def create_relation(src, validity, dst, derivation):
    """Create a relation for the given specification items.

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
