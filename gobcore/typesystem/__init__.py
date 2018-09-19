"""GOB Data types

GOB data consists of entities with attributes, eg Meetbout = { meetboutidentificatie, locatie, ... }
The possible types for each attribute are defined in this module.
The definition and characteristics of each type is in the gob_types module

"""
from gobcore.typesystem import gob_types, gob_geotypes

# The possible type definitions are imported from the gob_types module
GOB = gob_types
GEO = gob_geotypes

# The actual types that are used within GOB
GOB_TYPES = [
    GOB.String,
    GOB.Character,
    GOB.Integer,
    GOB.PKInteger,
    GOB.Decimal,
    GOB.Boolean,
    GOB.Date,
    GOB.DateTime,
    GOB.JSON,
]

GEO_TYPES = [
    GEO.Point
]

# Convert GOB_TYPES to a dictionary indexed by the name of the type, prefixed by GOB.
_gob_types = {f'GOB.{gob_type.name}': gob_type for gob_type in GOB_TYPES}
_gob_geotypes = {f'GOB.Geo.{gob_type.name}': gob_type for gob_type in GEO_TYPES}
_gob_types_dict = {**_gob_types, **_gob_geotypes}


def get_gob_type(name):
    """
    Get the type definition for a given type name

    Example:
        get_gob_type("string") => GOBType:String

    :param name:
    :return: the type definition (class) for the given type name
    """
    return _gob_types_dict[name]


def get_modifications(entity, data, model):
    """Get a list of modifications

    :param entity: an object with named attributes with values
    :param data: a dict with named keys and changed values
    :param model: a dict describing the model of both entity and data

    :return: a list of modification-dicts: `{'key': "fieldname", 'old_value': "old_value", 'new_value': "new_value"}`
    """
    modifications = []

    if entity is None or data is None:
        return modifications

    for field_name, field in model.items():
        gob_type = get_gob_type(field['type'])
        old_value = gob_type.from_value(getattr(entity, field_name))
        new_value = gob_type.from_value(data[field_name])

        if old_value != new_value:
            modifications.append({'key': field_name, 'old_value': old_value, 'new_value': new_value})

    return modifications
