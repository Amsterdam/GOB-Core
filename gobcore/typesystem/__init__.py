"""GOB Data types

GOB data consists of entities with attributes, eg Meetbout = { meetboutidentificatie, locatie, ... }
The possible types for each attribute are defined in this module.
The definition and characteristics of each type is in the gob_types module

"""
from gobcore.typesystem import gob_types

# The possible type definitions are imported from the gob_types module
GOB = gob_types

# The actual types that are used within GOB
GOB_TYPES = [
    GOB.String,
    GOB.Character,
    GOB.Integer,
    GOB.PKInteger,
    GOB.Number,
    GOB.Decimal,
    GOB.Date,
    GOB.DateTime,
    GOB.JSON,
    GOB.Boolean
]

# Convert GOB_TYPES to a dictionary indexed by the name of the type, prefixed by GOB.
_gob_types_dict = {f'GOB.{gob_type.__name__}': gob_type for gob_type in GOB_TYPES}

# no geo implemented yet. We pass wkt strings around for now.
# todo: Implement geo
_gob_types_dict['GOB.Geo.Point'] = GOB.String


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
        old_value = getattr(entity, field_name)
        new_value = data[field_name]

        if not gob_type.equals(old_value, new_value):
            modifications.append({'key': field_name, 'old_value': old_value, 'new_value': new_value})

    return modifications
