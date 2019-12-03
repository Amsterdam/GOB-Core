"""GOB Data types

GOB data consists of entities with attributes, eg Meetbout = { identificatie, locatie, ... }
The possible types for each attribute are defined in this module.
The definition and characteristics of each type is in the gob_types module

"""
import sqlalchemy
import geoalchemy2

from gobcore.exceptions import GOBException
from gobcore.typesystem import gob_types, gob_secure_types, gob_geotypes

# The possible type definitions are imported from the gob_types module
GOB = gob_types
SEC = gob_secure_types
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
    GOB.Reference,
    GOB.ManyReference,
    GOB.VeryManyReference
]

GOB_SECURE_TYPES = [
    SEC.SecureString,
    SEC.SecureDecimal,
    SEC.SecureDateTime
]

GEO_TYPES = [
    GEO.Point,
    GEO.Polygon,
    GEO.Geometry
]

# Convert GOB_TYPES to a dictionary indexed by the name of the type, prefixed by GOB.
_gob_types = {f'GOB.{gob_type.name}': gob_type for gob_type in GOB_TYPES}
_gob_securetypes = {f'GOB.{gob_type.name}': gob_type for gob_type in GOB_SECURE_TYPES}
_gob_geotypes = {f'GOB.Geo.{gob_type.name}': gob_type for gob_type in GEO_TYPES}
_gob_types_dict = {**_gob_types, **_gob_securetypes, **_gob_geotypes}


# Convert GOB_TYPES to a dictionary indexed by the name of the type, prefixed by GOB.
_gob_sql_types_list = [{'gob_type': gob_type, 'sql_type': gob_type.sql_type} for gob_type in GOB_TYPES + GEO_TYPES]

# Postgres specific mapping for sql types to GOBTypes
_gob_postgres_sql_types_list = [
    {'sql_type': sqlalchemy.types.VARCHAR, 'gob_type': GOB.String},
    {'sql_type': sqlalchemy.types.TEXT, 'gob_type': GOB.String},
    {'sql_type': sqlalchemy.types.CHAR, 'gob_type': GOB.Character},
    {'sql_type': sqlalchemy.types.INTEGER, 'gob_type': GOB.Integer},
    {'sql_type': sqlalchemy.types.BIGINT, 'gob_type': GOB.Integer},
    {'sql_type': sqlalchemy.types.NUMERIC, 'gob_type': GOB.Decimal},
    {'sql_type': sqlalchemy.types.BOOLEAN, 'gob_type': GOB.Boolean},
    {'sql_type': sqlalchemy.types.DATE, 'gob_type': GOB.Date},
    {'sql_type': sqlalchemy.dialects.postgresql.base.TIMESTAMP, 'gob_type': GOB.DateTime},
    {'sql_type': sqlalchemy.dialects.postgresql.JSON, 'gob_type': GOB.JSON},
    {'sql_type': sqlalchemy.dialects.postgresql.JSONB, 'gob_type': GOB.JSON},
    {'sql_type': sqlalchemy.types.JSON, 'gob_type': GOB.JSON},
    {'sql_type': geoalchemy2.types.Geometry, 'gob_type': GEO.Point},
]


def enhance_type_info(type_info):
    """
    Enhance type info with gob types

    For every type attribute a corresponding gob_type attribute is set
    that contains the GOB type class

    :param type_info:
    :return:
    """
    if type_info.get("type"):
        type_info["gob_type"] = get_gob_type(type_info["type"])
    for value in type_info.values():
        # Recurse into type info to add type info for each "type" attribute
        if isinstance(value, dict):
            enhance_type_info(value)


def get_gob_type(name):
    """
    Get the type definition for a given type name

    Example:
        get_gob_type("string") => GOBType:String

    :param name:
    :return: the type definition (class) for the given type name
    """
    return _gob_types_dict[name]


def is_gob_reference_type(type_name):
    """
    Tels if type_name is the name of a GOB Reference type

    :param type_name:
    :return:
    """
    return type_name in [f"GOB.{t.name}" for t in [GOB.Reference, GOB.ManyReference, GOB.VeryManyReference]]


def is_gob_geo_type(name):
    """Returns a boolean value

    :param name:
    :return:
    """
    return name in _gob_geotypes.keys()


def is_gob_json_type(name):
    """Returns a boolean value

    :param name:
    :return:
    """
    try:
        gobtype = _gob_types_dict[name]
    except KeyError:
        return False

    return gobtype.sql_type.__visit_name__ == "JSONB"


def get_gob_type_from_sql_type(sql_type):
    """
    Get the type definition for a given sqlalchemy type

    Example:
        get_gob_type_from_sqlalchemy_type(<class 'sqlalchemy.sql.sqltypes.Integer'>) => GOBType:String

    :param name:
    :return: the type definition (class) for the given type name
    """
    for type_map in _gob_postgres_sql_types_list:
        if sql_type == type_map['sql_type']:
            return type_map['gob_type']
    raise GOBException(f"No GOBType found for SQLType: {sql_type}")


def get_modifications(entity, data, model):     # noqa: C901
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

        # Try to get the new value from the data, if missing, skip this field
        try:
            new_value = gob_type.from_value(data[field_name])
        except KeyError:
            continue

        if old_value != new_value:
            modifications.append({'key': field_name, 'old_value': old_value, 'new_value': new_value})

    return modifications


def get_value(entity):
    """Get a dictionatry with python objects of an entity of GOBTypes

    :param entity: an object with named attributes with values

    :return: a dictionary of key, values
    """
    return {key: value.to_value for key, value in entity.items()}
