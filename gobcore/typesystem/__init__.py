"""GOB Data types.

GOB data consists of entities with attributes, eg Meetbout = { identificatie, locatie, ... }
The possible types for each attribute are defined in this module.
The definition and characteristics of each type is in the gob_types module
"""


from typing import Any, Optional, TypedDict

import geoalchemy2
import sqlalchemy

from gobcore.exceptions import GOBException
from gobcore.typesystem import gob_geotypes, gob_secure_types, gob_types

# The possible type definitions are imported from the gob_types module
GOB = gob_types
SEC = gob_secure_types
GEO = gob_geotypes

# The actual types that are used within GOB
GOB_TYPES = [
    GOB.String,
    GOB.Character,
    GOB.Integer,
    GOB.BigInteger,
    GOB.PKInteger,
    GOB.Decimal,
    GOB.Boolean,
    GOB.Date,
    GOB.DateTime,
    GOB.JSON,
    GOB.Reference,
    GOB.ManyReference,
    GOB.VeryManyReference,
    GOB.IncompleteDate,
]

GOB_SECURE_TYPES = [
    SEC.SecureString,
    SEC.SecureDecimal,
    SEC.SecureDateTime,
    SEC.SecureDate,
    SEC.SecureIncompleteDate
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
    {'sql_type': sqlalchemy.types.BIGINT, 'gob_type': GOB.BigInteger},
    {'sql_type': sqlalchemy.types.NUMERIC, 'gob_type': GOB.Decimal},
    {'sql_type': sqlalchemy.types.BOOLEAN, 'gob_type': GOB.Boolean},
    {'sql_type': sqlalchemy.types.DATE, 'gob_type': GOB.Date},
    {'sql_type': sqlalchemy.dialects.postgresql.base.TIMESTAMP, 'gob_type': GOB.DateTime},
    {'sql_type': sqlalchemy.dialects.postgresql.JSON, 'gob_type': GOB.JSON},
    {'sql_type': sqlalchemy.dialects.postgresql.JSONB, 'gob_type': GOB.JSON},
    {'sql_type': sqlalchemy.types.JSON, 'gob_type': GOB.JSON},
    {'sql_type': geoalchemy2.types.Geometry, 'gob_type': GEO.Point},
]


def enhance_type_info(type_info: dict[str, Any]) -> None:
    """Enhance GOBModel field type info with GOBType class.

    For every "type" key a corresponding "gob_type" key is set that contains the GOBType class.

    :param type_info:
    :return:
    """
    if type_info.get("type") and not isinstance(type_info["type"], dict):
        type_info["gob_type"] = get_gob_type(type_info["type"])
    for value in type_info.values():
        # Recurse into GOB.JSON values to add a "gob_type" key for each attribute dict with a "type" key
        if isinstance(value, dict):
            enhance_type_info(value)


def get_gob_type_from_info(type_info):
    """Return the GOBType class for the given GOBModel type info.

    The type info is enhanced (adding GOB types to it)
    :param type_info:
    :return:
    """
    if not type_info.get("gob_type"):
        enhance_type_info(type_info)
    return type_info["gob_type"]


def get_gob_type(name):
    """Return GOBType class for a given GOBModel type name.

    Example:
        get_gob_type("GOB.String") => GOBType:String class

    :param name:
    :return: the type definition (class) for the given type name
    """
    return _gob_types_dict[name]


def fully_qualified_type_name(gob_type):
    """Return the fully qualified type name of GOBType class."""
    return f"GOB.{gob_type.name}"


def is_gob_reference_type(type_name):
    """Tell if type_name is the name of a GOB Reference type.

    :param type_name:
    :return:
    """
    return type_name in [fully_qualified_type_name(t)
                         for t in [GOB.Reference, GOB.ManyReference, GOB.VeryManyReference]]


def is_gob_geo_type(name) -> bool:
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


class Modification(TypedDict):
    """Modification dictionary."""

    key: str
    old_value: gob_types.GOBType
    new_value: gob_types.GOBType


def get_modifications(
    entity: Optional[Any], data: Optional[dict[str, Any]], model: dict[str, Any]
) -> list[Modification]:
    """Return a list with compare result modifications.

    :param entity: a SQLAlchemy object with named attributes with old values
    :param data: a dict with named keys and changed source values
    :param model: a dict describing the model of both entity and data (collection["all_fields"])

    :return: a list of modification-dicts: `{'key': "fieldname", 'old_value': "old_value", 'new_value': "new_value"}`
    """
    modifications: list[Modification] = []

    if entity is None or data is None:
        return modifications

    for field_name, field_info in model.items():
        gob_type = get_gob_type_from_info(field_info)
        field_kwargs = gob_types.get_kwargs_from_type_info(field_info)
        old_value = gob_type.from_value(getattr(entity, field_name), **field_kwargs)

        # Try to get the new value from the data, if missing, skip this field
        try:
            new_value = gob_type.from_value(data[field_name], **field_kwargs)
        except KeyError:
            continue

        if old_value != new_value:
            modifications.append({'key': field_name, 'old_value': old_value, 'new_value': new_value})

    return modifications


def get_value(entity):
    """Return a dictionary with Python objects of an entity of GOBTypes.

    :param entity: an object with named attributes with values

    :return: a dictionary of key, values
    """
    return {key: value.to_value for key, value in entity.items()}
