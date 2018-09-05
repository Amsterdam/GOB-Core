import os

import sqlalchemy
import geoalchemy2

from gobcore.exceptions import GOBException
from gobcore.typesystem.gob_types import GOBType

# todo: define here? Or in model? (Or both)?
DEFAULT_SRID = int(os.getenv("DEFAULT_SRID", "4326"))


class GEOType(GOBType):
    name = "geometry"
    sql_type = geoalchemy2.Geometry
    is_composite = True


class Point(GEOType):
    name = "point"
    sql_type = geoalchemy2.Geometry('POINT')
    _srid = DEFAULT_SRID

    @classmethod
    def get_column_definition(cls, column_name, **kwargs):
        srid = kwargs['srid'] if 'srid' in kwargs else cls._srid
        return sqlalchemy.Column(column_name, geoalchemy2.Geometry(geometry_type='POINT', srid=srid))

    @classmethod
    def to_db(cls, value, srid=None):
        """ The value is """
        if srid is None:
            srid = cls._srid

        return geoalchemy2.WKTElement(value, srid=srid)

    @classmethod
    def to_str_or_none(cls, **values):
        required_keys = ['x', 'y']
        for key in required_keys:
            if key not in values:
                raise GOBException(f"Missing required key {key}")

        x = values['x']
        y = values['y']
        srid = values['srid'] if 'srid' in values else cls._srid

        geo_element = geoalchemy2.WKTElement(f'POINT({x} {y})', srid=srid)

        return super().to_str_or_none(geo_element)
