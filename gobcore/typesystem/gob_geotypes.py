import os
import json
from abc import abstractmethod
from json import JSONDecodeError

import sqlalchemy
import geoalchemy2
from geomet import wkt

from gobcore.exceptions import GOBException
from gobcore.typesystem.gob_types import GOBType, Decimal

# todo: define here? Or in model? (Or both)?
DEFAULT_SRID = int(os.getenv("DEFAULT_SRID", "28992"))
DEFAULT_PRECISION = 2


class GEOType(GOBType):
    """Abstract baseclass for the GOB Geo Types, use like follows:

    Instances can be generated with the from_value method (for db, geojson or wkt values) or
    constructed with their composite values:

    gob_type1 = Point.from_value(db_field)
    gob_type2 = Point.from_value(geojson)
    gob_type3 = Point.from_value(db_field)

    gob_type4 = Point.from_values(x=1, y=2, srid=28992)

    # gob type store internal representation as a wkt string, the `str()` method exposes that
    $ 'POINT(112.0 22.0)' == str(Point.from_values(x=112, y=22)
    True

    # use comparison like this:
    gob_type1 == gob_type2  # True if both are None or str(gob_type1) == str(gob_type2)

    """
    name = "geometry"
    sql_type = geoalchemy2.Geometry
    # Geotypes are composite and have an srid
    is_composite = True
    _srid = DEFAULT_SRID
    _precision = DEFAULT_PRECISION

    def __init__(self, value, **kwargs):
        if 'srid' in kwargs:
            self._srid = kwargs['srid']
        super().__init__(value)

    @classmethod
    @abstractmethod
    def from_values(cls, **values):
        """Class method for composity GOB Types, that wil instantiate the type with the required values"""
        pass

    @classmethod
    def _to_wkt_elem(cls, value, srid=None):
        """Private method to create a geoalchemy WKTElement from the GOB_type, based on value (and optional srid)"""
        if srid is None:
            srid = cls._srid
        return geoalchemy2.WKTElement(value, srid=srid)

    @property
    def to_db(self):
        if self._string is None:
            return None
        return self._to_wkt_elem(self._string, self._srid)

    @property
    def json(self):
        if self._string is None or self._string == '':
            return json.dumps(None)
        return json.dumps(wkt.loads(self._string))


class Point(GEOType):
    name = "Point"
    sql_type = geoalchemy2.Geometry('POINT')

    @classmethod  # noqa: C901
    def from_value(cls, value, **kwargs):
        """Instantiates the GOBType Point, with either a database value, a geojson or WKT string"""

        if isinstance(value, geoalchemy2.Geometry):
            return cls(value.desc, srid=value.srid)

        if isinstance(value, dict):
            # serialize possible geojson
            value = json.dumps(value)

        # if is geojson dump to wkt string
        try:
            precision = kwargs['precision'] if 'precision' in kwargs else cls._precision
            wkt_string = wkt.dumps(json.loads(value), decimals=precision)
            value = wkt_string

            # it is not a to wkt_string dumpable json, let it pass:
        except JSONDecodeError:
            pass
        except TypeError:
            pass

        # is wkt string
        return cls(value)

    @classmethod
    def from_values(cls, **values):
        """Instantiate a Point, using x and y coordinates, and optional srid:

            point = Point.from_values(x=1, y=2, srid=28992)
        """
        required_keys = ['x', 'y']
        optional_keys = ['srid']
        for key in required_keys:
            if key not in values:
                raise GOBException(f"Missing required key {key}")

        kwargs = {k: v for k, v in values.items() if k not in required_keys+optional_keys}
        if 'precision' not in kwargs:
            kwargs['precision'] = cls._precision

        x = str(Decimal.from_value(values['x'], **kwargs))
        y = str(Decimal.from_value(values['y'], **kwargs))

        srid = values['srid'] if 'srid' in values else cls._srid
        return cls(f'POINT({x} {y})', srid=srid)

    @classmethod
    def get_column_definition(cls, column_name, **kwargs):
        srid = kwargs['srid'] if 'srid' in kwargs else cls._srid
        return sqlalchemy.Column(column_name, geoalchemy2.Geometry(geometry_type='POINT', srid=srid))
