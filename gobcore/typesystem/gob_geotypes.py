import os
import json
import re

from abc import abstractmethod
from json import JSONDecodeError

import sqlalchemy
import geoalchemy2
from geoalchemy2.shape import to_shape
from geomet import wkt

from gobcore.exceptions import GOBException
from gobcore.typesystem.gob_types import GOBType, Decimal
from gobcore.typesystem.json import GobTypeJSONEncoder

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

        if isinstance(value, str):
            regex = re.compile("^POINT\s*\([0-9\s\.]*\)$")
            if not regex.match(value):
                raise ValueError("Illegal Point WKT value")

        if isinstance(value, geoalchemy2.elements.WKBElement):
            # Use shapely to construct wkt string and use wkt load to get correct precision
            value = wkt.loads(to_shape(value).wkt)

        if isinstance(value, dict):
            # serialize possible geojson
            value = json.dumps(value, cls=GobTypeJSONEncoder)

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


class Polygon(GEOType):
    # POLYGON ((115145.619264024 485115.91328199, ...))
    name = "Polygon"
    sql_type = geoalchemy2.Geometry('POLYGON')

    @classmethod  # noqa: C901
    def from_value(cls, value, **kwargs):
        """Instantiates a Polygon from a value and optional arguments

        Currently precision is supported as an optional argument

        :param value: the value to convert to a polygon
        :param kwargs: optional arguments
        :return: Polygon
        """

        if isinstance(value, str):
            regex = re.compile("^POLYGON\s*\([0-9\s\.,\(\)]*\)$")
            if not regex.match(value):
                raise ValueError(f"Illegal Polygon WKT value: {value}")

        if isinstance(value, geoalchemy2.elements.WKBElement):
            # Use shapely to construct wkt string and use wkt load to get correct precision
            value = wkt.loads(to_shape(value).wkt)

        if isinstance(value, dict):
            # serialize possible geojson
            value = json.dumps(value, cls=GobTypeJSONEncoder)

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
        """Instantiates a Polygon from a values dictionary

        :param values: dictionary containing construction parameters
        :raises ValueError because the method is not implemented
        :return None
        """
        raise ValueError(f"NYI")

    @classmethod
    def get_column_definition(cls, column_name, **kwargs):
        """Get the database column definition for a Polygon

        :param column_name: name of the column in the database
        :param kwargs: arguments
        :return: sqlalchemy.Column
        """
        srid = kwargs['srid'] if 'srid' in kwargs else cls._srid
        return sqlalchemy.Column(column_name, geoalchemy2.Geometry(geometry_type='POLYGON', srid=srid))


class Geometry(GEOType):
    """Geometry

    General geometry:

        "GEOMETRY",
        "POINT",
        "LINESTRING",
        "POLYGON",
        "MULTIPOINT",
        "MULTILINESTRING",
        "MULTIPOLYGON",
        "GEOMETRYCOLLECTION"
        "CURVE",

    """
    name = "Geometry"
    sql_type = geoalchemy2.Geometry('GEOMETRY')

    @classmethod  # noqa: C901
    def from_value(cls, value, **kwargs):
        """Instantiates a Geometry from a value and optional arguments

        Currently precision is supported as an optional argument.

        A rudimentary check on the validity of string values is performed.

        :param value: the value to convert to a geometry
        :param kwargs: optional arguments
        :return: Geometry
        """

        if isinstance(value, str):
            regex = re.compile("^[A-Z]+\s*\([A-Z0-9.,\s\(\)]+\)$")
            if not regex.match(value):
                raise ValueError(f"Illegal Geometry WKT value: {value}")

        if isinstance(value, geoalchemy2.elements.WKBElement):
            # Use shapely to construct wkt string and use wkt load to get correct precision
            value = wkt.loads(to_shape(value).wkt)

        if isinstance(value, dict):
            # serialize possible geojson
            value = json.dumps(value, cls=GobTypeJSONEncoder)

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
        """Instantiates a Geometry from a values dictionary

        :param values: dictionary containing construction parameters
        :raises ValueError because the method is not implemented
        :return None
        """
        raise ValueError(f"NYI")

    @classmethod
    def get_column_definition(cls, column_name, **kwargs):
        """Get the database column definition for a Geometry

        :param column_name: name of the column in the database
        :param kwargs: arguments
        :return: sqlalchemy.Geometry
        """
        srid = kwargs['srid'] if 'srid' in kwargs else cls._srid
        return sqlalchemy.Column(column_name, geoalchemy2.Geometry(geometry_type='GEOMETRY', srid=srid))
