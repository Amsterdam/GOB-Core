import unittest
from unittest.mock import MagicMock

import geoalchemy2

from gobcore.typesystem import get_gob_type


class TestGobGeoTypes(unittest.TestCase):

    def test_gob_point(self):
        GobType = get_gob_type("GOB.Geo.Point")
        self.assertEqual(GobType.name, "Point")

        self.assertEqual('POINT(112.0 22.0)', str(GobType.from_values(x=112, y=22, precision=1)))
        self.assertEqual('POINT(112.000 22.000)', str(GobType.from_values(x=112, y=22)))
        self.assertEqual('POINT(52.3063972 4.9627873)', str(GobType.from_values(x=52.3063972, y=4.9627873, precision=7)))
        self.assertEqual('POINT(52.3063972 4.9627873)',
                         str(GobType.from_values(x="52,3063972", y="4,9627873", precision=7, decimal_separator=",")))

        self.assertEqual('{"type": "Point", "coordinates": [112.0, 22.0]}', GobType.from_values(x=112, y=22).json)
        self.assertEqual('{"type": "Point", "coordinates": [52.3063972, 4.9627873]}',
                         GobType.from_values(x=52.3063972, y=4.9627873, precision=7).json)

        self.assertEqual('POINT(112.0 22.0)', str(GobType.from_value('POINT(112.0 22.0)')))
        self.assertEqual('POINT(52.3063972 4.9627873)', str(GobType.from_value('POINT(52.3063972 4.9627873)')))

        empty_point = GobType('')
        self.assertEqual('null', empty_point.json)

        mock_db_field = MagicMock(spec=geoalchemy2.elements.WKBElement)
        mock_db_field.srid = 28992
        mock_db_field.data = '01010000204071000000000000f0abfd400000000050a11d41'

        self.assertEqual('POINT (121535.000 485460.000)', str(GobType.from_value(mock_db_field)))

    def test_gob_polygon(self):
        GobType = get_gob_type("GOB.Geo.Polygon")
        self.assertEqual(GobType.name, "Polygon")

        self.assertEqual('POLYGON(112.0 22.0, 113.0 22.0, 113.0 21.0)', str(GobType.from_value('POLYGON(112.0 22.0, 113.0 22.0, 113.0 21.0)')))

        empty_polygon = GobType('')
        self.assertEqual('null', empty_polygon.json)

    def test_gob_geometry(self):
        GobType = get_gob_type("GOB.Geo.Geometry")
        self.assertEqual(GobType.name, "Geometry")

        self.assertEqual('POINT (1.000 2.000)', str(GobType.from_value('POINT (1 2)')))

        empty_geometry = GobType('')
        self.assertEqual('null', empty_geometry.json)
