import unittest
from unittest.mock import MagicMock, patch

import geoalchemy2

from gobcore.typesystem import _gob_types_dict, get_gob_type, is_gob_geo_type
from gobcore.typesystem.gob_geotypes import GEOType, Point, Polygon, Geometry, GobTypeJSONEncoder
from gobcore.exceptions import GOBException


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

    def test_is_gob_geo_type(self):
        all_types = _gob_types_dict.keys()

        for type in all_types:
            # Same test, different approach
            is_geo = type.startswith("GOB.Geo.")
            self.assertTrue(is_geo == is_gob_geo_type(type))

    def _get_mock_geotype(self, value):
        class MockChild(GEOType):

            def from_value(cls, value):
                super().from_value(value)

            def from_values(cls, **values):
                super().from_values(**values)

        return MockChild(value)

    @patch('gobcore.typesystem.gob_geotypes.geoalchemy2.WKTElement')
    def test_to_wkt_elem(self, mock_wkt_element):
        geotype = self._get_mock_geotype('value')
        res = geotype._to_wkt_elem('val')

        mock_wkt_element.assert_called_with('val', srid=geotype._srid)
        self.assertEqual(res, mock_wkt_element.return_value)

    def test_to_db(self):
        geotype = self._get_mock_geotype('value')
        geotype._to_wkt_elem = MagicMock()
        geotype._string = None

        self.assertIsNone(geotype.to_db)

        geotype._string = 'string'
        res = geotype.to_db
        geotype._to_wkt_elem.assert_called_with(geotype._string, geotype._srid)
        self.assertEqual(res, geotype._to_wkt_elem.return_value)

    def test_to_value(self):
        geotype = self._get_mock_geotype('value')
        geotype._string = 'some string'
        self.assertEqual(geotype._string, geotype.to_value)


class TestPoint(unittest.TestCase):

    @patch('gobcore.typesystem.gob_geotypes.json.loads')
    def test_from_value(self, mock_json_loads):
        with self.assertRaises(ValueError):
            Point.from_value('val')

        # Any TypeError in try block should just pass through silently
        mock_json_loads.side_effect = TypeError
        res = Point.from_value('POINT(112.0 22.0)')

    def test_from_values(self):
        with self.assertRaisesRegexp(GOBException, "Missing required key"):
            Point.from_values()


class TestPolygon(unittest.TestCase):

    def test_from_value(self):
        with self.assertRaises(ValueError):
            Polygon.from_value('val')

        mock_db_field = MagicMock(spec=geoalchemy2.elements.WKBElement)
        mock_db_field.srid = 28992
        mock_db_field.data = '01010000204071000000000000f0abfd400000000050a11d41'

        res = Polygon.from_value(mock_db_field)

    @patch('gobcore.typesystem.gob_geotypes.json.dumps')
    @patch('gobcore.typesystem.gob_geotypes.json.loads')
    def test_from_value_dict(self, mock_loads, mock_dumps):
        mock_dumps.return_value = 'value'
        mock_loads.side_effect = TypeError

        Polygon.from_value({'dict': 'value'})
        mock_dumps.assert_called_with({'dict': 'value'}, cls=GobTypeJSONEncoder)

    def test_from_values(self):
        with self.assertRaises(ValueError):
            Polygon.from_values()


class TestGeometry(unittest.TestCase):

    def test_from_value(self):
        with self.assertRaises(ValueError):
            Polygon.from_value('val')

        mock_db_field = MagicMock(spec=geoalchemy2.elements.WKBElement)
        mock_db_field.srid = 28992
        mock_db_field.data = '01010000204071000000000000f0abfd400000000050a11d41'

        res = Polygon.from_value(mock_db_field)

    def test_from_value_errors(self):
        # TODO
        pass