import unittest
from unittest.mock import MagicMock

import geoalchemy2

from gobcore.typesystem import get_gob_type


class TestGobGeoTypes(unittest.TestCase):

    def test_gob_point(self):
        GobType = get_gob_type("GOB.Geo.Point")
        self.assertEqual(GobType.name, "Point")

        self.assertEqual('POINT(112.0 22.0)', str(GobType.from_values(x=112, y=22)))
        self.assertEqual('POINT(52.3063972 4.9627873)', str(GobType.from_values(x=52.3063972, y=4.9627873)))
        self.assertEqual('POINT(52.3063972 4.9627873)',
                         str(GobType.from_values(x="52,3063972", y="4,9627873", decimal_separator=",")))

        self.assertEqual('{"type": "Point", "coordinates": [112.0, 22.0]}', GobType.from_values(x=112, y=22).json)
        self.assertEqual('{"type": "Point", "coordinates": [52.3063972, 4.9627873]}',
                         GobType.from_values(x=52.3063972, y=4.9627873).json)

        self.assertEqual('POINT(112.0 22.0)', str(GobType.from_value('POINT(112.0 22.0)')))
        self.assertEqual('POINT(52.3063972 4.9627873)', str(GobType.from_value('POINT(52.3063972 4.9627873)')))

        mock_db_field = MagicMock(spec=geoalchemy2.Geometry)
        mock_db_field.srid = 28992
        mock_db_field.desc = 'POINT(112.0 22.0)'

        self.assertEqual('POINT(112.0 22.0)', str(GobType.from_value(mock_db_field)))
