import json
import unittest
import datetime
import decimal

from gobcore.typesystem.gob_geotypes import Point
from gobcore.typesystem.gob_types import String, Integer, Decimal, Boolean, JSON
from gobcore.typesystem.json import GobTypeJSONEncoder

from tests.gobcore import fixtures

class TestJsonEncoding(unittest.TestCase):

    def test_geo_json(self):

        gob_type = Point.from_values(x=1, y=2)
        geojson = json.dumps(gob_type, cls=GobTypeJSONEncoder)

        self.assertEqual('{"type": "Point", "coordinates": [1.0, 2.0]}', geojson)

    def test_decimal(self):
        dump = json.dumps(decimal.Decimal("1.5"), cls=GobTypeJSONEncoder)
        self.assertEqual(dump, "1.5")

    def test_date(self):
        dump = json.dumps(datetime.date(2000, 12, 20), cls=GobTypeJSONEncoder)
        self.assertEqual(dump, '"2000-12-20"')

    def test_datetime(self):
        dump = json.dumps(datetime.datetime(2000, 12, 20, 11, 25, 30), cls=GobTypeJSONEncoder)
        self.assertEqual(dump, '"2000-12-20T11:25:30.000000"')

    def test_normal_json(self):

        # non primitives should be quoted
        gob_type = String.from_value(123)
        to_json = json.dumps({'string': gob_type}, cls=GobTypeJSONEncoder)
        self.assertEqual('{"string": "123"}', to_json)

        # Nones should be null
        gob_type = String.from_value(None)
        to_json = json.dumps({'string': gob_type}, cls=GobTypeJSONEncoder)
        self.assertEqual('{"string": null}', to_json)

        # Integer should be primitive
        gob_type = Integer.from_value(123)
        to_json = json.dumps({'string': gob_type}, cls=GobTypeJSONEncoder)
        self.assertEqual('{"string": 123}', to_json)

        # Decimal should be primitive
        gob_type = Decimal.from_value("123,4", decimal_separator=',')
        to_json = json.dumps({'string': gob_type}, cls=GobTypeJSONEncoder)
        self.assertEqual('{"string": 123.4}', to_json)

        # Boolean should be primitive
        gob_type = Boolean.from_value("N", format='YN')
        to_json = json.dumps({'string': gob_type}, cls=GobTypeJSONEncoder)
        self.assertEqual('{"string": false}', to_json)

    def test_fallthrough(self):
        unknown_type = type('UnknownType', (object,), {})

        with self.assertRaises(TypeError):
            to_json = json.dumps({'unknown_type': unknown_type}, cls=GobTypeJSONEncoder)

    test_samples = [
        '{"coordinates": [1.0, 2.0], "type": "Point"}',
        '{"string": "123"}',
        '{"string": null}',
        '{"string": 123}',
        '{"string": 123.4}',
        '{"string": false}',
        f'["{fixtures.random_string()}", "{fixtures.random_bool()}", "{fixtures.random_string()}"]'
    ]

    def test_json_json(self):
        for json_string in self.test_samples:
            gob_type = JSON.from_value(json_string)
            to_json = json.dumps(gob_type, cls=GobTypeJSONEncoder)
            self.assertEqual(json_string, to_json)
