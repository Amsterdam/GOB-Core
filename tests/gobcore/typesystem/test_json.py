import datetime
import decimal
import enum
import functools
import json
import unittest

from orjson import orjson

from gobcore.typesystem.gob_geotypes import Point
from gobcore.typesystem.gob_types import JSON, Boolean, Decimal, Integer, String
from gobcore.typesystem.json import GobTypeJSONEncoder, GobTypeORJSONEncoder
from tests.gobcore import fixtures


class MockEnum(enum.Enum):
    attribute1 = "1"
    attribute2 = 2


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

    def test_enum(self):
        dump = json.dumps(MockEnum.attribute1, cls=GobTypeJSONEncoder)
        self.assertEqual(dump, '"1"')

        dump = json.dumps(MockEnum.attribute2, cls=GobTypeJSONEncoder)
        self.assertEqual(dump, '"2"')

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

        # Decimal should be quoted
        gob_decimal = Decimal.from_value("123,4", decimal_separator=',')
        to_json = json.dumps({'string': gob_decimal}, cls=GobTypeJSONEncoder)
        self.assertEqual('{"string": "123.4"}', to_json)

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


class TestORJSONEncoding(unittest.TestCase):

    def setUp(self) -> None:
        self.kwargs = {
            "default": GobTypeORJSONEncoder(),
            "option": orjson.OPT_PASSTHROUGH_DATETIME
        }
        self.dump = functools.partial(orjson.dumps, **self.kwargs)

    def test_geo_json(self):
        assert self.dump(Point.from_values(x=1, y=2)) == b'{"type":"Point","coordinates":[1.0,2.0]}'

    def test_decimal(self):
        assert self.dump(decimal.Decimal("1.5")) == b'"1.5"'

    def test_date(self):
        assert self.dump(datetime.date(2000, 12, 20)) == b'"2000-12-20"'

    def test_datetime(self):
        assert self.dump(datetime.datetime(2000, 12, 20, 11, 25, 30)) == b'"2000-12-20T11:25:30.000000"'

    def test_enum(self):
        assert self.dump(MockEnum.attribute1) == b'"1"'
        assert self.dump(MockEnum.attribute2) == b'2'

    def test_normal_json(self):
        # non primitives should be quoted
        assert self.dump({'string': String.from_value(123)}) == b'{"string":"123"}'

        # Nones should be null
        assert self.dump({'string': String.from_value(None)}) == b'{"string":null}'

        # Integer should be primitive
        assert self.dump({'string': Integer.from_value(123)}) == b'{"string":123}'

        # Decimal should be quoted
        assert self.dump({'string': Decimal.from_value("123,40", decimal_separator=',')}) == b'{"string":"123.40"}'
        # Decimal in JSON should honor precision
        assert self.dump({'decimal': Decimal.from_value("123", precision=2)}) == b'{"decimal":"123.00"}'

        # Boolean should be primitive
        assert self.dump({"string": Boolean.from_value("N", format='YN')}) == b'{"string":false}'

    def test_fallthrough(self):
        class UnknownType:
            pass

        with self.assertRaises(TypeError, msg="Type is not JSON serializable: UnknownType"):
            self.dump({'unknown_type': UnknownType()})

    def test_json_json(self):
        samples = {
            '{"coordinates": [1.0, 2.0], "type": "Point"}': b'{"coordinates":[1.0,2.0],"type":"Point"}',
            '{"string": "123"}': b'{"string":"123"}',
            '{"string": null}': b'{"string":null}',
            '{"string": 123}': b'{"string":123}',
            '{"string": 123.4}': b'{"string":123.4}',
            '{"string": false}': b'{"string":false}',
            f'["abcd e 123 (#$&", "{True}", "asldkfj #($&^"]': b'["abcd e 123 (#$&","True","asldkfj #($&^"]'
        }
        for sample, expected in samples.items():
            assert self.dump(JSON.from_value(sample)) == expected
