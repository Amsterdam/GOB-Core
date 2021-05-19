import random
import json
import unittest
from unittest import mock
from datetime import datetime, date

from gobcore.exceptions import GOBException, GOBTypeException
from gobcore.typesystem import get_gob_type, is_gob_json_type, _gob_types
from gobcore.typesystem.gob_types import GOBType, Boolean, Date, JSON
from tests.gobcore import fixtures


class TestGobTypes(unittest.TestCase):

    def test_gob_type_and_string(self):
        GobType = get_gob_type("GOB.String")
        self.assertEqual(GobType.name, "String")

        self.assertEqual('None', str(GobType(None)))

        fixture = fixtures.random_string()

        # Gobtype can be instantiated with a string
        gob_type = GobType(fixture)

        # Gobtype can be compared with string
        self.assertTrue(gob_type == fixture)

        # Gobtype has a json representation
        self.assertEqual(gob_type.json, f'"{fixture}"')

        # Gobtype can be constructed from a value
        gob_type1 = GobType.from_value(fixture)

        # Gobtype can be compared with other gob_type
        self.assertTrue(gob_type == gob_type1)

        # DB ouptut is string
        self.assertIsInstance(gob_type.to_db, str)

        # Gobtype cannot be instantiated with something different than a string
        with self.assertRaises(GOBException):
            GobType(fixtures.random_bool())
        with self.assertRaises(GOBException):
            GobType(random.randint(0, 10))

        # Gobtype can be constructed from a value
        gob_type1 = GobType.from_value(int)
        gob_type2 = GobType.from_value(bool)

        # JSON values are String representations
        self.assertEqual('"1"', GobType.from_value(1).json)
        self.assertEqual('"True"', GobType.from_value(True).json)

        # Gobtype can be constructed from None
        self.assertNotEqual(GobType.from_value(None), GobType.from_value("None"))

        self.assertEqual(GobType.from_value(None), GobType.from_value(None))
        self.assertEqual('null', GobType.from_value(None).json)
        self.assertEqual('"None"', GobType.from_value("None").json)
        self.assertEqual('null', GobType.from_value(float('nan')).json)

    def test_char(self):
        GobType = get_gob_type("GOB.Character")
        self.assertEqual(GobType.name, "Character")
        self.assertEqual('null', GobType.from_value(None).json)
        with self.assertRaises(GOBException):
            self.assertEqual('"1"', GobType.from_value(123).json)
        with self.assertRaises(GOBException):
            self.assertEqual('"1"', GobType.from_value("123").json)
        self.assertEqual('"1"', GobType.from_value(1).json)
        self.assertEqual('"N"', GobType.from_value('N').json)
        with self.assertRaises(GOBException):
            self.assertEqual('"O"', GobType.from_value('Overtime').json)

        # DB ouptut is string
        self.assertIsInstance(GobType.from_value('O').to_db, str)

        # Python value is string
        self.assertIsInstance(GobType.from_value('O').to_value, str)

    def test_int(self):
        GobType = get_gob_type("GOB.Integer")
        self.assertEqual(GobType.name, "Integer")
        self.assertEqual('null', GobType.from_value(None).json)
        self.assertEqual('null', GobType.from_value("nan").json)
        self.assertEqual('123', GobType.from_value(123).json)
        self.assertEqual('123', GobType.from_value("123").json)

        # DB ouptut is int
        self.assertIsInstance(GobType.from_value('123').to_db, int)

        # Python value is int
        self.assertIsInstance(GobType.from_value('123').to_value, int)

        with self.assertRaises(GOBException):
            GobType.from_value('N')
        with self.assertRaises(GOBException):
            GobType.from_value(True)
        with self.assertRaises(GOBException):
            GobType.from_value(1.3)
        with self.assertRaises(GOBException):
            GobType.from_value('Overtime')

    def test_bigint(self):
        GobType = get_gob_type("GOB.BigInteger")
        self.assertEqual(GobType.name, "BigInteger")
        self.assertEqual('null', GobType.from_value(None).json)
        self.assertEqual('null', GobType.from_value("nan").json)
        self.assertEqual('123', GobType.from_value(123).json)
        self.assertEqual('123', GobType.from_value("123").json)

        # DB ouptut is int
        self.assertIsInstance(GobType.from_value('123').to_db, int)

        # Python value is int
        self.assertIsInstance(GobType.from_value('123').to_value, int)

        with self.assertRaises(GOBException):
            GobType.from_value('N')
        with self.assertRaises(GOBException):
            GobType.from_value(True)
        with self.assertRaises(GOBException):
            GobType.from_value(1.3)
        with self.assertRaises(GOBException):
            GobType.from_value('Overtime')

    def test_decimal(self):
        GobType = get_gob_type("GOB.Decimal")
        self.assertEqual(GobType.name, "Decimal")
        self.assertEqual('null', GobType.from_value(None).json)
        self.assertEqual('null', GobType.from_value("nan").json)
        self.assertEqual('123.0', GobType.from_value(123).json)
        self.assertEqual('123.123', GobType.from_value(123.123).json)

        # DB ouptut is float
        self.assertIsInstance(GobType.from_value('123').to_db, float)

        # Python value is string
        self.assertIsInstance(GobType.from_value('123').to_value, str)

        with self.assertRaises(GOBException):
            GobType.from_value("123,123")
        self.assertEqual('123.123', GobType.from_value("123,123", decimal_separator=',').json)
        self.assertEqual('123.1', GobType.from_value("123.123", precision=1).json)
        self.assertEqual('123.1000', str(GobType.from_value("123.1", precision=4)))
        self.assertEqual('123.1', GobType.from_value("123.1", precision=4).json)

        with self.assertRaises(GOBException):
            GobType.from_value('N')
        with self.assertRaises(GOBException):
            GobType.from_value(True)
        with self.assertRaises(GOBException):
            GobType.from_value('Overtime')

    def test_boolean(self):
        GobType = get_gob_type("GOB.Boolean")

        self.assertEqual(GobType.name, "Boolean")
        self.assertEqual('null', GobType.from_value(None).json)

        self.assertEqual('true', GobType.from_value(True).json)
        self.assertEqual("False", str(GobType.from_value(False)))
        self.assertEqual('false', GobType.from_value(False).json)

        self.assertIsNone(GobType.from_value(None).to_db)
        self.assertTrue(GobType.from_value(True).to_db)
        self.assertFalse(GobType.from_value(False).to_db)

        self.assertIsNone(GobType.from_value(None).to_value)
        self.assertTrue(GobType.from_value(True).to_value)
        self.assertFalse(GobType.from_value(False).to_value)

        with self.assertRaises(GOBException):
            GobType.from_value('N')
        with self.assertRaises(GOBException):
            GobType.from_value('Overtime')
        with self.assertRaises(GOBException):
            GobType.from_value(1)
        self.assertEqual('true', GobType.from_value(1, format='10').json)
        self.assertEqual('false', GobType.from_value(0, format='10').json)
        self.assertIsNone(GobType.from_value('J', format='10').to_db)
        self.assertEqual('true', GobType.from_value('J', format='JN').json)
        self.assertEqual('false', GobType.from_value('N', format='JN').json)
        self.assertIsNone(GobType.from_value('Y', format='JN').to_db)
        self.assertEqual('true', GobType.from_value('Y', format='YN').json)
        self.assertEqual('false', GobType.from_value('N', format='YN').json)
        self.assertIsNone(GobType.from_value('J', format='YN').to_db)
        # Test unknown format
        with self.assertRaises(GOBTypeException):
            GobType.from_value('Yes', format='YesNo')

    def test_date(self):
        GobType = get_gob_type("GOB.Date")
        self.assertEqual(GobType.name, "Date")

        self.assertEqual('null', GobType.from_value(None).json)
        self.assertEqual("2016-05-04", str(GobType.from_value('2016-05-04')))
        self.assertEqual("2016-05-04", str(GobType.from_value('20160504', format="%Y%m%d")))

        self.assertEqual('"2016-05-04"', GobType.from_value('2016-05-04').json)
        self.assertEqual('"2016-05-04"', GobType.from_value('20160504', format="%Y%m%d").json)

        # Test edge case https://bugs.python.org/issue13305 strftime is not consistent for years < 1000
        self.assertEqual('"0001-05-04"', GobType.from_value('0001-05-04').json)

        with self.assertRaises(GOBException):
            GobType.from_value('N')
        with self.assertRaises(GOBException):
            GobType.from_value('Overtime')
        with self.assertRaises(GOBException):
            GobType.from_value(1)

        # DB ouptut is datetime
        self.assertIsInstance(GobType.from_value('2016-05-04').to_db, datetime)

        # Python value is datetime
        self.assertIsInstance(GobType.from_value('2016-05-04').to_value, date)

    def test_datetime(self):
        GobType = get_gob_type("GOB.DateTime")
        self.assertEqual(GobType.name, "DateTime")

        self.assertEqual('null', GobType.from_value(None).json)
        self.assertEqual("2016-05-04T12:00:00.123000", str(GobType.from_value('2016-05-04T12:00:00.123000')))
        self.assertEqual("2016-05-04T12:00:00.123000", str(GobType.from_value('20160504 12:00:00.123000', format="%Y%m%d %H:%M:%S.%f")))

        self.assertEqual("2016-05-04T12:00:00.000000", str(GobType.from_value('2016-05-04T12:00:00')))
        self.assertEqual("2016-05-04T12:00:00.000000", str(GobType.from_value('20160504 12:00:00', format="%Y%m%d %H:%M:%S")))

        self.assertEqual('"2016-05-04T12:00:00.123000"', GobType.from_value('2016-05-04T12:00:00.123000').json)
        self.assertEqual('"2016-05-04T12:00:00.123000"', GobType.from_value('20160504 12:00:00.123000', format="%Y%m%d %H:%M:%S.%f").json)

        # Test edge case https://bugs.python.org/issue13305 strftime is not consistent for years < 1000
        self.assertEqual('"0005-05-04T12:00:00.123000"', GobType.from_value('0005-05-04T12:00:00.123000').json)
        self.assertEqual('"0005-05-04T12:00:00.123000"', GobType.from_value('00050504 12:00:00.123000', format="%Y%m%d %H:%M:%S.%f").json)

        with self.assertRaises(GOBException):
            GobType.from_value('N')
        with self.assertRaises(GOBException):
            GobType.from_value('Overtime')
        with self.assertRaises(GOBException):
            GobType.from_value(1)

        # DB ouptut is datetime
        self.assertIsInstance(GobType.from_value('2016-05-04T12:00:00.123000').to_db, datetime)

        # unless an empty string is entered
        self.assertIsNone(GobType.from_value(None).to_db)

        # Python value is datetime
        self.assertIsInstance(GobType.from_value('2016-05-04T12:00:00.123000').to_value, datetime)

        # unless an empty string is entered
        self.assertIsNone(GobType.from_value(None).to_value)

    def test_json(self):
        GobType = get_gob_type("GOB.JSON")
        self.assertEqual(GobType.name, "JSON")

        self.assertEqual('null', GobType.from_value(None).json)

        key = fixtures.random_string()
        value = fixtures.random_string()

        generated_json = GobType.from_value({key: value}).json

        self.assertEqual(f'{{"{key}": "{value}"}}', GobType.from_value({key: value}).json)
        self.assertEqual(f'["{key}", "{value}"]', GobType.from_value([key, value]).json)

        value = random.randint(9,33)
        self.assertEqual(f'{{"{key}": {value}}}', GobType.from_value({key: value}))

        value = False
        self.assertEqual(f'{{"{key}": false}}', GobType.from_value({key: value}))

        value = None
        self.assertEqual(f'{{"{key}": null}}', GobType.from_value({key: value}))

        self.assertEqual(generated_json, GobType.from_value(generated_json))
        self.assertEqual(generated_json, GobType(generated_json))

        out_of_order = '{"b": "c", "a": "d"}'
        in_order = '{"a": "d", "b": "c"}'

        self.assertEqual(in_order, GobType(out_of_order))
        self.assertEqual(in_order, GobType.from_value(GobType(out_of_order)))

        # Test unknown format
        with self.assertRaises(GOBTypeException):
            GobType.from_value('{"test" = "test"}')

        # DB ouptut is json
        self.assertIsInstance(GobType.from_value('{"key": "value"}').to_db, dict)

        # Python value is dict
        self.assertIsInstance(GobType.from_value('{"key": "value"}').to_value, dict)

        self.assertEqual({"a": 1, "b": 2}, GobType.get_value(GobType('{"a": 1, "b": 2}')))
        self.assertEqual({"a": {"c": 3}, "b": 2}, GobType.get_value(GobType('{"a": {"c": 3}, "b": 2}')))
        self.assertEqual([{"a": 1}, {"b": 2}], GobType.get_value(GobType('[{"a": 1}, {"b": 2}]')))
        self.assertEqual(None, GobType.get_value(GobType(None)))

    def test_reference(self):
        GobType = get_gob_type("GOB.Reference")
        self.assertEqual(GobType.name, "Reference")

        # Test that a value of id is ignored when comparing references. Only test bronwaarde
        v1 = GobType.from_value('{"bronwaarde": "123456"}')
        v2 = GobType.from_value('{"bronwaarde": "123456", "id": "123456", "volgnummer": "1"}')
        self.assertEqual(v1, v2)

        # Test that None value is accepted
        self.assertTrue(GobType.from_value(None) == GobType.from_value(None))
        self.assertTrue(GobType.from_value({}) == GobType.from_value({}))
        self.assertTrue(GobType.from_value(None) != GobType.from_value({}))
        self.assertTrue(GobType.from_value({}) != GobType.from_value(None))
        self.assertTrue(GobType.from_value(v1) != GobType.from_value(None))
        self.assertTrue(GobType.from_value(None) != GobType.from_value(v1))

    def test_many_reference(self):
        GobType = get_gob_type("GOB.ManyReference")
        self.assertEqual(GobType.name, "ManyReference")

        # Test that a value of id is ignored when comparing references. Only test bronwaarde
        v1 = GobType.from_value('[{"bronwaarde": "123456"}, {"bronwaarde": "654321"}]')
        v2 = GobType.from_value('[{"bronwaarde": "123456", "id": "123456", "volgnummer": "1"}, {"bronwaarde": "654321", "id": "654321"}]')
        self.assertEqual(v1, v2)

        # Test that None value is accepted
        self.assertTrue(GobType.from_value(None) == GobType.from_value(None))
        self.assertTrue(GobType.from_value([]) == GobType.from_value([]))
        self.assertTrue(GobType.from_value(None) != GobType.from_value([]))
        self.assertTrue(GobType.from_value([]) != GobType.from_value(None))
        self.assertTrue(GobType.from_value(v1) != GobType.from_value(None))
        self.assertTrue(GobType.from_value(None) != GobType.from_value(v1))

    def test_incomplete_date(self):
        GobType = get_gob_type("GOB.IncompleteDate")
        self.assertEqual(GobType.name, "IncompleteDate")

        def test_result(t, year, month, day, formatted):
            self.assertEqual(year, t.year)
            self.assertEqual(month, t.month)
            self.assertEqual(day, t.day)
            self.assertEqual(json.dumps({'year': year, 'month': month, 'day': day, 'formatted': formatted}, sort_keys=True), t._string)

        t = GobType.from_value(None)
        self.assertIsNone(t.to_value)

        t = GobType.from_value("2020-03-21")
        test_result(t, 2020, 3, 21, '2020-03-21')
        t = GobType.from_value({'year': 2020, 'month': 3, 'day': 21})
        test_result(t, 2020, 3, 21, '2020-03-21')

        t = GobType.from_value("2020-03-00")
        test_result(t, 2020, 3, None, '2020-03-00')
        t = GobType.from_value({'year': 2020, 'month': None, 'day': 21})
        test_result(t, 2020, None, 21, '2020-00-21')
        t = GobType.from_value(json.dumps({'year': 2020, 'month': None, 'day': 21}))
        test_result(t, 2020, None, 21, '2020-00-21')

        with self.assertRaises(GOBTypeException):
            GobType.from_value(json.dumps({'year': 2020, 'month': None, 'day': 21})[:-2])

        with self.assertRaises(GOBTypeException):
            GobType.from_value({'year': 2020})

        with self.assertRaises(GOBTypeException):
            GobType.from_value('2020-invalid')

    def test_None_to_db(self):
        from gobcore.typesystem import gob_types, gob_geotypes
        GOB = gob_types
        GEO = gob_geotypes

        for gob_type in [
            GOB.String,
            GOB.Character,
            GOB.Integer,
            GOB.PKInteger,
            GOB.Decimal,
            GOB.Boolean,
            GOB.Date,
            GOB.JSON,
            GEO.Point
        ]:
            self.assertIsNone(gob_type(None).to_db)


    def test_is_gob_json_type(self):
        json_types = [
            "GOB.JSON",
            "GOB.Reference",
            "GOB.ManyReference",
            "GOB.VeryManyReference",
            "GOB.IncompleteDate",
        ]
        non_json_types = [t for t in _gob_types.keys() if t not in json_types]

        for t in json_types:
            self.assertTrue(is_gob_json_type(t))
        for t in non_json_types:
            self.assertFalse(is_gob_json_type(t))

        self.assertFalse(is_gob_json_type('invalid_type'))


class TestGOBType(unittest.TestCase):

    class MockChild(GOBType):
        is_secure = True

        def __init__(self, value, **kwargs):
            self.value = value
            self.kwargs = kwargs
            super().__init__(value)

        @classmethod
        def from_value(cls, value, **kwargs):
            return cls(value, **kwargs)

        def json(self):
            pass

        def to_db(self):
            pass

        def to_value(self):
            pass

    def test_from_value_secure(self):
        res = self.MockChild.from_value_secure('value', {'level': 20})

        self.assertEqual({'level': 20}, res.kwargs)
        self.assertEqual('value', res.value)

        mock_from_value = mock.MagicMock()
        self.MockChild.from_value = mock_from_value
        self.MockChild.is_secure = False
        res = self.MockChild.from_value_secure({'key': 'value'}, {'secure': 'any security info'})
        mock_from_value.assert_called_with({'key': 'value'}, secure='any security info')

        res = self.MockChild.from_value_secure({'key': 'value'}, {'attributes': 'any other attribute'})
        mock_from_value.assert_called_with({'key': 'value'}, attributes='any other attribute')


class TestBoolean(unittest.TestCase):

    def test_bool_or_none_fallthrough(self):
        self.assertIsNone(Boolean._bool_or_none('YN', 'YN'))


class TestDate(unittest.TestCase):

    def test_to_value_string_none(self):
        date = Date("the value")
        date._string = None

        self.assertIsNone(date.to_value)

class TestJSON(unittest.TestCase):

    def test_to_value_string_none(self):
        js = JSON(None)
        self.assertIsNone(js.to_value)
