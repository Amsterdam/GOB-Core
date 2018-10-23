import sqlalchemy
import unittest

from gobcore.exceptions import GOBException
from gobcore.typesystem import get_gob_type_from_sql_type, _gob_postgres_sql_types_list


class TestSQLTypes(unittest.TestCase):

    def test_sql_types(self):
        for mapping in _gob_postgres_sql_types_list:
            gob_type = get_gob_type_from_sql_type(mapping['sql_type'])
            self.assertEqual(gob_type, mapping['gob_type'])

    def test_unknown_sql_type(self):
        with self.assertRaises(GOBException):
            get_gob_type_from_sql_type(sqlalchemy.types.ARRAY)
