import unittest
import copy
from unittest.mock import MagicMock, patch

from gobcore.exceptions import GOBException
from gobcore.model import GOBModel
from gobcore.model.schema import SchemaException
from tests.gobcore.fixtures import random_string


class TestModel(unittest.TestCase):

    def setUp(self):
        self.model = GOBModel()

    def test_get_catalog_names(self):
        self.assertIn('meetbouten', self.model.get_catalog_names())

    def test_get_catalogs(self):
        catalogs = self.model.get_catalogs()
        self.assertIsInstance(catalogs.items(), type({}.items()))
        self.assertIn('meetbouten', catalogs.keys())

    def test_get_collection_names(self):
        self.assertIn('meetbouten', self.model.get_collection_names('meetbouten'))

    def test_get_table_names(self):
        self.assertIn('meetbouten_meetbouten', self.model.get_table_names())

    def test_get_table_name(self):
        self.assertEqual('meetbouten_meetbouten', self.model.get_table_name('meetbouten', 'meetbouten'))

    def test_get_reference_by_abbreviations(self):
        # Test lower and upper case
        self.assertEqual('meetbouten:meetbouten', self.model.get_reference_by_abbreviations('mbn', 'mbt'))
        self.assertEqual('meetbouten:meetbouten', self.model.get_reference_by_abbreviations('mbn', 'MBT'))

        # Test non existing abbreviation
        self.assertEqual(None, self.model.get_reference_by_abbreviations('xxx', 'xxx'))

    def test_functional_key_fields(self):
        fields = self.model.get_functional_key_fields('meetbouten', 'meetbouten')
        self.assertEqual(fields, ['_source', 'identificatie'])

    def test_functional_key_fields_state(self):
        self.model.get_collection = MagicMock(return_value={'entity_id': 'identificatie'})
        self.model.has_states = MagicMock(return_value=True)

        self.assertEqual(['_source', 'identificatie', 'volgnummer'],
                         self.model.get_functional_key_fields('cat', 'coll'))

    def test_technical_key_fields(self):
        fields = self.model.get_technical_key_fields('meetbouten', 'meetbouten')
        self.assertEqual(fields, ['_source', '_source_id'])

    def test_source_id(self):
        entity = {
            'idfield': 'idvalue'
        }
        spec = {
            'catalogue': 'meetbouten',
            'entity': 'meetbouten',
            'source': {
                'entity_id': 'idfield'
            }
        }
        source_id = self.model.get_source_id(entity, spec)
        self.assertEqual(source_id, 'idvalue')

    def test_source_id_states(self):
        self.model.has_states = MagicMock(return_value=True)

        entity = {
            'idfield': 'idvalue',
            'volgnummer': '1'
        }
        spec = {
            'catalogue': 'meetbouten',
            'entity': 'meetbouten',
            'source': {
                'entity_id': 'idfield'
            }
        }

        self.assertEqual("idvalue.1", self.model.get_source_id(entity, spec))
        self.model.has_states.assert_called_with('meetbouten', 'meetbouten')

    def test_set_api(self):
        model = {}
        self.model._set_api('meetbouten', 'meetbouten', model)
        self.assertEqual(model, {'api': {'filters': []}})

    def test_get_table_name_from_ref(self):
        self.assertEqual("abc_def", self.model.get_table_name_from_ref("abc:def"))

    def test_get_table_name_from_ref_error(self):
        self.model.split_ref = MagicMock(side_effect=GOBException)

        with self.assertRaises(GOBException):
            self.model.get_table_name_from_ref("something_invalid")

    def test_split_ref(self):
        error_testcases = [
            "abc:",
            ":abc",
            "abc"
            "abc:def:ghi"
        ]

        for testcase in error_testcases:
            with self.assertRaises(GOBException):
                self.model.split_ref(testcase)

    def test_get_collection_from_ref(self):
        self.model.get_collection = MagicMock(return_value={"fake": "collection"})
        self.model.split_ref = MagicMock(return_value=("some", "reference"))

        result = self.model.get_collection_from_ref("some:reference")

        self.assertEqual({"fake": "collection"}, result)
        self.model.split_ref.assert_called_with("some:reference")
        self.model.get_collection.assert_called_with("some", "reference")

    @patch("gobcore.model.get_inverse_relations")
    def test_get_inverse_relations(self, mock_get_inverse_relations):
        model = GOBModel()
        self.assertEqual(mock_get_inverse_relations.return_value, model.get_inverse_relations())

        # Call twice. Expect same result
        self.assertEqual(mock_get_inverse_relations.return_value, model.get_inverse_relations())
        # But should only be evaluated once
        mock_get_inverse_relations.assert_called_once()

    def test_get_catalog_from_table_name(self):
        model = GOBModel()
        testcases = [
            ('brk_something', 'brk'),
            ('brk_long_table_name', 'brk'),
        ]

        for arg, result in testcases:
            self.assertEqual(result, model.get_catalog_from_table_name(arg))

        with self.assertRaisesRegexp(GOBException, "Invalid table name"):
            model.get_catalog_from_table_name('brk_')

    def test_get_collection_from_table_name(self):
        model = GOBModel()
        testcases = [
            ('brk_collection', 'collection'),
            ('brk_coll_lection', 'coll_lection'),
        ]

        for arg, result in testcases:
            self.assertEqual(result, model.get_collection_from_table_name(arg))

        with self.assertRaisesRegexp(GOBException, "Invalid table name"):
            model.get_collection_from_table_name('brk_')

    def test_split_table_name(self):
        model = GOBModel()
        testcases = [
            'brk_',
            '_brk',
            '',
            '_',
            'brk'
        ]

        for testcase in testcases:
            with self.assertRaisesRegexp(GOBException, "Invalid table name"):
                model._split_table_name(testcase)

    def test_get_collection_by_name(self):
        model = GOBModel()
        model._data = {
            'cat_a': {
                'collections': {
                    'coll_a': 'collection a',
                    'coll_b': 'collection b'
                }
            },
            'cat_b': {
                'collections': {
                    'coll_a': 'second collection a'
                }
            }
        }

        # Success
        res = model.get_collection_by_name('coll_b')
        self.assertEqual(('cat_a', 'collection b'), res)

        # Not found
        self.assertIsNone(model.get_collection_by_name('nonexistent'))

        # Multiple found
        with self.assertRaises(GOBException):
            model.get_collection_by_name('coll_a')

    def test_catalog_collection_names_from_ref(self):
        model = GOBModel()
        model.split_ref = MagicMock()
        ref = random_string()
        result = model.get_catalog_collection_names_from_ref(ref)
        model.split_ref.assert_called_with(ref)
        self.assertEqual(model.split_ref.return_value, result)

    @patch("gobcore.model.os.getenv", lambda x, _: x == 'DISABLE_TEST_CATALOGUE')
    def test_test_catalog_deleted(self):
        # Reinitialise
        GOBModel._data = None

        model = GOBModel()
        self.assertFalse('test_catalogue' in model._data)

        # Cleanup
        GOBModel._data = None

    def test_test_catalog_present(self):
        model = GOBModel()
        self.assertTrue('test_catalogue' in model._data)

    @patch('gobcore.model.load_schema')
    def test_load_schemas(self, mock_load_schema):
        mock_load_schema.return_value = 'loaded_attributes'
        model = GOBModel()
        model._data = {
            'cat_a': {
                'collections': {
                    'coll_a': {
                        'attributes': {
                            'some': 'attribute',
                        },
                    },
                    'coll_b': {
                        '_attributes': {
                            'some': 'attribute',
                        },
                        'schema': 'schema_b',
                    }
                }
            },
        }
        expected = copy.deepcopy(model._data)
        expected['cat_a']['collections']['coll_b']['attributes'] = 'loaded_attributes'

        model._load_schemas()
        self.assertEqual(expected, model._data)

        mock_load_schema.assert_called_with('schema_b', 'cat_a', 'coll_b')

    @patch('gobcore.model.load_schema')
    @patch('builtins.print')
    def test_load_schema_error(self, mock_print, mock_load_schema):
        mock_load_schema.side_effect = SchemaException
        model = GOBModel()
        model._data = {
            'cat_a': {
                'collections': {
                    'coll_a': {
                        'attributes': {
                            'some': 'attribute',
                        },
                    },
                    'coll_b': {
                        '_attributes': {
                            'some': 'attribute',
                        },
                        'schema': 'schema_b',
                    }
                }
            },
        }
        expected = copy.deepcopy(model._data)
        expected['cat_a']['collections']['coll_b']['attributes'] = \
            expected['cat_a']['collections']['coll_b']['_attributes']

        model._load_schemas()
        self.assertEqual(expected, model._data)
        mock_print.assert_called_once()
        self.assertTrue(mock_print.call_args[0][0].startswith('ERROR: failed to load schema'))

