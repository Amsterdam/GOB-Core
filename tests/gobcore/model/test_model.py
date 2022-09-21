from unittest import TestCase
from unittest.mock import MagicMock, patch
from typing import ItemsView, KeysView
import copy

from gobcore.exceptions import GOBException
from gobcore.model import GOBModel, NoSuchCollectionException, NoSuchCatalogException, Schema
from tests.gobcore.fixtures import random_string


class TestModel(TestCase):

    def setUp(self):
        self.model = GOBModel()

    def test_fail_on_different_legacy_value(self):
        with self.assertRaisesRegex(Exception, "Tried to initialise model with different legacy setting"):
            GOBModel(legacy=True)

    def test_get_catalog_names(self):
        self.assertIn('meetbouten', self.model.get_catalog_names())

    def test_get_catalogs(self):
        catalogs = self.model.get_catalogs()
        self.assertIsInstance(catalogs.items(), type({}.items()))
        self.assertIsInstance(self.model.items(), ItemsView)
        self.assertIsInstance(self.model.keys(), KeysView)
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

    def test_legacy_attributes(self):
        GOBModel.legacy_mode = False
        model = GOBModel()
        peilmerken = model.get_collection('nap', 'peilmerken')
        self.assertTrue('legacy_attributes' in peilmerken)

        # Test legacy attributes not initialised
        self.assertTrue('ligt_in_gebieden_bouwblok' in peilmerken['all_fields'])
        self.assertFalse('ligt_in_bouwblok' in peilmerken['all_fields'])
        self.assertTrue('ligt_in_gebieden_bouwblok' in peilmerken['attributes'])
        self.assertFalse('ligt_in_bouwblok' in peilmerken['attributes'])
        self.assertFalse(all([key in peilmerken['all_fields'] for key in peilmerken['legacy_attributes']]))
        self.assertFalse(all([key in peilmerken['attributes'] for key in peilmerken['legacy_attributes']]))

        # Check that relations are build based on the correct set of attributes
        self.assertTrue('ligt_in_gebieden_bouwblok' in peilmerken['references'])
        self.assertFalse('ligt_in_bouwblok' in peilmerken['references'])

        # And we have created the correct relation tables
        self.assertIsNone(model.get_collection('rel', 'nap_pmk_gbd_bbk_ligt_in_bouwblok'))
        self.assertIsNotNone(model.get_collection('rel', 'nap_pmk_gbd_bbk_ligt_in_gebieden_bouwblok'))

        # Reset so we can make it legacy mode
        GOBModel.legacy_mode = True
        GOBModel._data = None

        # Test legacy attributes are initialised
        model = GOBModel(True)
        peilmerken = model.get_collection('nap', 'peilmerken')
        self.assertFalse('ligt_in_gebieden_bouwblok' in peilmerken['all_fields'])
        self.assertTrue('ligt_in_bouwblok' in peilmerken['all_fields'])
        self.assertFalse('ligt_in_gebieden_bouwblok' in peilmerken['attributes'])
        self.assertTrue('ligt_in_bouwblok' in peilmerken['attributes'])
        self.assertTrue(all([key in peilmerken['all_fields'] for key in peilmerken['legacy_attributes']]))
        self.assertTrue(all([key in peilmerken['attributes'] for key in peilmerken['legacy_attributes']]))

        # Check that relations are build based on the correct set of attributes
        self.assertFalse('ligt_in_gebieden_bouwblok' in peilmerken['references'])
        self.assertTrue('ligt_in_bouwblok' in peilmerken['references'])

        # And we have created the correct relation tables
        self.assertIsNotNone(model.get_collection('rel', 'nap_pmk_gbd_bbk_ligt_in_bouwblok'))
        self.assertIsNone(model.get_collection('rel', 'nap_pmk_gbd_bbk_ligt_in_gebieden_bouwblok'))

        # Reset
        GOBModel.legacy_mode = False
        GOBModel._data = None
        model = GOBModel()

    def test_init_legacy_attributes_not_set_for_schema(self):
        GOBModel.legacy_mode = True
        GOBModel._data = None
        model = GOBModel(True)

        # Prepare object. Remove legacy_attributes
        data = model.data
        data['nap']['collections']['peilmerken']['attributes'] = data['nap']['collections']['peilmerken']['legacy_attributes']
        del data['nap']['collections']['peilmerken']['legacy_attributes']

        with self.assertRaisesRegex(GOBException, "Expected 'legacy_attributes' to be defined for nap peilmerken"):
            model._init_catalog(data['nap'])

        # Reset
        GOBModel.legacy_mode = False
        GOBModel._data = None
        model = GOBModel()

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

    def test_source_id_states_other_seqnr_field(self):
        self.model.has_states = MagicMock(return_value=True)

        entity = {
            'idfield': 'idvalue',
            'nummervolg': '1'
        }
        spec = {
            'catalogue': 'meetbouten',
            'entity': 'meetbouten',
            'source': {
                'entity_id': 'idfield'
            },
            'gob_mapping': {
                'volgnummer': {
                    'source_mapping': 'nummervolg'
                }
            }
        }

        self.assertEqual("idvalue.1", self.model.get_source_id(entity, spec))
        self.model.has_states.assert_called_with('meetbouten', 'meetbouten')

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
        collections = MagicMock()
        collections.__getitem__.return_value = {"fake": "collection"}
        self.model.data = {'some': {'collections': collections}}
        self.model.split_ref = MagicMock(return_value=("some", "reference"))

        result = self.model.get_collection_from_ref("some:reference")

        self.assertEqual({"fake": "collection"}, result)
        self.model.split_ref.assert_called_with("some:reference")
        collections.__getitem__.assert_called_with("reference")

        self.model.data = {'other': {'collections': collections}}
        result = self.model.get_collection_from_ref("some:reference")
        self.assertIsNone(result)

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

        with self.assertRaisesRegex(GOBException, "Invalid table name"):
            model.get_catalog_from_table_name('brk_')

    def test_get_collection_from_table_name(self):
        model = GOBModel()
        testcases = [
            ('brk_collection', 'collection'),
            ('brk_coll_lection', 'coll_lection'),
        ]

        for arg, result in testcases:
            self.assertEqual(result, model.get_collection_from_table_name(arg))

        with self.assertRaisesRegex(GOBException, "Invalid table name"):
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
            with self.assertRaisesRegex(GOBException, "Invalid table name"):
                model._split_table_name(testcase)

    def test_get_collection_by_name(self):
        model = GOBModel()
        model.data = {
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

    @patch("gobcore.model.os.getenv", lambda x: x == 'DISABLE_TEST_CATALOGUE')
    def test_test_catalog_deleted(self):
        # Reinitialise
        GOBModel._data = None

        model = GOBModel()
        self.assertFalse('test_catalogue' in model.data)

        # Cleanup
        GOBModel._data = None

    def test_test_catalog_present(self):
        model = GOBModel()
        self.assertTrue('test_catalogue' in model.data)

    @patch('gobcore.model.load_schema')
    def test_load_schemas(self, mock_load_schema):
        mock_load_schema.return_value = {'attributes': {}, 'version': '1.0', 'entity_id': 'some_attribute'}
        model = GOBModel()
        model.data = {
            'cat_a': {
                'collections': {
                    'coll_a': {
                        'attributes': {
                            'some': 'attribute',
                        },
                    },
                    'coll_b': {
                        'legacy_attributes': {
                            'some': 'attribute',
                        },
                        'schema': {
                            'datasetId': 'the dataset',
                            'tableId': 'the table',
                            'version': '1.0'
                        },
                    }
                }
            },
        }
        expected = copy.deepcopy(model.data)
        expected['cat_a']['collections']['coll_b'] = {
            'attributes': {},
            'version': '1.0',
            'entity_id': 'some_attribute',
            'legacy_attributes': {
                'some': 'attribute',
            },
            'schema': {
                'datasetId': 'the dataset',
                'tableId': 'the table',
                'version': '1.0'
            },
        }
        model._load_schemas(model.data)
        self.assertEqual(expected, model.data)
        mock_load_schema.assert_called_with(
            Schema(datasetId='the dataset', tableId='the table', version='1.0'))

    def test_catalog_collection_from_abbr(self):
        model = GOBModel()
        model.data = {
            'cat_a': {
                'abbreviation': 'ca',
                'collections': {
                    'col_a': {
                        'abbreviation': 'coa',
                        'some other': 'data',
                    },
                    'col_b': {
                        'abbreviation': 'cob',
                        'some other': 'data',
                    }
                }
            }
        }

        self.assertEqual(({
            'abbreviation': 'ca',
            'collections': {
                'col_a': {
                    'abbreviation': 'coa',
                    'some other': 'data',
                },
                'col_b': {
                    'abbreviation': 'cob',
                    'some other': 'data',
                }
            }
        }, {
            'abbreviation': 'cob',
            'some other': 'data',
        }), model.get_catalog_collection_from_abbr('ca', 'cob'))

        with self.assertRaises(NoSuchCatalogException):
            model.get_catalog_collection_from_abbr('cc', 'cob')

        with self.assertRaises(NoSuchCollectionException):
            model.get_catalog_collection_from_abbr('ca', 'coc')

    def test_catalog_from_abbr(self):
        model = GOBModel()
        model.data = {
            'cat_a': {
                'abbreviation': 'ca',
                'collections': {
                    'col_a': {}
                }
            }
        }

        self.assertEqual(model.data['cat_a'], model.get_catalog_from_abbr('ca'))

        with self.assertRaises(NoSuchCatalogException):
            model.get_catalog_from_abbr('nonexistent')

    def test_collections_no_underscore(self):
        ignore_catalogs = {'test_catalogue', 'qa', 'rel'}
        catalogs = [cat for cat in self.model.get_catalog_names() if cat not in ignore_catalogs]

        for cat in catalogs:
            for collection in self.model.get_collection_names(cat):
                self.assertNotIn('_', collection)

    def test_reference_no_underscore(self):
        ignore_catalogs = {'test_catalogue', 'qa', 'rel'}
        catalogs = [cat for cat in self.model.get_catalog_names() if cat not in ignore_catalogs]

        for cat in catalogs:
            for collection in self.model.get_collections(cat):
                refs = self.model.get_collection(cat, collection)['references']

                for attr, ref in refs.items():
                    self.assertNotIn('_', ref['ref'], msg=f"{cat}.{collection}.{attr}")
