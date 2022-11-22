"""GOBModel tests."""

from unittest import TestCase
from unittest.mock import Mock, MagicMock, patch
from typing import ItemsView, KeysView
import copy

from gobcore.exceptions import GOBException
from gobcore.model import GOBModel, NoSuchCollectionException, NoSuchCatalogException, Schema
from tests.gobcore.fixtures import random_string



class TestUserDict(TestCase):
    """GOBModel UserDict tests."""

    def setUp(self):
        self.gob_model = GOBModel()

    def test_model_data(self):
        """GOBModel data check."""
        self.assertIs(GOBModel.data, self.gob_model.data)

    def test_model_catalogs(self):
        """GOBModel catalog checks."""
        # Catalog count.
        self.assertEqual(len(self.gob_model), 14, msg="catalog count has changed")
        # Catalog 'doesnotexist" should not exist.
        self.assertIsNone(
            self.gob_model.get('doesnotexist'), msg="Catalog 'doesnotexist' should not exist!")

    def test_model_iter(self):
        """GOBModel catalog and collection iteration checks."""
        for catalog_name in self.gob_model:
            # __getitem__
            self.assertIs(self.gob_model[catalog_name], self.gob_model.get(catalog_name))
            # __contains__
            self.assertIn(catalog_name, self.gob_model)
            #
            # Catalog collection checks.
            for collection in self.gob_model[catalog_name]['collections']:
                # The number of collections should be more than 8!?
                self.assertTrue(
                    len(self.gob_model[catalog_name]['collections'][collection]) > 8,
                    msg=f"Not enough collections for {catalog_name}!?"
                )


class TestModel(TestCase):
    """GOBModel non legacy mode tests."""

    def setUp(self):
        self.model = GOBModel()

    def test_singleton(self):
        gob_model = GOBModel()
        self.assertIs(self.model, gob_model)

    def test_fail_on_different_legacy_value(self):
        with self.assertRaisesRegex(
                Exception, "Tried to initialise model with different legacy setting"):
            GOBModel(legacy=True)

    def test_contains(self):
        self.assertIn('meetbouten', self.model)

    def test_items_and_keys(self):
        self.assertIsInstance(self.model.items(), ItemsView)
        self.assertIsInstance(self.model.keys(), KeysView)
        self.assertIn('meetbouten', self.model.keys())

    def test_collections_contains(self):
        self.assertIn('collections', self.model['meetbouten'])
        self.assertIn('meetbouten', self.model['meetbouten']['collections'])

    def test_get_table_names(self):
        self.assertIn('meetbouten_meetbouten', self.model.get_table_names())

    def test_get_table_name(self):
        self.assertEqual(
            'meetbouten_meetbouten', self.model.get_table_name('meetbouten', 'meetbouten'))

    def test_get_reference_by_abbreviations(self):
        # Test lower and upper case
        self.assertEqual(
            'meetbouten:meetbouten', self.model.get_reference_by_abbreviations('mbn', 'mbt'))
        self.assertEqual(
            'meetbouten:meetbouten', self.model.get_reference_by_abbreviations('mbn', 'MBT'))

        # Test non existing abbreviation
        self.assertEqual(None, self.model.get_reference_by_abbreviations('xxx', 'xxx'))

    def test_legacy_attributes(self):
        peilmerken = self.model['nap']['collections']['peilmerken']
        self.assertTrue('legacy_attributes' in peilmerken)

        # Test legacy attributes not initialised
        self.assertTrue('ligt_in_gebieden_bouwblok' in peilmerken['all_fields'])
        self.assertFalse('ligt_in_bouwblok' in peilmerken['all_fields'])
        self.assertTrue('ligt_in_gebieden_bouwblok' in peilmerken['attributes'])
        self.assertFalse('ligt_in_bouwblok' in peilmerken['attributes'])
        self.assertFalse(
            all(key in peilmerken['all_fields'] for key in peilmerken['legacy_attributes']))
        self.assertFalse(
            all(key in peilmerken['attributes'] for key in peilmerken['legacy_attributes']))

        # Check that relations are build based on the correct set of attributes
        self.assertTrue('ligt_in_gebieden_bouwblok' in peilmerken['references'])
        self.assertFalse('ligt_in_bouwblok' in peilmerken['references'])

        # And we have created the correct relation tables
        with self.assertRaises(KeyError):
            _ = self.model['rel']['collections']['nap_pmk_gbd_bbk_ligt_in_bouwblok']
        self.assertIsNotNone(
            self.model['rel']['collections']['nap_pmk_gbd_bbk_ligt_in_gebieden_bouwblok'])

        # Reset GOBModel so we can make it legacy mode
        GOBModel._initialised = False
        self.model = GOBModel(True)

        # Test legacy attributes are initialised
        peilmerken = self.model['nap']['collections']['peilmerken']
        self.assertFalse('ligt_in_gebieden_bouwblok' in peilmerken['all_fields'])
        self.assertTrue('ligt_in_bouwblok' in peilmerken['all_fields'])
        self.assertFalse('ligt_in_gebieden_bouwblok' in peilmerken['attributes'])
        self.assertTrue('ligt_in_bouwblok' in peilmerken['attributes'])
        self.assertTrue(
            all(key in peilmerken['all_fields'] for key in peilmerken['legacy_attributes']))
        self.assertTrue(
            all(key in peilmerken['attributes'] for key in peilmerken['legacy_attributes']))

        # Check that relations are build based on the correct set of attributes
        self.assertFalse('ligt_in_gebieden_bouwblok' in peilmerken['references'])
        self.assertTrue('ligt_in_bouwblok' in peilmerken['references'])

        # And we have created the correct relation tables
        self.assertIsNotNone(self.model['rel']['collections']['nap_pmk_gbd_bbk_ligt_in_bouwblok'])
        with self.assertRaises(KeyError):
            _ = self.model['rel']['collections']['nap_pmk_gbd_bbk_ligt_in_gebieden_bouwblok']

        # Reset GOBModel for further testing
        GOBModel._initialised = False

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
        self.model.has_states = Mock(
            spec_set=GOBModel.has_states, name="has_states", return_value=True)

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

    def test_has_states_catalog_keyerror(self):
        self.assertFalse(self.model.has_states('nonexistingcatalog', 'collection'))

    def test_source_id_states_other_seqnr_field(self):
        self.model.has_states = Mock(
            spec_set=GOBModel.has_states, name="has_states", return_value=True)

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
        collections = MagicMock(spec_set=dict, name="collections")
        collections.__getitem__.return_value = {"fake": "collection"}
        self.model.data = {'some': {'collections': collections}}
        self.model.split_ref = Mock(
            spec_set=self.model.split_ref, name="split_ref", return_value=("some", "reference"))

        result = self.model.get_collection_from_ref("some:reference")

        self.assertEqual({"fake": "collection"}, result)
        self.model.split_ref.assert_called_with("some:reference")
        collections.__getitem__.assert_called_with("reference")

        self.model.data = {'other': {'collections': collections}}
        result = self.model.get_collection_from_ref("some:reference")
        self.assertIsNone(result)

        # Reset GOBModel data
        GOBModel._initialised = False

    @patch("gobcore.model.get_inverse_relations")
    def test_get_inverse_relations(self, mock_get_inverse_relations):
        self.assertEqual(mock_get_inverse_relations.return_value, self.model.get_inverse_relations())

        # Call twice. Expect same result
        self.assertEqual(mock_get_inverse_relations.return_value, self.model.get_inverse_relations())
        # But should only be evaluated once
        mock_get_inverse_relations.assert_called_once()

    def test_get_catalog_from_table_name(self):
        testcases = [
            ('brk_something', 'brk'),
            ('brk_long_table_name', 'brk'),
        ]

        for arg, result in testcases:
            self.assertEqual(result, self.model.get_catalog_from_table_name(arg))

        with self.assertRaisesRegex(GOBException, "Invalid table name"):
            self.model.get_catalog_from_table_name('brk_')

    def test_get_collection_from_table_name(self):
        testcases = [
            ('brk_collection', 'collection'),
            ('brk_coll_lection', 'coll_lection'),
        ]

        for arg, result in testcases:
            self.assertEqual(result, self.model.get_collection_from_table_name(arg))

        with self.assertRaisesRegex(GOBException, "Invalid table name"):
            self.model.get_collection_from_table_name('brk_')

    def test_split_table_name(self):
        testcases = [
            'brk_',
            '_brk',
            '',
            '_',
            'brk'
        ]

        for testcase in testcases:
            with self.assertRaisesRegex(GOBException, "Invalid table name"):
                self.model._split_table_name(testcase)

    def test_catalog_collection_names_from_ref(self):
        self.model.split_ref = Mock(spec_set=self.model.split_ref, name="split_ref")
        ref = random_string()
        result = self.model.get_catalog_collection_names_from_ref(ref)
        self.model.split_ref.assert_called_with(ref)
        self.assertEqual(self.model.split_ref.return_value, result)

    @patch("gobcore.model.os.getenv", lambda x: x == 'DISABLE_TEST_CATALOGUE')
    def test_test_catalog_deleted(self):
        # Reinitialise without test catalog
        GOBModel._initialised = False
        model = GOBModel()

        self.assertFalse('test_catalogue' in model.data)

        # Reset GOBModel data
        GOBModel._initialised = False

    def test_test_catalog_present(self):
        self.assertTrue('test_catalogue' in self.model)

    @patch('gobcore.model.load_schema')
    def test_load_schemas(self, mock_load_schema):
        mock_load_schema.return_value = {
            'attributes': {}, 'version': '1.0', 'entity_id': 'some_attribute'}
        self.model.data = {
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
        expected = copy.deepcopy(self.model.data)
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
        self.model._load_schemas(self.model.data)
        self.assertEqual(expected, self.model.data)
        mock_load_schema.assert_called_with(
            Schema(datasetId='the dataset', tableId='the table', version='1.0'))

        # Reset GOBModel data
        GOBModel._initialised = False

    def test_catalog_collection_from_abbr(self):
        self.model.data = {
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
        }), self.model.get_catalog_collection_from_abbr('ca', 'cob'))

        with self.assertRaises(NoSuchCatalogException):
            self.model.get_catalog_collection_from_abbr('cc', 'cob')

        with self.assertRaises(NoSuchCollectionException):
            self.model.get_catalog_collection_from_abbr('ca', 'coc')

        # Reset GOBModel data
        GOBModel._initialised = False

    def test_catalog_from_abbr(self):
        self.model.data = {
            'cat_a': {
                'abbreviation': 'ca',
                'collections': {
                    'col_a': {}
                }
            }
        }

        self.assertIs(self.model['cat_a'], self.model.get_catalog_from_abbr('ca'))
        with self.assertRaises(NoSuchCatalogException):
            self.model.get_catalog_from_abbr('nonexistent')

        # Reset GOBModel data
        GOBModel._initialised = False

    def test_collections_no_underscore(self):
        ignore_catalogs = {'test_catalogue', 'qa', 'rel'}
        catalogs = [cat for cat in self.model if cat not in ignore_catalogs]

        for cat in catalogs:
            for collection in self.model[cat]['collections']:
                self.assertNotIn('_', collection)

    def test_reference_no_underscore(self):
        ignore_catalogs = {'test_catalogue', 'qa', 'rel'}
        catalogs = [cat for cat in self.model if cat not in ignore_catalogs]

        for cat in catalogs:
            for collection in self.model[cat]['collections']:
                refs = self.model[cat]['collections'][collection]['references']

                for attr, ref in refs.items():
                    self.assertNotIn('_', ref['ref'], msg=f"{cat}.{collection}.{attr}")


class TestLegacyModel(TestCase):
    """GOBModel legacy mode tests."""

    @staticmethod
    def setUpClass():
        # Reset from TestModel
        GOBModel._initialised = False

    @staticmethod
    def tearDownClass():
        # Reset for further testing
        GOBModel._initialised = False

    def setUp(self):
        self.model = GOBModel(True)

    def test_singleton(self):
        gob_model = GOBModel(True)
        self.assertIs(self.model, gob_model)

    def test_fail_on_different_legacy_value(self):
        with self.assertRaisesRegex(
                Exception, "Tried to initialise model with different legacy setting"):
            GOBModel()
