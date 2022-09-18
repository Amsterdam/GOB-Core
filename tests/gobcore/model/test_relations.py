import unittest
from unittest import mock

from gobcore.model.relations import _get_relation, _get_relation_name, get_relation_name, get_relations, \
    create_relation, get_inverse_relations, get_fieldnames_for_missing_relations, split_relation_table_name, \
    get_reference_name_from_relation_table_name, _get_destination, get_relations_for_collection
from gobcore.model.name_compressor import NameCompressor
from gobcore.model import GOBModel
from gobcore.exceptions import GOBException


class TestRelations(unittest.TestCase):

    def setUp(self):
        pass

    def test_relation(self):
        global_attributes = ['id', 'derivation', 'bronwaarde']
        src_attributes = ['src_id', 'src_source', 'src_volgnummer']
        dst_attributes = ['dst_id', 'dst_source', 'dst_volgnummer']
        validity_attributes = ['begin_geldigheid', 'eind_geldigheid']
        relate_attributes = ['_last_src_event', '_last_dst_event']
        attributes = global_attributes + src_attributes + dst_attributes + validity_attributes + relate_attributes

        result = _get_relation("name")
        self.assertEqual(result['abbreviation'], "name")
        self.assertEqual(result['entity_id'], "id")
        self.assertEqual(len(result['attributes'].keys()), len(attributes))

        for attr in attributes:
            self.assertTrue(attr in result['attributes'])
            for prop in ['type', 'description']:
                self.assertTrue(prop in result['attributes'][attr])

    @mock.patch("gobcore.model.relations.NameCompressor")
    def test_relation_name(self, mock_name_compressor):
        src = {
            "catalog": {
                'abbreviation': 'cat'
            },
            "catalog_name": "catalog",
            "collection": {
                'abbreviation': 'col',
                'attributes': {
                    'reference': {
                        'ref': 'src:dst'
                    }
                }
            },
            "collection_name": "collection"
        }
        dst = {
            "catalog": {
                'abbreviation': 'dst_cat'
            },
            "catalog_name": "catalog",
            "collection": {
                'abbreviation': 'dst_col'
            },
            "collection_name": "collection"
        }

        # Assert that NameCompressor is used.
        mock_name_compressor.compress_name.side_effect = lambda s: s
        name = _get_relation_name(src, dst, "reference")
        expect = 'cat_col_dst_cat_dst_col_reference'
        self.assertEqual(name, expect)
        mock_name_compressor.compress_name.assert_called_with(expect)

        # test get_relation_name()
        model = GOBModel()
        # catalog['collections']['collection'] => src['collections']['dst']
        model.data = {
            'catalog': {'abbreviation': 'cat', 'collections': src},
            'src': {'abbreviation': 'cat',
                    'collections': {'dst': src['collection']}
                   }
        }
        name = get_relation_name(model, "catalog", "collection", "reference")
        expect = 'cat_col_cat_col_reference'
        self.assertEqual(name, expect)

    @mock.patch('gobcore.model.relations._get_relation_name')
    def test_relations(self, mock_get_relation_name):
        model = mock.MagicMock()
        relations = get_relations(model)
        expect = {
            'version': '0.1',
            'abbreviation': 'REL',
            'description': 'GOB Relations',
            'collections': {}
        }
        self.assertEqual(relations, expect)

        mock_get_relation_name.return_value = 'name'
        data = {
            "catalog": {
                "abbreviation": "cat",
                "collections": {
                    "collection": {
                        "abbreviation": "col",
                        "attributes": {}
                    }
                }
            }
        }
        model.items.return_value = data.items()
        model._extract_references.return_value = {
            "reference": {
                "type": "GOB.Reference",
                "ref": "dst_cat:dst_col"
            }
        }
        relations = get_relations(model)
        self.assertIsNotNone(relations['collections']['name'])
        self.assertEqual(len(relations['collections'].items()), 1)

    def test_split_relation_table_name(self):
        test_case = "rel_srccat_srccol_dstcat_dstcol_reference"

        res = split_relation_table_name(test_case)
        self.assertEqual({
            'src_cat_abbr': 'srccat',
            'src_col_abbr': 'srccol',
            'dst_cat_abbr': 'dstcat',
            'dst_col_abbr': 'dstcol',
            'reference_name': 'reference'
        }, res)

        with self.assertRaisesRegex(GOBException, "Invalid table name"):
            split_relation_table_name("rel_srccat_srccol_dstcat_dstcol")

    def test_get_reference_name_from_relation_table_name(self):
        test_case = "rel_srccat_srccol_dstcat_dstcol_relation_name"

        self.assertEqual("relation_name", get_reference_name_from_relation_table_name(test_case))

    def test_create_relation(self):
        src = {
            'source': 'src_source',
            'id': 'src_id',
            'volgnummer': 'src_volgnummer'
        }
        dst = {
            'source': 'dst_source',
            'id': 'dst_id',
            'volgnummer': 'dst_volgnummer'
        }
        validity = {
            'begin_geldigheid': 'begin',
            'eind_geldigheid': 'eind'
        }
        result = create_relation(src, validity, dst, "derivation")
        expect = {
            'source': 'src_source.dst_source',
            'id': 'src_id.src_volgnummer.dst_id.dst_volgnummer',
            'src_source': 'src_source',
            'src_id': 'src_id',
            'src_volgnummer': 'src_volgnummer',
            'derivation': 'derivation',
            'begin_geldigheid': 'begin',
            'eind_geldigheid': 'eind',
            'dst_source': 'dst_source',
            'dst_id': 'dst_id',
            'dst_volgnummer': 'dst_volgnummer'
        }
        self.assertEqual(result, expect)

        src['volgnummer'] = None
        result = create_relation(src, validity, dst, "derivation")
        self.assertEqual(result['id'], 'src_id.dst_id.dst_volgnummer')

        dst['volgnummer'] = None
        result = create_relation(src, validity, dst, "derivation")
        self.assertEqual(result['id'], 'src_id.dst_id')

    def test_get_inverse_relations(self):
        model = {
            "cat": {
                "collections": {
                    "entity_a": {
                        "attributes": {
                            "ref_to_d": {
                                "type": "GOB.Reference",
                                "description": "",
                                "ref": "cat2:entity_d"
                            },
                            "manyref_to_c": {
                                "type": "GOB.ManyReference",
                                "description": "",
                                "ref": "cat:entity_c"
                            }
                        }
                    },
                }
            },
            "cat2": {
                "collections": {
                    "entity_d": {
                        "attributes": {
                            "ref_to_a": {
                                "type": "GOB.Reference",
                                "description": "",
                                "ref": "cat:entity_a"
                            },
                            "manyref_to_d": {
                                "type": "GOB.ManyReference",
                                "description": "",
                                "ref": "cat2:entity_d"
                            }
                        }
                    }
                }
            }
        }

        class MockModel:
            gobmodel = GOBModel()
            data = model

            # Wire original GOBModel _extract_references method.
            def _extract_references(self, attributes):
                return self.gobmodel._extract_references(attributes)

            def items(self):
                """Fix GOBModel items method."""
                return self.data.items()

        expected_result = {
            "cat": {
                "entity_a": {
                    "cat2": {
                        "entity_d": ["ref_to_a"]
                    }
                },
                "entity_c": {
                    "cat": {
                        "entity_a": ["manyref_to_c"],
                    }
                }
            },
            "cat2": {
                "entity_d": {
                    "cat": {
                        "entity_a": ["ref_to_d"]
                    },
                    "cat2": {
                        "entity_d": ["manyref_to_d"]
                    }
                }
            }
        }
        result = get_inverse_relations(MockModel())
        self.assertEqual(expected_result, result)

    def test_get_fieldnames_for_missing_relations(self):

        model = {
            "cat": {
                "collections": {
                    "entity": {
                        "attributes": {
                            "refa": {
                                "type": "GOB.Reference",
                                "description": "",
                                "ref": "some:ref"
                            },
                            "refb": {
                                "type": "GOB.Reference",
                                "description": "",
                                "ref": "some:ref"
                            },
                            "refc": {
                                "type": "GOB.Reference",
                                "description": "",
                                "ref": "some:ref"
                            }
                        }
                    }
                }
            }
        }

        class MockModel:
            gobmodel = GOBModel()
            data = model

            # Wire original GOBModel _extract_references method.
            def _extract_references(self, attributes):
                return self.gobmodel._extract_references(attributes)

            def items(self):
                """Fix GOBModel items method."""
                return self.data.items()

        expected_result = {
            "cat": {
                "entity": ["refa", "refb"]
            }
        }

        with mock.patch("gobcore.model.relations._get_destination") as mock_get_destination, \
            mock.patch("gobcore.model.relations._get_relation_name") as mock_get_relation_name:

            mock_get_relation_name.side_effect = ["name", None, "name2"]
            mock_get_destination.side_effect = [None, "destination", "destination2"]
            result = get_fieldnames_for_missing_relations(MockModel())
            self.assertEqual(expected_result, result)

    def test_get_destination_errors(self):
        model = mock.MagicMock()

        # dst_catalog = model[dst_catalog_name]
        model.__getitem__.side_effect = KeyError
        self.assertIsNone(_get_destination(model, 'cat', 'coll'))

        # dst_catalog = model[dst_catalog_name]
        model.__getitem__.side_effect = TypeError
        self.assertIsNone(_get_destination(model, 'cat', 'coll'))
        model.__getitem__.side_effect = None

    def test_get_relations_for_collection(self):
        model = {
            "cat": {
                "abbreviation": "cat",
                "collections": {
                    "entity": {
                        "abbreviation": "ent",
                        "attributes": {
                            "refa": {
                                "type": "GOB.Reference",
                                "description": "",
                                "ref": "some:ref"
                            },
                            "refb": {
                                "type": "GOB.Reference",
                                "description": "",
                                "ref": "some:ref"
                            },
                            "refc": {
                                "type": "GOB.Reference",
                                "description": "",
                                "ref": "some:ref"
                            }
                        }
                    }
                }
            }
        }

        gobmodel = GOBModel()
        gobmodel.data = {
            'cat': model['cat'],
            'some': {'abbreviation': 'some',
                        'collections': {'ref': {'abbreviation': 'ref'}}}
        }

        expected_result = {
            "refa": "cat_ent_some_ref_refa",
            "refb": "cat_ent_some_ref_refb",
            "refc": "cat_ent_some_ref_refc",
        }

        result = get_relations_for_collection(gobmodel, 'cat', 'entity')
        self.assertEqual(expected_result, result)
