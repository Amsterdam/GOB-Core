import unittest
from unittest import mock

from gobcore.model.relations import _get_relation, _get_relation_name, get_relation_name, get_relations, create_relation


class TestRelations(unittest.TestCase):

    def setUp(self):
        pass

    def test_relation(self):
        global_attributes = ['source', 'id', 'derivation', 'begin_geldigheid', 'eind_geldigheid']
        src_attributes = ['src_id', 'src_source', 'src_volgnummer']
        dst_attributes = ['dst_id', 'dst_source', 'dst_volgnummer']
        attributes = global_attributes + src_attributes + dst_attributes

        result = _get_relation("name", {'type': 'GOB.DateTime'}, {'type': 'GOB.Date'})
        self.assertEqual(result['abbreviation'], "name")
        self.assertEqual(result['entity_id'], "id")
        for validity in ['begin_geldigheid', 'eind_geldigheid']:
            self.assertEqual(result['attributes'][validity]['type'], 'GOB.DateTime')
        self.assertEqual(len(result['attributes'].keys()), len(attributes))
        for attr in attributes:
            self.assertTrue(attr in result['attributes'])
            for prop in ['type', 'description']:
                self.assertTrue(prop in result['attributes'][attr])

        result = _get_relation("name", None, None)
        for validity in ['begin_geldigheid', 'eind_geldigheid']:
            self.assertEqual(result['attributes'][validity]['type'], 'GOB.Date')
        self.assertEqual(len(result['attributes'].keys()), len(attributes))

        result = _get_relation("name",  {'type': 'GOB.Date'}, None)
        for validity in ['begin_geldigheid', 'eind_geldigheid']:
            self.assertEqual(result['attributes'][validity]['type'], 'GOB.Date')

        result = _get_relation("name", None, {'type': 'GOB.Date'})
        for validity in ['begin_geldigheid', 'eind_geldigheid']:
            self.assertEqual(result['attributes'][validity]['type'], 'GOB.Date')

    def test_relation_name(self):
        model = mock.MagicMock()
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

        name = _get_relation_name(src, dst, "reference")
        expect = 'cat_col_dst_cat_dst_col_reference'
        self.assertEqual(name, expect)

        model.get_catalog.return_value = src['catalog']
        model.get_collection.return_value = src['collection']
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
        model._data = {
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
        model._extract_references.return_value = {
            "reference": {
                "type": "GOB.Reference",
                "ref": "dst_cat:dst_col"
            }
        }
        relations = get_relations(model)
        self.assertIsNotNone(relations['collections']['name'])
        self.assertEqual(len(relations['collections'].items()), 1)

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
