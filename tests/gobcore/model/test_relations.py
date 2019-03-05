import unittest
from unittest import mock

from gobcore.model.relations import _get_relation, _get_relation_name, get_relations


class TestRelations(unittest.TestCase):

    def setUp(self):
        pass

    def test_relation(self):
        result = _get_relation("name", {'type': 'GOB.DateTime'}, {'type': 'GOB.Date'})
        expect = {
            'version': '0.1',
            'abbreviation': 'name',
            'entity_id': 'id',
            'attributes': {
                'id': {
                    'type': 'GOB.String',
                    'description': 'Functional source id, a generic (independent from Stelselpedia) id field for every entity.'
                },
                'volgnummer': {
                    'type': 'GOB.String',
                    'description': 'Uniek volgnummer van de toestand van het object.'
                },
                'derivation': {
                    'type': 'GOB.String',
                    'description': 'Describes the derivation logic for the relation (e.g. geometric, reference, ..)'
                },
                'begin_geldigheid': {
                    'type': 'GOB.DateTime',
                    'description': 'Begin relation'
                },
                'eind_geldigheid': {
                    'type': 'GOB.DateTime',
                    'description': 'End relation'
                },
                'dst_source': {
                    'type': 'GOB.String',
                    'description': 'Functional source for the specific entity, eg the name of a department.'
                },
                'dst_id': {
                    'type': 'GOB.String',
                    'description': 'Functional source id, a generic (independent from Stelselpedia) id field for every entity.'
                },
                'dst_volgnummer': {
                    'type': 'GOB.String',
                    'description': 'Uniek volgnummer van de toestand van het object.'
                }
            }
        }
        self.assertEqual(result, expect)

        result = _get_relation("name", None, None)
        expect = {
            'id': {
                'type': 'GOB.String',
                'description': 'Functional source id, a generic (independent from Stelselpedia) id field for every entity.'
            },
            'derivation': {
                'type': 'GOB.String',
                'description': 'Describes the derivation logic for the relation (e.g. geometric, reference, ..)'
            },
            'dst_source': {
                'type': 'GOB.String',
                'description': 'Functional source for the specific entity, eg the name of a department.'
            },
            'dst_id': {
                'type': 'GOB.String',
                'description': 'Functional source id, a generic (independent from Stelselpedia) id field for every entity.'
            }
        }
        self.assertEqual(result["attributes"], expect)

        result = _get_relation("name",  {'type': 'GOB.Date'}, None)
        expect = {
            'id': {
                'type': 'GOB.String',
                'description': 'Functional source id, a generic (independent from Stelselpedia) id field for every entity.'
            },
            'volgnummer': {
                'type': 'GOB.String',
                'description': 'Uniek volgnummer van de toestand van het object.'
            },
            'derivation': {
                'type': 'GOB.String',
                'description': 'Describes the derivation logic for the relation (e.g. geometric, reference, ..)'
            },
            'begin_geldigheid': {
                'type': 'GOB.Date',
                'description': 'Begin relation'
            },
            'eind_geldigheid': {
                'type': 'GOB.Date',
                'description': 'End relation'
            },
            'dst_source': {
                'type': 'GOB.String',
                'description': 'Functional source for the specific entity, eg the name of a department.'
            },
            'dst_id': {
                'type': 'GOB.String',
                'description': 'Functional source id, a generic (independent from Stelselpedia) id field for every entity.'
            }
        }
        self.assertEqual(result["attributes"], expect)

        result = _get_relation("name", None, {'type': 'GOB.Date'})
        expect = {
            'id': {
                'type': 'GOB.String',
                'description': 'Functional source id, a generic (independent from Stelselpedia) id field for every entity.'
            },
            'derivation': {
                'type': 'GOB.String',
                'description': 'Describes the derivation logic for the relation (e.g. geometric, reference, ..)'
            },
            'begin_geldigheid': {
                'type': 'GOB.Date',
                'description': 'Begin relation'
            },
            'eind_geldigheid': {
                'type': 'GOB.Date',
                'description': 'End relation'
            },
            'dst_source': {
                'type': 'GOB.String',
                'description': 'Functional source for the specific entity, eg the name of a department.'
            },
            'dst_id': {
                'type': 'GOB.String',
                'description': 'Functional source id, a generic (independent from Stelselpedia) id field for every entity.'
            },
            'dst_volgnummer': {
                'type': 'GOB.String',
                'description': 'Uniek volgnummer van de toestand van het object.'
            }
        }
        print(result["attributes"])
        self.assertEqual(result["attributes"], expect)

    def test_relation_name(self):
        model = mock.MagicMock()
        src = {
            "catalog": {
                'abbreviation': 'cat'
            },
            "catalog_name": "catalog",
            "collection": {
                'abbreviation': 'col'
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
        name = _get_relation_name(model, src, dst, "reference")
        expect = 'cat_col_dst_cat_dst_col_reference'
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
