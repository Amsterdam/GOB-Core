import unittest
from unittest.mock import patch

from gobcore.model import FIELD
from gobcore.model.sa.indexes import _get_special_column_type, get_indexes


class TestIndexes(unittest.TestCase):

    def setUp(self):
        pass

    @patch("gobcore.model.sa.indexes.is_gob_geo_type", lambda x: x is not None and x.startswith('geo'))
    @patch("gobcore.model.sa.indexes.is_gob_json_type", lambda x: x is not None and x.startswith('json'))
    def test_get_special_column_type(self):
        self.assertEqual("geo", _get_special_column_type("geocolumn"))
        self.assertEqual("json", _get_special_column_type("jsoncolumn"))
        self.assertIsNone(_get_special_column_type(None))

    class MockModelForGetIndexes:
        model = {
            'catalog_a': {
                'abbreviation': 'ca',
                'collections': {
                    'collection_a1': {
                        'abbreviation': 'coa1',
                        'all_fields': {
                            FIELD.ID: {'type': 'GOB.String'},
                            FIELD.APPLICATION: {'type': 'GOB.String'},
                            'a': {'type': 'GOB.String'}
                        }
                    },
                    'collection_a2': {
                        'abbreviation': 'coa2',
                        'all_fields': {
                            FIELD.GOBID: {'type': 'GOB.String'},
                            FIELD.SEQNR: {'type': 'GOB.Integer'},
                            FIELD.ID: {'type': 'GOB.String'},
                            'reference': {
                                'type': 'GOB.Reference',
                                'ref': 'catalog_a:collection_a1'
                            },
                            'manyreference': {
                                'type': 'GOB.ManyReference',
                                'ref': 'catalog_a:collection_a1'
                            },
                            'geocol': {
                                'type': 'GOB.Geometry'
                            },
                            'json': {
                                'type': 'GOB.JSON'
                            },
                            'str': {
                                'type': 'GOB.String'
                            }
                        }
                    }
                }
            },
            'catalog_b': {
                'abbreviation': 'cb',
                'collections': {
                    'collection_b3': {
                        'abbreviation': 'cob3',
                        'all_fields': {
                            FIELD.EXPIRATION_DATE: {'type': 'GOB.DateTime'}
                        }
                    }
                }
            },
            'rel': {
                'abbreviation': 'rel',
                'collections': {
                    'relation_a': {
                        'abbreviation': 'ra',
                        'all_fields': {
                            FIELD.SOURCE_VALUE: {'type': 'GOB.String'},
                            FIELD.GOBID: {'type': 'GOB.String'},
                            FIELD.EXPIRATION_DATE: {'type': 'GOB.DateTime'}
                        }
                    },
                    'relation_b': {
                        'abbreviation': 'rb',
                        'all_fields': {
                            f'src{FIELD.ID}': {'type': 'GOB.String'},
                            f'src{FIELD.SEQNR}': {'type': 'GOB.Integer'},
                            f'dst{FIELD.ID}': {'type': 'GOB.String'},
                            f'dst{FIELD.SEQNR}': {'type': 'GOB.Integer'},
                            f'src{FIELD.SOURCE}': {'type': 'GOB.String'},
                            FIELD.SOURCE_VALUE: {'type': 'GOB.String'},
                            FIELD.APPLICATION: {'type': 'GOB.String'},
                            FIELD.LAST_SRC_EVENT: {'type': 'GOB.Integer'}
                        }
                    }
                }
            }
        }

        def __getitem__(self, catalog_name):
            return self.model[catalog_name]

        def items(self):
            return self.model.items()

        def get_table_name(self, catalog_name, collection_name):
            return f'{catalog_name}_{collection_name}'.lower()

        def get_table_name_from_ref(self, ref: str):
            return ref.replace(':', '_')

        def get_collection_from_ref(self, ref: str):
            cat, coll = self.get_catalog_collection_names_from_ref(ref)
            try:
                return self.model[cat]['collections'][coll]
            except KeyError:
                return

        def get_catalog_collection_names_from_ref(self, ref: str):
            spl = ref.split(':')
            return spl[0], spl[1]

    class MockModelDstNotKnown(MockModelForGetIndexes):
        model = {
            'catalog_a': {
                'abbreviation': 'ca',
                'collections': {
                    'collection_a2': {
                        'abbreviation': 'coa2',
                        'all_fields': {
                            'reference_not_known': {
                                'type': 'GOB.Reference',
                                'ref': 'catalog_b:collection_a2b'
                            }
                        }
                    }
                }
            }
        }

    class MockSourcesForGetIndexes():
        sources = {
            'catalog_a': {
                'collection_a2': {
                    'reference': [{
                        'destination_attribute': 'a',
                    }],
                    'manyreference': [{
                        'source_attribute': 'str',
                        'destination_attribute': 'a',
                    }]
                }
            }
        }

        def __init__(self, model):
            pass

        def get_field_relations(self, catalog_name, collection_name, col):
            try:
                return self.sources[catalog_name][collection_name][col]
            except KeyError:
                return []

    @patch("gobcore.model.sa.indexes.GOBSources", MockSourcesForGetIndexes)
    def test_get_indexes(self):
        result = get_indexes(self.MockModelForGetIndexes())

        expected = {
            'ca_coa1_b80bb7740288fda1f201890375a60c8f': {
                'columns': (
                    '_id',
                ),
                'table_name': 'catalog_a_collection_a1'
            },
            'ca_coa1_3676d55f84497cbeadfc614c1b1b62fc': {
                'columns': (
                    '_application',
                ),
                'table_name': 'catalog_a_collection_a1'
            },
            'ca_coa1_37abd7da5cbd49b20a1090ba960d82e7': {
                'columns': [
                    '_source',
                    '_last_event DESC'
                ],
                'table_name': 'catalog_a_collection_a1'
            },
            'ca_coa1_ed3f22b3eec2fb035647f924a5b2136e': {
                'columns': [
                    "COALESCE(_expiration_date, '9999-12-31'::timestamp without time zone)"
                ],
                'table_name': 'catalog_a_collection_a1'
            },
            'ca_coa2_b80bb7740288fda1f201890375a60c8f': {
                'columns': (
                    '_id',
                ),
                'table_name': 'catalog_a_collection_a2'
            },
            'ca_coa2_d05569f886377400312d8c2edd4c6f4c': {
                'columns': (
                    '_gobid',
                ),
                'table_name': 'catalog_a_collection_a2'
            },
            'ca_coa2_2a4dbedb477015cfe2b9f2c990906f44': {
                'columns': (
                    '_id',
                    'volgnummer'
                ),
                'table_name': 'catalog_a_collection_a2'
            },
            'ca_coa2_37abd7da5cbd49b20a1090ba960d82e7': {
                'columns': [
                    '_source',
                    '_last_event DESC'
                ],
                'table_name': 'catalog_a_collection_a2'
            },
            'ca_coa2_b8af13ea9c8fe890c9979a1fa8dbde22': {
                'table_name': 'catalog_a_collection_a2',
                'columns': [
                    'reference'
                ],
                'type': 'json'
            },
            'ca_coa1_0cc175b9c0f1b6a831c399e269772661': {
                'table_name': 'catalog_a_collection_a1',
                'columns': [
                    'a'
                ],
                'type': None
            },
            'ca_coa2_341be97d9aff90c9978347f66f945b77': {
                'table_name': 'catalog_a_collection_a2',
                'columns': [
                    'str'
                ],
                'type': None
            },
            'ca_coa2_ed3f22b3eec2fb035647f924a5b2136e': {
                'columns': [
                    "COALESCE(_expiration_date, '9999-12-31'::timestamp without time zone)"
                ],
                'table_name': 'catalog_a_collection_a2'
            },
            'cb_cob3_1a9d849ff5a68997176b6144236806ae': {
                'columns': (
                    '_expiration_date',
                ),
                'table_name': 'catalog_b_collection_b3'
            },
            'cb_cob3_37abd7da5cbd49b20a1090ba960d82e7': {
                'columns': [
                    '_source',
                    '_last_event DESC'
                ],
                'table_name': 'catalog_b_collection_b3'
            },
            'cb_cob3_ed3f22b3eec2fb035647f924a5b2136e': {
                'columns': [
                    "COALESCE(_expiration_date, '9999-12-31'::timestamp without time zone)"
                ],
                'table_name': 'catalog_b_collection_b3'
            },
            'rel_relation_a_d41d8cd9_d05569f886377400312d8c2edd4c6f4c': {
                'columns': (
                    '_gobid',
                ),
                'table_name': 'rel_relation_a'
            },
            'rel_relation_a_d41d8cd9_1a9d849ff5a68997176b6144236806ae': {
                'columns': (
                    '_expiration_date',
                ),
                'table_name': 'rel_relation_a'
            },
            'rel_relation_a_d41d8cd9_ab35fb2f74ba637ec5dff03e521947fc': {
                'columns': (
                    'bronwaarde',
                ),
                'table_name': 'rel_relation_a'
            },
            'rel_relation_a_d41d8cd9_4acfc3d0636d198ba3ed562be2273f9e': {
                'columns': (
                    '_gobid',
                    '_expiration_date'
                ),
                'table_name': 'rel_relation_a'
            },
            'rel_relation_a_d41d8cd9_37abd7da5cbd49b20a1090ba960d82e7': {
                'columns': [
                    '_source',
                    '_last_event DESC'
                ],
                'table_name': 'rel_relation_a'
            },
            'rel_relation_a_d41d8cd9_ed3f22b3eec2fb035647f924a5b2136e': {
                'columns': [
                    "COALESCE(_expiration_date, '9999-12-31'::timestamp without time zone)"
                ],
                'table_name': 'rel_relation_a'
            },
            'rel_relation_b_d41d8cd9_3676d55f84497cbeadfc614c1b1b62fc': {
                'columns': (
                    '_application',
                ),
                'table_name': 'rel_relation_b'
            },
            'rel_relation_b_d41d8cd9_ab35fb2f74ba637ec5dff03e521947fc': {
                'columns': (
                    'bronwaarde',
                ),
                'table_name': 'rel_relation_b'
            },
            'rel_relation_b_d41d8cd9_dc79a884dc55f09863437f9198baf021': {
                'columns': (
                    '_last_src_event',
                ),
                'table_name': 'rel_relation_b'
            },
            'rel_relation_b_d41d8cd9_37abd7da5cbd49b20a1090ba960d82e7': {
                'columns': [
                    '_source',
                    '_last_event DESC'
                ],
                'table_name': 'rel_relation_b'
            },
            'rel_relation_b_d41d8cd9_ed3f22b3eec2fb035647f924a5b2136e': {
                'columns': [
                    "COALESCE(_expiration_date, '9999-12-31'::timestamp without time zone)"
                ],
                'table_name': 'rel_relation_b'
            }
        }
        self.assertEqual(result, expected)

    @patch("gobcore.model.sa.indexes.GOBSources", MockSourcesForGetIndexes)
    def test_get_indexes_dst_not_defined(self):
        model = self.MockModelDstNotKnown()
        result = get_indexes(model)
