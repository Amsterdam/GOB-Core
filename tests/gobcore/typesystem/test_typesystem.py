from unittest import TestCase
from unittest.mock import patch

from gobcore.typesystem import (
    GOB,
    _gob_types_dict,
    enhance_type_info,
    get_gob_type_from_info,
    get_modifications,
    get_value,
    is_gob_reference_type,
)


class TestTypesystem(TestCase):

    def test_get_modifications(self):
        """Test get_modifications with changed values."""

        self.assertEqual([], get_modifications(None, 'data', 'model'), 'Should return empty list when model is None')
        self.assertEqual([], get_modifications('entity', None, 'model'), 'Should return empty list when data is None')

        model = {
            'field1': {
                'type': 'GOB.String',
            },
            'field2': {
                'type': 'GOB.String',
            }
        }

        entity = type('MockEntity', (object,), {
            'field1': 'oldField1value',
            'field2': 'oldField2value',
        })

        data = {
            'field1': 'newField1value',
            'field2': 'oldField2value',
        }

        expected_result = [
            {'key': 'field1', 'old_value': 'oldField1value', 'new_value': 'newField1value'},
        ]

        self.assertEqual(expected_result, get_modifications(entity, data, model))

    def test_get_modifications_no_fields(self):
        """Test get_modifications with missing values."""

        model = {
            'field1': {
                'type': 'GOB.String',
            },
            'field2': {
                'type': 'GOB.String',
            }
        }

        entity = type('MockEntity', (object,), {
            'field1': 'oldField1value',
            'field2': 'oldField2value',
        })

        data = {}
        expected_result = []

        self.assertEqual(expected_result, get_modifications(entity, data, model))

    def test_get_modifications_decimal_precision(self):
        """Test get_modifications with Decimal precision."""

        model = {
            'field1': {
                'type': 'GOB.String',
            },
            'field2': {
                'type': 'GOB.Decimal',
                'precision': 2,
            }
        }

        entity = type('MockEntity', (object,), {
            'field1': 'oldField1value',
            'field2': 12.0,
        })

        data = {
            'field1': 'oldField1value',
            'field2': 12.00
        }

        self.assertEqual([], get_modifications(entity, data, model))

    def test_get_value(self):
        entity = {
            'k1': type('MockGobType', (object,), {'to_value': 'k1value'}),
            'k2': type('MockGobType', (object,), {'to_value': 'k2value'}),
            'k3': type('MockGobType', (object,), {'to_value': 'k3value'}),
        }

        self.assertEqual({
            'k1': 'k1value',
            'k2': 'k2value',
            'k3': 'k3value',
        }, get_value(entity))

    def test_is_reference_type(self):
        self.assertTrue(is_gob_reference_type("GOB.Reference"))
        self.assertFalse(is_gob_reference_type("Any other string"))
        for type_name in _gob_types_dict.keys():
            is_reference_type = type_name in ['GOB.Reference', 'GOB.ManyReference', 'GOB.VeryManyReference']
            self.assertEqual(is_gob_reference_type(type_name), is_reference_type)

    def test_enhance_type_info(self):
        type_info = {
            'type': "GOB.String"
        }
        enhance_type_info(type_info)
        self.assertEqual(type_info['gob_type'], GOB.String)

        type_info = {
            'type': "GOB.String",
            'secure': {
                'attr': {
                    'type': "GOB.String"
                }
            }
        }
        enhance_type_info(type_info)
        self.assertEqual(type_info['secure']['attr']['gob_type'], GOB.String)

        type_info = {
            'type': "GOB.String",
            'some key': {
                'secure': {
                    'attr': {
                        'type': "GOB.String"
                    }
                }
            }
        }
        enhance_type_info(type_info)
        self.assertEqual(type_info['some key']['secure']['attr']['gob_type'], GOB.String)

        type_info = {
            'type': {
                'code': {
                    'type': "GOB.String"
                }
            }
        }
        enhance_type_info(type_info)
        self.assertEqual(type_info['type']['code']['gob_type'], GOB.String)

    def test_gob_type_from_info(self):
        type_info = {
            'type': "GOB.String"
        }
        self.assertEqual(get_gob_type_from_info(type_info), GOB.String)
