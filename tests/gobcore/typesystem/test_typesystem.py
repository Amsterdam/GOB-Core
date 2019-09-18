from unittest import TestCase
from unittest.mock import patch

from gobcore.typesystem import get_modifications, get_value


class TestTypesystem(TestCase):
   
    @patch('gobcore.typesystem.get_gob_type')
    def test_get_modifications(self, mock_get_gob_type):
        self.assertEquals([], get_modifications(None, 'data', 'model'), 'Should return empty list when model is None')
        self.assertEquals([], get_modifications('entity', None, 'model'), 'Should return empty list when data is None')
        
        model = {
            'field1': {
                'type': 'the type',
            },
            'field2': {
                'type': 'field2 type',
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
        
        mock_get_gob_type.return_value.from_value = lambda x: x
        
        self.assertEqual(expected_result, get_modifications(entity, data, model))

    @patch('gobcore.typesystem.get_gob_type')
    def test_get_modifications_no_fields(self, mock_get_gob_type):

        model = {
            'field1': {
                'type': 'the type',
            },
            'field2': {
                'type': 'field2 type',
            }
        }

        entity = type('MockEntity', (object,), {
            'field1': 'oldField1value',
            'field2': 'oldField2value',
        })

        data = {}
        expected_result = []

        mock_get_gob_type.return_value.side_effect = KeyError

        self.assertEqual(expected_result, get_modifications(entity, data, model))

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