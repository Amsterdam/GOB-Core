import unittest
from unittest.mock import MagicMock, patch

from gobcore.events import import_events
from gobcore.exceptions import GOBException
from gobcore.typesystem import GOB

from tests.gobcore import fixtures


class TestImportEvents(unittest.TestCase):

    def test_add_create_event(self):
        source_id = fixtures.random_string()
        event = import_events.ADD.create_event(source_id, source_id, {})
        self.assertEqual(event['event'], 'ADD')

    def test_modify_create_event(self):
        source_id = fixtures.random_string()
        event = import_events.MODIFY.create_event(source_id, source_id, {'modifications': [], '_hash': fixtures.random_string()})
        self.assertEqual(event['event'], 'MODIFY')

    def test_modify_create_event_without_modifications(self):
        source_id = fixtures.random_string()

        with self.assertRaises(GOBException):
            event = import_events.MODIFY.create_event(source_id, source_id, {'_hash': fixtures.random_string()})

    def test_delete_create_event(self):
        source_id = fixtures.random_string()
        event = import_events.DELETE.create_event(source_id, source_id, {})
        self.assertEqual(event['event'], 'DELETE')

    def test_confirm_create_event(self):
        source_id = fixtures.random_string()
        event = import_events.CONFIRM.create_event(source_id, source_id, {})
        self.assertEqual(event['event'], 'CONFIRM')

    def test_bulkconfirm_create_event(self):
        source_id = fixtures.random_string()
        event = import_events.BULKCONFIRM.create_event([])
        self.assertEqual(event['event'], 'BULKCONFIRM')

    def test_add_apply_to(self):
        source_id = fixtures.random_string()
        event = import_events.ADD.create_event(source_id, source_id, {'identificatie': source_id})

        metadata = fixtures.get_metadata_fixture()
        entity = fixtures.get_entity_fixture(event['data'])

        gob_event = import_events.ADD(event['data'], metadata)
        gob_event.apply_to(entity)

        # Assert entity has the attribute with the value
        self.assertEqual(entity.identificatie, source_id)
