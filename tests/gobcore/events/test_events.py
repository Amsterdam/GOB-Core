import unittest
import json
from unittest.mock import patch

from gobcore import events
from gobcore.events import GOB, GobEvent, database_to_gobevent
from gobcore.exceptions import GOBException

from tests.gobcore import fixtures

def dict_to_object(dict):
    class Obj(object):
        def __init__(self, dict):
            self.__dict__ = dict
            self.eventid = 1
    return Obj(dict)


class TestEvents(unittest.TestCase):

    def test_get_event(self):
        for event in events.GOB_EVENTS:
            self.assertEqual(events._get_event(f"{event.name}"), event)

        with self.assertRaises(GOBException):
            events._get_event(fixtures.random_string())

    def test_get_event_for_creates_add(self):
        old_data = None
        new_data = fixtures.get_data_fixture()
        modifications = []

        event = events.get_event_for(old_data, new_data, modifications, '0.9')
        self.assertEqual(event["event"], "ADD")
        self.assertEqual(event['version'], '0.9')

    def test_get_event_for_creates_confirm(self):
        new_data = fixtures.get_data_fixture()
        old_data = fixtures.get_entity_fixture(new_data)
        modifications = []

        event = events.get_event_for(old_data, new_data, modifications, '0.9')
        self.assertEqual(event["event"], "CONFIRM")
        self.assertEqual(event['version'], '0.9')

    def test_get_event_for_creates_modify(self):
        new_data = fixtures.get_data_fixture()
        old_data = fixtures.get_entity_fixture(fixtures.get_data_fixture())
        modifications = [{'key': 'value'}]

        event = events.get_event_for(old_data, new_data, modifications, '0.9')
        self.assertEqual(event["event"], "MODIFY")
        self.assertEqual(event['version'], '0.9')

    def test_get_event_for_creates_delete(self):
        new_data = None
        old_data = fixtures.get_entity_fixture(fixtures.get_data_fixture())
        modifications = []

        event = events.get_event_for(old_data, new_data, modifications, '0.9')
        self.assertEqual(event["event"], "DELETE")
        self.assertEqual(event['version'], '0.9')

    def test_get_event_class_for(self):
        with self.assertRaises(GOBException):
            events._get_event_class_for(has_old_state=False, has_new_state=False,
                                        has_modifications=fixtures.random_bool())

        self.assertEqual(events._get_event_class_for(has_old_state=True, has_new_state=False,
                                                     has_modifications=fixtures.random_bool()), GOB.DELETE)
        self.assertEqual(events._get_event_class_for(has_old_state=False, has_new_state=True,
                                                     has_modifications=fixtures.random_bool()), GOB.ADD)

        self.assertEqual(events._get_event_class_for(has_old_state=True, has_new_state=True, has_modifications=False),
                         GOB.CONFIRM)
        self.assertEqual(events._get_event_class_for(has_old_state=True, has_new_state=True, has_modifications=True),
                         GOB.MODIFY)

    def test_GobEvent(self):
        event = fixtures.get_event_fixture()
        metadata = fixtures.get_metadata_fixture()

        gob_event = GobEvent(event, metadata)

        self.assertIsInstance(gob_event, events._get_event(event['event']))
        self.assertEqual(event['data'], gob_event._data)
        self.assertEqual(metadata, gob_event._metadata)

    @patch('gobcore.events.GOBModel')
    @patch('gobcore.events.GobEvent')
    @patch('gobcore.events.MessageMetaData')
    def test_database_to_gobevent(self, mock_message_meta_data, mock_gob_event, mock_model):
        mock_model().get_collection.return_value = {
            'version': '0.1'
        }

        mock_event = {
            'version': '0.1',
            'catalogue': 'test_catalogue',
            'application': 'TEST',
            'entity': 'test_entity',
            'timestamp': None,
            'source': 'test',
            'action': 'ADD',
            'source_id': 'source_id',
        }

        data = {
            'entity': {
                '_version': '0.1'
            }
        }
        mock_event['contents'] = data
        event = dict_to_object(mock_event)

        expected_event_msg = {
            'event': event.action,
            'data': data
        }
        expected_meta_data = mock_message_meta_data.return_value = 'meta_data'

        database_to_gobevent(event)

        mock_gob_event.assert_called_with(expected_event_msg, expected_meta_data)

    @patch('gobcore.events.GOBMigrations')
    @patch('gobcore.events.GOBModel')
    @patch('gobcore.events.GobEvent')
    @patch('gobcore.events.MessageMetaData')
    def test_database_to_gobevent_migration(self, mock_message_meta_data, mock_gob_event, mock_model, mock_migrations):
        target_version = '0.2'

        mock_model().get_collection.return_value = {
            'version': target_version
        }

        mock_migrations().migrate_event_data.return_value = {
            'entity': {
                '_version': target_version
            }
        }

        mock_event = {
            'version': '0.1',
            'catalogue': 'test_catalogue',
            'application': 'TEST',
            'entity': 'test_entity',
            'timestamp': None,
            'source': 'test',
            'action': 'ADD',
            'source_id': 'source_id',
            'contents': None
        }

        data = {
            'entity': {
                '_version': '0.1'
            }
        }
        # Create JSON string. Force unpacking. Test above handles default dict case
        mock_event['contents'] = json.dumps(data)
        event = dict_to_object(mock_event)

        expected_event_msg = {
            'event': event.action,
            'data': {
                'entity': {
                    '_version': target_version
                }
            }
        }
        expected_meta_data = mock_message_meta_data.return_value = 'meta_data'

        database_to_gobevent(event)

        mock_migrations().migrate_event_data.assert_called_with(event, data, event.catalogue, event.entity,
                                                                target_version)

        mock_gob_event.assert_called_with(expected_event_msg, expected_meta_data)
