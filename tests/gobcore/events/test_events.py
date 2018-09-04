import unittest

from gobcore import events
from gobcore.events import GOB, GobEvent
from gobcore.exceptions import GOBException
from tests.gobcore import fixtures


class TestEvents(unittest.TestCase):

    def test_get_event(self):
        for event in events.GOB_EVENTS:
            self.assertEqual(events._get_event(f"{event.name}"), event)

        with self.assertRaises(GOBException):
            events._get_event(fixtures.random_string())

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
