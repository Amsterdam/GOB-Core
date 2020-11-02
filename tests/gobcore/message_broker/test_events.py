from unittest import TestCase
from gobcore.message_broker.events import get_routing_key


class TestEvents(TestCase):

    def test_get_routing_key(self):
        self.assertEqual("event.cat.coll", get_routing_key("cat", "coll"))
        self.assertEqual("event.othercat.othercoll", get_routing_key("othercat", "othercoll"))
