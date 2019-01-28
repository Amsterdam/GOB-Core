import unittest
import mock

from gobcore.status.heartbeat import Heartbeat


class MockHeartbeat:

    msg = None
    queue = None
    key = None

    def __init__(self):
        MockHeartbeat.msg = None
        MockHeartbeat.queue = None
        MockHeartbeat.key = None

    def publish(self, queue, key, msg):
        MockHeartbeat.msg = msg
        MockHeartbeat.queue = queue
        MockHeartbeat.key = key


class TestHeartbeat(unittest.TestCase):

    @mock.patch("atexit.register")
    @mock.patch("gobcore.message_broker.message_broker.Connection.connect")
    @mock.patch("gobcore.message_broker.message_broker.Connection.publish")
    def test_constructor(self, mocked_publish, mocked_connect, mocked_atexit_register):
        heartbeat = Heartbeat(MockHeartbeat(), "Myname")
        mocked_connect.assert_not_called()
        self.assertIsNotNone(MockHeartbeat.msg)
        mocked_atexit_register.assert_called()

    @mock.patch("atexit.register")
    @mock.patch("gobcore.message_broker.message_broker.Connection.connect")
    @mock.patch("gobcore.message_broker.message_broker.Connection.publish")
    def test_send(self, mocked_publish, mocked_connect, mocked_atexit_register):
        heartbeat = Heartbeat(MockHeartbeat(), "Myname")
        mocked_publish.reset_mock()

        heartbeat.send()
        self.assertIsNotNone(MockHeartbeat.msg)
        self.assertEqual(MockHeartbeat.queue["name"], "gob.status.heartbeat")
        self.assertEqual(MockHeartbeat.key, "HEARTBEAT")
        self.assertEqual(MockHeartbeat.msg["name"], "Myname")
