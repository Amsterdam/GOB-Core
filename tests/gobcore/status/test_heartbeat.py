import unittest
import mock

from gobcore.status.heartbeat import Heartbeat


class TestHeartbeat(unittest.TestCase):

    @mock.patch("atexit.register")
    @mock.patch("gobcore.message_broker.message_broker.Connection.connect")
    @mock.patch("gobcore.message_broker.message_broker.Connection.publish")
    def test_constructor(self, mocked_publish, mocked_connect, mocked_atexit_register):
        heartbeat = Heartbeat("Myname")
        mocked_connect.assert_called()
        mocked_publish.assert_called()
        mocked_atexit_register.assert_called()

    @mock.patch("atexit.register")
    @mock.patch("gobcore.message_broker.message_broker.Connection.connect")
    @mock.patch("gobcore.message_broker.message_broker.Connection.publish")
    def test_send(self, mocked_publish, mocked_connect, mocked_atexit_register):
        heartbeat = Heartbeat("Myname")
        mocked_publish.reset_mock()

        heartbeat.send()
        mocked_publish.assert_called()
        queue, key, status_msg = mocked_publish.call_args[0]
        assert(queue["name"] == "gob.status.heartbeat")
        assert(key == "HEARTBEAT")
        assert(status_msg["name"] == "Myname")
