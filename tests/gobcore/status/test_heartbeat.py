import collections
import unittest
import mock

from gobcore.message_broker.config import HEARTBEAT_QUEUE, get_queue
from gobcore.status.heartbeat import Heartbeat


class MockHeartbeatConnection:

    msg = None
    queue = None
    key = None

    def __init__(self):
        self.msg = None
        self.queue = None
        self.key = None

    def publish(self, queue, key, msg):
        self.msg = msg
        self.queue = queue
        self.key = key

class MockedThread:
    _is_alive = True

    def __init__(self, name):
        self.name = name

    def is_alive(self):
        return self._is_alive;


def generate_threads(*specs):
    def func():
        for spec in specs:
            yield MockedThread(*spec)
    return func

class TestHeartbeat(unittest.TestCase):

    @mock.patch("gobcore.message_broker.message_broker.Connection.connect")
    @mock.patch("gobcore.message_broker.message_broker.Connection.publish")
    def test_constructor(self, mocked_publish, mocked_connect):
        connection = MockHeartbeatConnection()
        _heartbeat = Heartbeat(connection, "Myname")
        mocked_connect.assert_not_called()
        self.assertIsNotNone(connection.msg)

    @mock.patch("gobcore.message_broker.message_broker.Connection.connect")
    @mock.patch("gobcore.message_broker.message_broker.Connection.publish")
    def test_send(self, mocked_publish, mocked_connect):
        connection = MockHeartbeatConnection()
        heartbeat = Heartbeat(connection, "Myname")
        mocked_publish.reset_mock()

        heartbeat.send()
        self.assertIsNotNone(connection.msg)
        self.assertEqual(connection.queue["name"], "gob.status.heartbeat")
        self.assertEqual(connection.key, "HEARTBEAT")
        self.assertEqual(connection.msg["name"], "Myname")

    @mock.patch("threading.enumerate", new=generate_threads(["a"]))
    def test_heartbeat_threads(self):
        connection = MockHeartbeatConnection()
        heartbeat = Heartbeat(connection, "Myname")
        assert len(heartbeat.threads) == 1

    @mock.patch("threading.enumerate", new=generate_threads(["_a"]))
    def test_heartbeat_no_threads(self):
        connection = MockHeartbeatConnection()
        heartbeat = Heartbeat(connection, "Myname")
        assert len(heartbeat.threads) == 0

    def test_progress(self):
        connection = MockHeartbeatConnection()
        connection.publish = mock.MagicMock()

        msg = {
            "header": {
                "jobid": "any job",
                "stepid": "any step"
            }
        }
        service = {
            "report": "any report"
        }

        Heartbeat.progress(None, {}, {}, None)
        connection.publish.assert_not_called()

        Heartbeat.progress(None, service, {}, None)
        connection.publish.assert_not_called()

        Heartbeat.progress(None, {}, msg, None)
        connection.publish.assert_not_called()

        Heartbeat.progress(connection, service, msg, "any status")
        connection.publish.assert_called_with({
            "exchange": "gob.status",
            "name": "gob.status.heartbeat",
            "key": "HEARTBEAT"
        },
        "PROGRESS", {
            "jobid": "any job",
            "stepid": "any step",
            "status": "any status"
        })
