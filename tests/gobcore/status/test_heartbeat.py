import unittest
from unittest.mock import MagicMock, patch

from gobcore.status.heartbeat import Heartbeat, STATUS_START, STATUS_OK, STATUS_FAIL
from gobcore.message_broker.config import STATUS_EXCHANGE, HEARTBEAT_KEY


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
        return self._is_alive


def generate_threads(*specs):
    def func():
        for spec in specs:
            yield MockedThread(*spec)
    return func


class TestHeartbeat(unittest.TestCase):

    @patch("gobcore.message_broker.message_broker.Connection.connect")
    @patch("gobcore.message_broker.message_broker.Connection.publish", MagicMock())
    @patch("atexit.register")
    def test_constructor(self, mocked_atexit_register, mocked_connect):
        connection = MockHeartbeatConnection()
        _heartbeat = Heartbeat(connection, "Myname")
        mocked_connect.assert_not_called()
        mocked_atexit_register.assert_called()
        self.assertIsNotNone(connection.msg)

    @patch("gobcore.message_broker.message_broker.Connection.connect", MagicMock())
    @patch("gobcore.message_broker.message_broker.Connection.publish")
    def test_send(self, mocked_publish):
        connection = MockHeartbeatConnection()
        heartbeat = Heartbeat(connection, "Myname")
        mocked_publish.reset_mock()

        heartbeat.send()
        self.assertIsNotNone(connection.msg)
        self.assertEqual(connection.queue, STATUS_EXCHANGE)
        self.assertEqual(connection.key, HEARTBEAT_KEY)
        self.assertEqual(connection.msg["name"], "Myname")

    @patch("threading.enumerate", new=generate_threads(["a"]))
    def test_heartbeat_threads(self):
        connection = MockHeartbeatConnection()
        heartbeat = Heartbeat(connection, "Myname")
        assert len(heartbeat.threads) == 1

    @patch("threading.enumerate", new=generate_threads(["_a"]))
    def test_heartbeat_no_threads(self):
        connection = MockHeartbeatConnection()
        heartbeat = Heartbeat(connection, "Myname")
        assert len(heartbeat.threads) == 0

    @patch("gobcore.status.heartbeat.Heartbeat._progress_log_msg")
    def test_progress(self, mock_progress_log):
        connection = MockHeartbeatConnection()
        connection.publish = MagicMock()

        msg = {
            "header": {
                "jobid": "any job",
                "stepid": "any step"
            }
        }
        service = {
            "report": "any report"
        }

        Heartbeat.progress(connection, {}, {})
        connection.publish.assert_not_called()

        Heartbeat.progress(connection, service, {})
        connection.publish.assert_not_called()

        Heartbeat.progress(connection, {}, msg)
        connection.publish.assert_not_called()

        def publish_msg(status, info_msg):
            return {
                'header': {'jobid': 'any job', 'stepid': 'any step'},
                "jobid": "any job",
                "stepid": "any step",
                "status": status,
                "info_msg": info_msg
            }

        # Case without exception
        with Heartbeat.progress(connection, service, msg):
            connection.publish.assert_called_with("gob.status", "status.progress", publish_msg(STATUS_START, None))
        connection.publish.assert_called_with("gob.status", "status.progress", publish_msg(STATUS_OK, None))
        assert mock_progress_log.call_count == 2

        # Case with exception
        with self.assertRaisesRegex(ZeroDivisionError, "division by zero"):
            with Heartbeat.progress(connection, service, msg):
                connection.publish.assert_called_with("gob.status", "status.progress", publish_msg(STATUS_START, None))
                my_error = 10 / 0

            connection.publish.assert_called_with(
                "gob.status", "status.progress", publish_msg(STATUS_FAIL, "division by zero")
            )
        assert mock_progress_log.call_count == 4

    def test_log_msg(self):
        queue = "any queue"
        status = "any status"
        header = {
            'catalogue': "any catalogue"
        }
        msg = Heartbeat._progress_log_msg("any queue", "any status", header)
        self.assertTrue(queue in msg)
        self.assertTrue(status in msg)
        self.assertTrue("address: " in msg)
        self.assertTrue("dns: " in msg)
        self.assertTrue(header['catalogue'] in msg)
        self.assertTrue("collection: None" in msg)
