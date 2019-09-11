from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.logging.log_publisher import LogPublisher


class TestLogPublisher(TestCase):

    def test_constructor(self):
        # Test if a log publisher can be initialized
        publisher = LogPublisher(None)
        assert(publisher is not None)

    @patch('gobcore.logging.log_publisher.LogPublisher._auto_disconnect')
    @patch('gobcore.message_broker.message_broker.Connection.connect')
    @patch('gobcore.message_broker.message_broker.Connection.publish')
    def test_publish(self, patched_publish, patched_connect, patched_auto_disconnect):
        publisher = LogPublisher(None)
        publisher.publish("Level", "Message")
        assert(patched_publish.called)

    @patch('gobcore.logging.log_publisher.LogPublisher._auto_disconnect')
    @patch('gobcore.message_broker.message_broker.Connection.connect')
    @patch('gobcore.message_broker.message_broker.Connection.publish')
    def test_auto_connect(self, patched_publish, patched_connect, patched_auto_disconnect):
        publisher = LogPublisher(None)
        publisher.publish("Level", "Message")
        assert(patched_connect.called)
        assert(patched_auto_disconnect.called)

    @patch('gobcore.logging.log_publisher.threading')
    @patch('gobcore.logging.log_publisher.LogPublisher._auto_disconnect')
    @patch('gobcore.message_broker.message_broker.Connection.connect')
    @patch('gobcore.message_broker.message_broker.Connection.publish')
    def test_auto_connect_join_thread(self, patched_publish, patched_connect, patched_auto_disconnect, patch_threading):
        publisher = LogPublisher(None)

        self.assertIsNone(publisher._auto_disconnect_thread)
        publisher.publish("Level", "Message")
        self.assertEqual(publisher._auto_disconnect_thread, patch_threading.Thread.return_value)
        publisher._auto_disconnect_thread.start.assert_called_once()

        publisher._connection = None
        publisher.publish("Level", "Message 2")
        publisher._auto_disconnect_thread.join.assert_called_once()

    def test_disconnect(self):
        publisher = LogPublisher(None)
        mock_connection = MagicMock()
        publisher._connection = mock_connection

        publisher._disconnect()
        mock_connection.disconnect.assert_called_once()
        self.assertIsNone(publisher._connection)

        mock_connection.disconnect.reset_mock()
        publisher._disconnect()
        mock_connection.disconnect.assert_not_called()

    def test_auto_disconnect(self):
        publisher = LogPublisher(None)
        publisher._connection_lock = MagicMock()
        publisher._disconnect = MagicMock()
        publisher._auto_disconnect_timeout = 1

        publisher._auto_disconnect()
        publisher._disconnect.assert_called_once()
        self.assertEqual(0, publisher._auto_disconnect_timeout)
