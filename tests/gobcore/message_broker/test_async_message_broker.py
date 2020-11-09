import json
import pika
from pika import spec
import pytest
import os

from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.message_broker import Connection


class MockChannel:

    is_open = None

    def __init__(self):
        self.is_open = True

    def close(self):
        self.is_open = False

    def basic_publish(
            self,
            exchange,
            routing_key,
            properties,
            body):
        global published_message
        published_message = body

    def consume(self, queue, no_ack=False,
                exclusive=False, arguments=None,
                inactivity_timeout=None):
        # No messages to consume
        if not published_message:
            yield (None, None, None)

        yield (spec.Basic.Deliver(delivery_tag=1, exchange="exchange", routing_key="key"),
               spec.BasicProperties(),
               published_message)

    def basic_ack(self, delivery_tag):
        global published_message
        published_message = None

    def cancel(self):
        self.is_open = False

class MockConnection:

    def __init__(self, connection_params, params=None):
        pass

    def close(self):
        pass

    def channel(self):
        return MockChannel()


published_message = None
connection_params = {'host': "host", 'credentials': {'username': "username", 'password': "password"}}


class MockPika:

    def BlockingConnection(self, params):
        self._params = params
        return MockConnection(connection_params)


def mock_connection(monkeypatch):
    _pika = MockPika()
    monkeypatch.setattr(pika, 'BlockingConnection', _pika.BlockingConnection)

    global published_message
    published_message = None


def test_connection_constructor():
    # Test if a connection can be initialized
    connection = Connection(connection_params)
    assert(connection is not None)
    assert(connection._connection_params == connection_params)


def test_disconnect():
    # Test if a disconnect can be called without an earlier connect
    connection = Connection('')
    assert(connection.disconnect() is None)


def test_idempotent_disconnect():
    # Test disconnect repeated execution
    connection = Connection('')
    assert(connection.disconnect() is None)
    assert(connection.disconnect() is None)


def test_connect_context_manager(monkeypatch):
    # Test if context manager calls disconnect on exit with statement
    mock_connection(monkeypatch)

    org_connection = None
    with patch.object(Connection, 'disconnect') as mocked_disconnect:
        with Connection(connection_params) as connection:
            org_connection = connection  # save to call the real disconnect afterwards
            pass
        assert(mocked_disconnect.called)
    org_connection.disconnect()    # Call the real disconnect


def test_publish(monkeypatch):
    mock_connection(monkeypatch)

    connection = Connection(connection_params)
    assert connection._params == {"load_message": True, "prefetch_count": 1, "stream_contents": False}
    connection.connect()
    connection.publish(exchange="exchange", key="key", msg="message")
    assert(published_message == json.dumps("message"))
    connection.disconnect()


def test_publish_failure(monkeypatch):
    mock_connection(monkeypatch)

    connection = Connection(connection_params)
    # publish should fail if we do not perform connection.connect()
    with pytest.raises(Exception):
        connection.publish(exchange="exchange", key="key", msg="message")


class TestAsyncConnection(TestCase):

    def setUp(self) -> None:
        self.connection_params = {'connection': 'params'}
        self.params = {'other': 'params'}

        self.async_connection = AsyncConnection(self.connection_params, self.params)

    def test_init(self):

        self.assertEqual({
            'load_message': True,
            'stream_contents': False,
            'other': 'params',
            'prefetch_count': 1,
        }, self.async_connection._params)
        self.assertEqual(self.connection_params, self.async_connection._connection_params)
        self.assertFalse(self.async_connection._eventloop_failed)

    def test_enter(self):
        self.async_connection.connect = MagicMock()
        self.assertEqual(self.async_connection, self.async_connection.__enter__())
        self.async_connection.connect.assert_called_once()

    def test_exit(self):
        self.async_connection.disconnect = MagicMock()
        self.async_connection.__exit__()
        self.async_connection.disconnect.assert_called_once()

    def test_is_alive(self):
        self.assertTrue(self.async_connection.is_alive())
        self.async_connection._eventloop_failed = True
        self.assertFalse(self.async_connection.is_alive())

    @patch('builtins.print')
    @patch("gobcore.message_broker.async_message_broker.threading.Thread")
    @patch("gobcore.message_broker.async_message_broker.os._exit")
    def test_on_message_redeliver(self, mock_os_exit, mock_thread, mock_print):
        msg = {'some': 'message'}
        message_handler = MagicMock()
        message_handler.side_effect = Exception
        on_message = self.async_connection.on_message('some queue', message_handler)
        channel = MagicMock()
        basic_deliver = MagicMock()
        properties = {}

        basic_deliver.redelivered = False
        on_message(channel, basic_deliver, properties, json.dumps(msg))

        thread_target = mock_thread.call_args[1]['target']
        thread_target()

        mock_os_exit.assert_called_with(os.EX_TEMPFAIL)
        print_msg = mock_print.call_args[0][0]

        self.assertEqual(print_msg, 'Message handling has failed, terminating program')

        basic_deliver.redelivered = True
        on_message(channel, basic_deliver, properties, json.dumps(msg))

        thread_target = mock_thread.call_args[1]['target']
        thread_target()

        print_msg = mock_print.call_args[0][0]

        self.assertTrue(print_msg.startswith('Message handling has failed on second try'))

        # Events should never be dropped
        basic_deliver.exchange = "gob.event"
        basic_deliver.redelivered = True
        on_message(channel, basic_deliver, properties, json.dumps(msg))

        thread_target = mock_thread.call_args[1]['target']
        thread_target()

        print_msg = mock_print.call_args[0][0]

        self.assertEqual(print_msg, 'Message handling has failed, terminating program')
