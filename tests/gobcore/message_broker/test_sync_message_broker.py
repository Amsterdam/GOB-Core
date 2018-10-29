import json
import pika
import pytest

from mock import patch

from gobcore.message_broker.message_broker import Connection


class MockChannel:

    is_open = None

    def __init__(self):
        self.is_open = True

    def close(self):
        self.is_open = False

    def basic_publish(self,
                      exchange,
                      routing_key,
                      properties,
                      body):
        global published_message
        published_message = body


class MockConnection:

    def __init__(self):
        pass

    def close(self):
        pass

    def channel(self):
        return MockChannel()


class MockPika:

    def BlockingConnection(self, params):
        self._params = params
        return MockConnection()


published_message = None
connection_params = {'host': "host", 'credentials': {'username': "username", 'password': "password"}}


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
    # Test publish message
    mock_connection(monkeypatch)

    connection = Connection(connection_params)
    connection.connect()
    queue = {
        "exchange": "exchange",
        "name": "name",
        "key": "key"
    }
    connection.publish(queue, "key", "message")
    assert(published_message == json.dumps("message"))
    connection.disconnect()


def test_publish_failure(monkeypatch):
    # Test publish failure
    mock_connection(monkeypatch)

    connection = Connection(connection_params)
    queue = {
        "name": "name",
        "key": "key"
    }
    with pytest.raises(Exception):
        connection.publish(queue, "key", "message")
