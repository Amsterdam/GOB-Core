import mock
import unittest

from tests.gobcore import fixtures

from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, CONNECTION_PARAMS
from gobcore.message_broker import messagedriven_service


def handler(msg):

    global return_message
    return return_message


def mock_get_on_message(service):

    global return_method
    return return_method


class TestMessageDrivenService(unittest.TestCase):

    def test_get_service(self):
        services = {
            "a": {
                "exchange": "e1",
                "queue": "q1",
                "key": "k1"
            },
            "b": {
                "exchange": "e2",
                "queue": "q2",
                "key": "#"
            }
        }
        self.assertTrue(messagedriven_service._get_service(services, "e1", "q1", "k1") == services["a"])
        self.assertTrue(messagedriven_service._get_service(services, "e2", "q2", "xyz") == services["b"])

    def test_on_message(self):

        global return_message

        # setup mocks and fixtures
        mocked_handler = mock.Mock(wraps=handler)
        service = fixtures.get_service_fixture(mocked_handler)
        single_service = [v for v in service.values()][0]

        message = fixtures.random_string()
        queue = {'name': single_service['queue']}
        key = {'name': single_service['key']}
        connection = AsyncConnection({})

        # setup expectations
        return_message = fixtures.random_string()
        return_queue = single_service['report']

        with mock.patch.object(connection, "publish") as mocked_publish:

            # on_message = messagedriven_service._get_on_message(single_service)
            result = messagedriven_service._on_message(connection, single_service, message)

            # The result should be True
            self.assertTrue(result)

            # The message handler should be called with the message
            mocked_handler.assert_called_with(message)

            # The return message should be published on the return queue
            mocked_publish.assert_called_with(return_queue, return_queue['key'], return_message)

    @mock.patch("gobcore.message_broker.messagedriven_service.AsyncConnection")
    def test_messagedriven_service(self, mocked_connection):

        global return_method
        return_method = fixtures.random_string()

        service_definition = fixtures.get_service_fixture(handler)
        single_service = [v for v in service_definition.values()][0]

        expected_key = single_service['key']
        expected_queue = single_service['queue']
        expected_exchange = single_service['exchange']

        messagedriven_service.keep_running = False
        messagedriven_service.messagedriven_service(service_definition)

        mocked_connection.assert_called_with(CONNECTION_PARAMS)
        mocked_connection.return_value.__enter__.return_value.subscribe\
            .assert_called_with([{'exchange': expected_exchange,
                                  'name': expected_queue,
                                  'key': expected_key}], mock.ANY)  # Inner function
