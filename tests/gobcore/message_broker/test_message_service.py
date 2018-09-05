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

    def test_get_on_message(self):

        global return_message

        # setup mocks and fixtures
        mocked_handler = mock.Mock(wraps=handler)
        service = fixtures.get_service_fixture(mocked_handler)
        key = [k for k in service.keys()][0]
        single_service = [v for v in service.values()][0]

        message = fixtures.random_string()
        queue = {'name': single_service['queue']}
        connection = AsyncConnection({})

        # setup expectations
        return_message = fixtures.random_string()
        return_key = single_service['report_back']
        return_queue = {
            "exchange": WORKFLOW_EXCHANGE,
            "name": single_service['report_queue'],
            "key": return_key
        }

        with mock.patch.object(connection, "publish") as mocked_publish:

            on_message = messagedriven_service._get_on_message(service)
            result = on_message(connection, queue, key, message)

            # The result should be True
            self.assertTrue(result)

            # The message handler should be called with the message
            mocked_handler.assert_called_with(message)

            # The return message should be published on the return queue
            mocked_publish.assert_called_with(return_queue, return_key, return_message)

    @mock.patch("gobcore.message_broker.messagedriven_service.AsyncConnection")
    @mock.patch("gobcore.message_broker.messagedriven_service._get_on_message",
                side_effect=mock_get_on_message)
    def test_messagedriven_service(self, get_on_message, mocked_connection):

        global return_method
        return_method = fixtures.random_string()

        service_definition = fixtures.get_service_fixture(handler)
        expected_key = [k for k in service_definition.keys()][0]
        single_service = [v for v in service_definition.values()][0]
        expected_queue = single_service['queue']

        messagedriven_service.keep_running = False
        messagedriven_service.messagedriven_service(service_definition)

        mocked_connection.assert_called_with(CONNECTION_PARAMS)
        mocked_connection.return_value.__enter__.return_value.subscribe\
            .assert_called_with([{'exchange': WORKFLOW_EXCHANGE,
                                  'name': expected_queue,
                                  'key': expected_key}], return_method)
