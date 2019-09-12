import mock
import unittest
from unittest.mock import MagicMock, call

from tests.gobcore import fixtures

from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, CONNECTION_PARAMS
from gobcore.message_broker import messagedriven_service
from gobcore.message_broker.messagedriven_service import MessagedrivenService, _on_message, STATUS_FAIL


def handler(msg):

    global return_message
    return return_message


def mock_get_on_message(service):

    global return_method
    return return_method


class TestMessageDrivenServiceFunctions(unittest.TestCase):
    def test_on_message(self):

        global return_message

        # setup mocks and fixtures
        mocked_handler = mock.Mock(wraps=handler)
        service = fixtures.get_service_fixture(mocked_handler)
        single_service = [v for v in service.values()][0]

        message = {}
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
            mocked_publish.assert_called_with(return_queue['exchange'], return_queue['key'], return_message)

    @mock.patch("gobcore.message_broker.messagedriven_service.Heartbeat")
    def test_on_message_fail(self, mock_heartbeat):
        connection = MagicMock()
        service = {
            'handler': MagicMock(side_effect=Exception('exception msg')),
        }

        with self.assertRaises(Exception):
            _on_message(connection, service, 'some message')

        mock_heartbeat.progress.assert_called_with(connection, service, 'some message', STATUS_FAIL, 'exception msg')

    @mock.patch("gobcore.message_broker.messagedriven_service.MessagedrivenService")
    def test_messagedriven_service_wrapper(self, mock_service_class):
        services = ['service 1', 'service 2']
        name = 'some name'
        params = {'param1': 'value1'}
        messagedriven_service.messagedriven_service(services, name, params)
        mock_service_class.assert_called_with(services, name, params)
        mock_service_class.return_value.start.assert_called_once()


@mock.patch("gobcore.message_broker.messagedriven_service.initialize_message_broker")
class TestMessageDrivenService(unittest.TestCase):

    def test_get_service(self, mock_init_broker):
        services = {
            "a": {
                "queue": "q1",
                "handler": "h1",
            },
            "b": {
                "queue": "q2",
                "handler": "h2",
            }
        }
        messagedriven_service = MessagedrivenService(services, 'name', {})
        self.assertTrue(messagedriven_service._get_service("q1") == services["a"])
        self.assertTrue(messagedriven_service._get_service("q2") == services["b"])

    @mock.patch("gobcore.message_broker.messagedriven_service.AsyncConnection")
    def test_messagedriven_service(self, mocked_connection, mock_init_broker):

        global return_method
        return_method = fixtures.random_string()

        service_definition = fixtures.get_service_fixture(handler)
        single_service = [v for v in service_definition.values()][0]

        expected_queue = single_service['queue']

        messagedriven_service = MessagedrivenService(service_definition, 'Any name')
        messagedriven_service._init = MagicMock()
        messagedriven_service._start_threads = MagicMock()
        messagedriven_service._heartbeat_loop = MagicMock()
        messagedriven_service.keep_running = False
        messagedriven_service.start()

        mock_init_broker.assert_called_with()
        mocked_connection.assert_called_with(CONNECTION_PARAMS, {})
        mocked_connection.return_value.__enter__.return_value.subscribe\
            .assert_called_with([expected_queue], mock.ANY)  # Inner function

        messagedriven_service._heartbeat_loop.assert_called_once()
        messagedriven_service._start_threads.assert_not_called()

    def test_messagedriven_service_multithreaded(self, mock_init_broker):
        service_definition = fixtures.get_service_fixture(handler)

        messagedriven_service = MessagedrivenService(service_definition, 'Some name', {'thread_per_service': True})
        messagedriven_service._init = MagicMock()
        messagedriven_service._start_threads = MagicMock()
        messagedriven_service._heartbeat_loop = MagicMock()
        messagedriven_service.keep_running = False
        messagedriven_service.start()

        mock_init_broker.assert_called_with()
        messagedriven_service._heartbeat_loop.assert_called_once()
        messagedriven_service._start_threads.assert_called_once()

    def test_messagedriven_service_startthreads(self, mock_init_broker):
        messagedriven_service = MessagedrivenService([], 'Some name', {'thread_per_service': True})
        queues = ['queue1', 'queue2']
        messagedriven_service._start_thread = MagicMock()
        messagedriven_service._start_threads(queues)

        messagedriven_service._start_thread.assert_has_calls([
            call(['queue1']),
            call(['queue2']),
        ])

    @mock.patch("gobcore.message_broker.messagedriven_service.threading.Thread")
    def test_messagedriven_service_startthread(self, mock_thread, mock_init_broker):
        messagedriven_service = MessagedrivenService([], 'Some name', {'thread_per_service': True})
        queues_arg = ['some queue', 'some other queue']
        mock_thread.return_value = MagicMock()
        result = messagedriven_service._start_thread(queues_arg)
        mock_thread.assert_called_with(target=messagedriven_service._listen,
                                       args=(queues_arg,), name="QueueHandler")
        mock_thread.return_value.start.assert_called_once()
        self.assertEqual(mock_thread.return_value, result)

        self.assertEqual([{
            'queues': queues_arg,
            'thread': mock_thread.return_value,
        }], messagedriven_service.threads)

    @mock.patch("gobcore.message_broker.messagedriven_service.sys.exit")
    def test_init_exception(self, mock_exit, mock_init_messagebroker):
        mock_init_messagebroker.side_effect = Exception

        MessagedrivenService([], 'name', {})

        mock_exit.assert_called_with(1)

    @mock.patch("gobcore.message_broker.messagedriven_service._on_message")
    def test_on_message(self, mock_on_message, mock_init_messagebroker):
        messagedriven_service = MessagedrivenService([], 'name', {})
        messagedriven_service._get_service = MagicMock()
        messagedriven_service._on_message('connection', 'exchange', 'queue', 'key', 'msg')
        messagedriven_service._get_service.assert_called_with('queue')
        mock_on_message.assert_called_with('connection', messagedriven_service._get_service.return_value, 'msg')

    @mock.patch("gobcore.message_broker.messagedriven_service.AsyncConnection")
    def test_listen(self, mock_connection, mock_init_messagebroker):
        messagedriven_service = MessagedrivenService([], 'name', {})

        class MockConnection:
            subscribe = MagicMock()

            def is_alive(self):
                # Trick to get around infinite while loop, while still being able to run the loop once
                messagedriven_service.keep_running = False
                return True

        mock_connection.return_value.__enter__.return_value = MockConnection()
        messagedriven_service.check_connection = 0

        messagedriven_service._listen(['q1', 'q1'])

    @mock.patch("gobcore.message_broker.messagedriven_service.AsyncConnection")
    @mock.patch("gobcore.message_broker.messagedriven_service.Heartbeat")
    def test_heartbeat_loop(self, mock_heartbeat, mock_connection, mock_init_messagebroker):
        messagedriven_service = MessagedrivenService([], 'name', {})

        class MockConnection:
            subscribe = MagicMock()

            def is_alive(self):
                # Trick to get around infinite while loop, while still being able to run the loop once
                messagedriven_service.keep_running = False
                return True

        mock_connection.return_value.__enter__.return_value = MockConnection()
        messagedriven_service.check_connection = 0
        messagedriven_service.heartbeat_interval = 0

        healthy_thread = type('MockThread', (object,), {'is_alive': lambda: True})
        unhealthy_thread = type('MockThread', (object,), {'is_alive': lambda: False})

        messagedriven_service.threads = [
            {'thread': healthy_thread, 'queues': 'the queues'},
            {'thread': unhealthy_thread, 'queues': 'the queues 2'},
        ]
        messagedriven_service._start_thread = MagicMock(return_value=healthy_thread)

        messagedriven_service._heartbeat_loop()

        messagedriven_service._start_thread.assert_has_calls([call('the queues 2')])
        mock_heartbeat.return_value.send.assert_called_once()