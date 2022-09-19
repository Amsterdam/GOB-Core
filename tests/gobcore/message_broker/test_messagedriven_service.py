import unittest
from unittest.mock import MagicMock, call, ANY
from unittest.mock import patch

from tests.gobcore import fixtures

from gobcore.message_broker import messagedriven_service
from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import CONNECTION_PARAMS
from gobcore.message_broker.messagedriven_service import MessagedrivenService, _on_message, RUNS_IN_OWN_THREAD, \
    LOG_HANDLERS
from gobcore.utils import get_logger_name


class TestMessageDrivenServiceFunctions(unittest.TestCase):

    @patch("gobcore.message_broker.messagedriven_service.contains_notification", MagicMock(return_value=True))
    @patch("gobcore.message_broker.messagedriven_service.logger")
    @patch("gobcore.message_broker.messagedriven_service.Heartbeat")
    @patch("gobcore.message_broker.messagedriven_service.send_notification")
    @patch("gobcore.message_broker.messagedriven_service.process_issues")
    def test_on_message(
            self, mock_process_issues, mock_send_notification, mock_heartbeat, mock_logger
    ):
        return_message = fixtures.random_string()
        mocked_handler = MagicMock(side_effect=lambda msg: return_message)

        service = fixtures.get_service_fixture(mocked_handler)
        message = {}
        connection = AsyncConnection({})

        # setup expectations
        return_queue = service['report']
        logger_name = get_logger_name(service)

        with patch.object(connection, "publish") as mocked_publish:
            result = messagedriven_service._on_message(connection, service, message)

            # The result should be True
            self.assertTrue(result)

            # The message handler should be called with the message
            mocked_handler.assert_called_with(message)

            # The return message should be published on the return queue
            mocked_publish.assert_called_with(return_queue['exchange'], return_queue['key'], return_message)

        mock_process_issues.assert_called_with(return_message)
        mock_send_notification.assert_called_with(return_message)

        # logger asserts
        mock_logger.configure_context.assert_called_with(message, logger_name, LOG_HANDLERS)
        mock_logger.configure_context.return_value.__enter__.assert_called()
        mock_logger.configure_context.return_value.__exit__.assert_called_with(None, None, None)

        # heartbeat asserts
        mock_heartbeat.progress.assert_called_with(connection, service, message)
        mock_heartbeat.progress.return_value.__enter__.assert_called()
        mock_heartbeat.progress.return_value.__exit__.assert_called_with(None, None, None)

    @patch("gobcore.message_broker.messagedriven_service.send_notification")
    @patch("gobcore.message_broker.messagedriven_service.process_issues")
    def test_on_message_false(self, mock_process_issues, mock_send_notification):
        connection = MagicMock()
        mocked_handler = MagicMock(side_effect=lambda msg: False)
        service = fixtures.get_service_fixture(mocked_handler)
        result = messagedriven_service._on_message(connection, service, {})

        # returns False and result_msg is not further processed
        self.assertFalse(result)
        mock_process_issues.assert_not_called()
        mock_send_notification.assert_not_called()

    @patch("gobcore.message_broker.messagedriven_service.send_notification")
    @patch("gobcore.message_broker.messagedriven_service.process_issues")
    def test_on_message_empty_msg(self, mock_process_issues, mock_send_notification):
        connection = MagicMock()
        mocked_handler = MagicMock(side_effect=lambda msg: {})
        service = fixtures.get_service_fixture(mocked_handler)
        result = messagedriven_service._on_message(connection, service, {})

        # returns True, but result_msg is not further processed
        self.assertTrue(result)
        mock_process_issues.assert_not_called()
        mock_send_notification.assert_not_called()

    def test_on_message_fail(self):
        connection = MagicMock()
        mocked_handler = MagicMock(side_effect=Exception("raised with this msg"))
        service = fixtures.get_service_fixture(mocked_handler)

        with self.assertRaisesRegex(Exception, "raised with this msg"):
            _on_message(connection, service, {"some": "message"})

    @patch("gobcore.message_broker.messagedriven_service.MessagedrivenService")
    def test_messagedriven_service_wrapper(self, mock_service_class):
        services = fixtures.get_servicedefinition_fixture(lambda msg: msg)
        name = 'some name'
        params = {'param1': 'value1'}
        messagedriven_service.messagedriven_service(services, name, params)
        mock_service_class.assert_called_with(services, name, params)
        mock_service_class.return_value.start.assert_called_once()


@patch("gobcore.message_broker.messagedriven_service.initialize_message_broker")
class TestMessageDrivenService(unittest.TestCase):

    def test_get_service(self, _):
        services = {
            "a": {
                "queue": "q1",
                "handler": "h1",
            },
            "b": {
                "queue": lambda: "q2",
                "handler": "h2",
            }
        }
        messagedriven_service = MessagedrivenService(services, 'name', {})
        self.assertTrue(messagedriven_service._get_service("q1") == services["a"])

        # Queue lambda function is replaced with its result
        self.assertTrue(messagedriven_service._get_service("q2") == {
            **services["b"],
            "queue": "q2"
        })

    @patch("gobcore.message_broker.messagedriven_service.AsyncConnection")
    def test_messagedriven_service(self, mocked_connection, mock_init_broker):
        return_message = fixtures.random_string()
        mocked_handler = MagicMock(side_effect=lambda msg: return_message)
        service_definition = fixtures.get_servicedefinition_fixture(mocked_handler)

        expected_queue = list(service_definition.values())[0]['queue']

        messagedriven_service = MessagedrivenService(service_definition, 'Any name')
        messagedriven_service._init = MagicMock()
        messagedriven_service._start_threads = MagicMock()
        messagedriven_service._heartbeat_loop = MagicMock()
        messagedriven_service.keep_running = False
        messagedriven_service.start()

        mock_init_broker.assert_called_with()
        mocked_connection.assert_called_with(CONNECTION_PARAMS, {})
        mocked_connection.return_value.__enter__.return_value.subscribe.assert_called_with([expected_queue], ANY)

        messagedriven_service._heartbeat_loop.assert_called_once()
        messagedriven_service._start_threads.assert_not_called()

    def test_messagedriven_service_multithreaded(self, mock_init_broker):
        mocked_handler = MagicMock(side_effect=lambda msg: "returned message")
        service_definition = fixtures.get_servicedefinition_fixture(mocked_handler)

        messagedriven_service = MessagedrivenService(service_definition, 'Some name', {'thread_per_service': True})
        messagedriven_service._init = MagicMock()
        messagedriven_service._start_threads = MagicMock()
        messagedriven_service._heartbeat_loop = MagicMock()
        messagedriven_service.keep_running = False
        messagedriven_service.start()

        mock_init_broker.assert_called_with()
        messagedriven_service._heartbeat_loop.assert_called_once()
        messagedriven_service._start_threads.assert_called_once()

    def test_messagedriven_service_start(self, _):
        services = {
            "s1": {
                'queue': "q1",
                'handler': MagicMock(),
                'report': {
                    'exchange': "e1",
                    'key': "k1"
                }
            },
            "s2": {
                'queue': "q2",
                'handler': MagicMock(),
                RUNS_IN_OWN_THREAD: True,
                'report': {
                    'exchange': "e2",
                    'key': "k2"
                }
            },
            "s3": {
                'queue': "q3",
                'handler': MagicMock(),
                RUNS_IN_OWN_THREAD: False,
                'report': {
                    'exchange': "e3",
                    'key': "k3"
                }
            },
        }
        messagedriven_service = MessagedrivenService(services, 'Some name', {'thread_per_service': True})

        messagedriven_service._init = MagicMock()
        messagedriven_service._heartbeat_loop = MagicMock()
        messagedriven_service.keep_running = False

        messagedriven_service._start_threads = MagicMock()
        messagedriven_service._start_thread = MagicMock()
        messagedriven_service.start()

        messagedriven_service._start_threads.assert_called_with(['q1', 'q2', 'q3'])
        messagedriven_service._start_thread.assert_not_called()

        messagedriven_service = MessagedrivenService(services, 'Some name')

        messagedriven_service._init = MagicMock()
        messagedriven_service._heartbeat_loop = MagicMock()
        messagedriven_service.keep_running = False

        messagedriven_service._start_threads = MagicMock()
        messagedriven_service._start_thread = MagicMock()
        messagedriven_service.start()

        messagedriven_service._start_threads.assert_called_with(['q2'])
        messagedriven_service._start_thread.assert_called_with(['q1', 'q3'])

    def test_messagedriven_service_startthreads(self, _):
        messagedriven_service = MessagedrivenService({}, 'Some name', {'thread_per_service': True})
        queues = ['queue1', 'queue2']
        messagedriven_service._start_thread = MagicMock()
        messagedriven_service._start_threads(queues)

        messagedriven_service._start_thread.assert_has_calls([
            call(['queue1']),
            call(['queue2']),
        ])

    @patch("gobcore.message_broker.messagedriven_service.threading.Thread")
    def test_messagedriven_service_startthread(self, mock_thread, _):
        messagedriven_service = MessagedrivenService({}, 'Some name', {'thread_per_service': True})
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

    @patch("gobcore.message_broker.messagedriven_service.sys.exit")
    def test_init_exception(self, mock_exit, mock_init_messagebroker):
        mock_init_messagebroker.side_effect = Exception

        MessagedrivenService({}, 'name', {})

        mock_exit.assert_called_with(1)

    @patch("gobcore.message_broker.messagedriven_service._on_message")
    def test_on_message(self, mock_on_message, _):
        messagedriven_service = MessagedrivenService({}, 'name', {})
        messagedriven_service._get_service = MagicMock()
        messagedriven_service._on_message('connection', 'exchange', 'queue', 'key', 'msg')
        messagedriven_service._get_service.assert_called_with('queue')
        mock_on_message.assert_called_with('connection', messagedriven_service._get_service.return_value, 'msg')

    @patch("gobcore.message_broker.messagedriven_service.AsyncConnection")
    def test_listen(self, mock_connection, _):
        messagedriven_service = MessagedrivenService({}, 'name', {})

        class MockConnection:
            subscribe = MagicMock()

            def is_alive(self):
                # Trick to get around infinite while loop, while still being able to run the loop once
                messagedriven_service.keep_running = False
                return True

        mock_connection.return_value.__enter__.return_value = MockConnection()
        messagedriven_service.check_connection = 0

        messagedriven_service._listen(['q1', 'q1'])

    @patch("gobcore.message_broker.messagedriven_service.AsyncConnection")
    @patch("gobcore.message_broker.messagedriven_service.Heartbeat")
    def test_heartbeat_loop(self, mock_heartbeat, mock_connection, _):
        messagedriven_service = MessagedrivenService({}, 'name', {})

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
