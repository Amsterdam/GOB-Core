import logging

from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.logging.log_publisher import LogPublisher
from gobcore.logging.logger import Logger, RequestsHandler, LoggerManager, DATAINFO, DATAWARNING, DATAERROR


class TestLogger(TestCase):

    def setUp(self):
        RequestsHandler.LOG_PUBLISHER = None
        Logger._logger = {}

    def tearDown(self):
        pass

    def test_init(self):
        logger = Logger("Any logger")
        self.assertEqual(logger._name, "Any logger")
        self.assertIsNotNone(Logger._logger["Any logger"])
        self.assertIsInstance(Logger._logger["Any logger"], logging.Logger)

    def test_get_warnings(self):
        logger = Logger()
        logger.messages['warning'] = 'warning messages'
        self.assertEqual('warning messages', logger.get_warnings())

    def test_get_errors(self):
        logger = Logger()
        logger.messages['error'] = 'error messages'
        self.assertEqual('error messages', logger.get_errors())

    def test_log(self):
        mock_level_logger = MagicMock()
        mock_logger = type('MockLogger', (object,), {'level': mock_level_logger})
        logger = Logger()
        logger._save_log = MagicMock()
        logger._name = 'name'
        Logger._logger = {logger._name: mock_logger}
        logger._default_args = {'some': 'arg'}
        logger.MAX_SIZE = 15
        logger.SHORT_MESSAGE_SIZE = 10
        message = 20 * 'a'
        short_message = (10 * 'a') + '...'

        logger._log('level', message, {'a': 'kwarg'})
        mock_level_logger.assert_called_with(short_message, extra=logger._default_args)

    def test_multiple_init(self):
        logger1 = Logger("Any logger")
        logger2 = Logger("Any other logger")
        logger3 = Logger(logger1._name)
        self.assertEqual(len(Logger._logger), 2)

    def test_info(self):
        logger = Logger("Info logger")
        with self.assertLogs(logger=Logger._logger[logger._name], level=logging.INFO) as result:
            logger.info("test")
        self.assertEqual(result.output, [f"INFO:{logger._name}:test"])

    def test_warning(self):
        logger = Logger("Warning logger")
        with self.assertLogs(logger=Logger._logger[logger._name], level=logging.INFO) as result:
            logger.warning("test")
        self.assertEqual(result.output, [f"WARNING:{logger._name}:test"])

    def test_error(self):
        logger = Logger("Error logger")
        # RequestsHandler.LOG_PUBLISHER.publish.reset_mock()
        with self.assertLogs(logger=Logger._logger[logger._name], level=logging.INFO) as result:
            logger.error("test")
        self.assertEqual(result.output, [f"ERROR:{logger._name}:test"])

    def test_data_info(self):
        logger = Logger("Data info logger")
        with self.assertLogs(logger=Logger._logger[logger._name], level=DATAINFO) as result:
            logger.data_info("test")
        self.assertEqual(result.output, [f"DATAINFO:{logger._name}:test"])

    def test_data_warning(self):
        logger = Logger("Data warning logger")
        with self.assertLogs(logger=Logger._logger[logger._name], level=DATAWARNING) as result:
            logger.data_warning("test")
        self.assertEqual(result.output, [f"DATAWARNING:{logger._name}:test"])

    def test_data_error(self):
        logger = Logger("Data error logger")
        # RequestsHandler.LOG_PUBLISHER.publish.reset_mock()
        with self.assertLogs(logger=Logger._logger[logger._name], level=DATAERROR) as result:
            logger.data_error("test")
        self.assertEqual(result.output, [f"DATAERROR:{logger._name}:test"])

    def test_configure(self):
        RequestsHandler.LOG_PUBLISHER = MagicMock(spec=LogPublisher)
        RequestsHandler.LOG_PUBLISHER.publish = MagicMock()

        logger = Logger("Config logger")
        msg = {
            "header": {
                'process_id': 'any process_id',
                'source': 'any source',
                'destination': 'any destination',
                'application': 'any application',
                'catalogue': 'any catalogue',
                'entity': 'any entity',
                'jobid': None,
                'stepid': None
            },
            "some": "other"
        }
        logger.configure(msg)
        self.assertEqual(logger._default_args, msg["header"])

        logger.warning("test")
        RequestsHandler.LOG_PUBLISHER.publish.assert_called_once()
        level, args = RequestsHandler.LOG_PUBLISHER.publish.call_args[0]
        self.assertEqual(level, "WARNING")
        self.assertIsNotNone(args["timestamp"])
        self.assertEqual(args["level"], "WARNING")
        self.assertEqual(args["name"], logger._name)
        self.assertEqual(args["msg"], "test")
        self.assertIsNotNone(args["formatted_msg"])
        self.assertEqual(args["process_id"], msg["header"]["process_id"])
        self.assertEqual(args["source"], msg["header"]["source"])
        self.assertEqual(args["application"], msg["header"]["application"])
        self.assertEqual(args["catalogue"], msg["header"]["catalogue"])
        self.assertEqual(args["entity"], msg["header"]["entity"])

        RequestsHandler.LOG_PUBLISHER.publish.reset_mock()

        logger.info("test", {"data": "any data", "id": "any id", "destination": "any destination"})
        RequestsHandler.LOG_PUBLISHER.publish.assert_called_once()
        level, args = RequestsHandler.LOG_PUBLISHER.publish.call_args[0]
        self.assertEqual(args["data"], "any data")
        self.assertEqual(args["id"], "any id")
        self.assertEqual(args["destination"], "any destination")

        RequestsHandler.LOG_PUBLISHER.publish.reset_mock()

        logger.error("any error msg", {
            **msg.get('header', {}),
            "data": {
                "error": "any error"
            }
        })
        level, args = RequestsHandler.LOG_PUBLISHER.publish.call_args[0]
        self.assertEqual(args["msg"], "any error msg")
        self.assertEqual(args["source"], msg["header"]["source"])
        self.assertEqual(args["data"], {"error": "any error"})

    def test_configure_with_name(self):
        RequestsHandler.LOG_PUBLISHER = MagicMock(spec=LogPublisher)
        RequestsHandler.LOG_PUBLISHER.publish = MagicMock()

        logger = Logger()
        msg = {
            "header": {
                'process_id': 'any process_id',
                'source': 'any source',
                'application': 'any application',
                'catalogue': 'any catalogue',
                'entity': 'any entity'
            },
            "some": "other"
        }
        logger.configure(msg, "Another config logger")

        logger.warning("test")
        RequestsHandler.LOG_PUBLISHER.publish.assert_called_once()
        level, args = RequestsHandler.LOG_PUBLISHER.publish.call_args[0]
        self.assertEqual(args["name"], logger._name)
        self.assertEqual(logger.get_name(), args["name"])
        self.assertEqual(logger.get_attribute('source'), 'any source')

    def test_add_issue(self):
        logger = Logger()
        self.assertEqual(logger.get_issues(), [])
        any_issue = MagicMock()
        any_issue.get_unique_id.return_value = 1

        logger.add_issue(any_issue)
        self.assertEqual(logger.get_issues(), [any_issue])

        logger.add_issue(any_issue)
        self.assertEqual(logger.get_issues(), [any_issue])

        another_issue = MagicMock()
        another_issue.get_unique_id.return_value = 1

        logger.add_issue(another_issue)
        self.assertEqual(logger.get_issues(), [any_issue])

        another_issue.get_unique_id.return_value = 2
        logger.add_issue(another_issue)
        self.assertEqual(logger.get_issues(), [any_issue, another_issue])


class TestRequestHandler(TestCase):

    def setUp(self):
        RequestsHandler.LOG_PUBLISHER = None
        Logger._logger = {}

    def tearDown(self):
        pass

    def test_init(self):
        request_handler = RequestsHandler()
        self.assertIsNone(request_handler.LOG_PUBLISHER)

    @patch("gobcore.logging.logger.LogPublisher")
    def test_emit(self, mock_log_publisher):
        mock_log_publisher = MagicMock(spec=LogPublisher)
        request_handler = RequestsHandler()
        record = MagicMock()

        request_handler.emit(record)
        self.assertIsNotNone(request_handler.LOG_PUBLISHER)

        # Reuse log publisher once it is created
        request_handler.LOG_PUBLISHER.publish = MagicMock()
        request_handler.emit(record)
        request_handler.LOG_PUBLISHER.publish.assert_called()


class TestLoggerManager(TestCase):
    class MockThread():
        def __init__(self, id):
            self.ident = id

    @patch("gobcore.logging.logger.Logger")
    @patch("gobcore.logging.logger.threading.current_thread")
    def test_get_logger(self, mock_current_thread, mock_logger):
        logger_manager = LoggerManager()

        mock_current_thread.side_effect = [self.MockThread(1), self.MockThread(2), self.MockThread(1)]

        res = logger_manager.get_logger()
        self.assertEqual({1: mock_logger.return_value}, logger_manager.loggers)
        self.assertEqual(mock_logger.return_value, res)

        res = logger_manager.get_logger()
        self.assertEqual([1, 2], list(logger_manager.loggers.keys()))
        self.assertEqual(mock_logger.return_value, res)

        # Assert we still have two loggers
        res = logger_manager.get_logger()
        self.assertEqual([1, 2], list(logger_manager.loggers.keys()))

    def test_proxy_method(self):
        logger_manager = LoggerManager()
        logger_manager.get_logger = MagicMock()

        logger_manager.abc()
        logger_manager.get_logger.return_value.abc.assert_called_once()

        logger_manager.ghi(1, 2, 3, kw=4, kw2=5)
        logger_manager.get_logger.return_value.ghi.assert_called_with(1, 2, 3, kw=4, kw2=5)
