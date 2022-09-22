import logging
import types
from collections import defaultdict

from unittest import TestCase
from unittest.mock import MagicMock, patch, mock_open, call

from gobcore.logging.log_publisher import LogPublisher
from gobcore.logging.logger import Logger, RequestsHandler, LoggerManager, ExtendedLogger


class TestLogger(TestCase):

    def setUp(self):
        RequestsHandler.LOG_PUBLISHER = None

    def tearDown(self):
        logging.shutdown()

    def test_init(self):
        logger = Logger("Any logger")
        self.assertEqual(logger.name, "Any logger")
        self.assertTrue(logger.get_logger().hasHandlers())

    def test_repr(self):
        logger = Logger("new")
        self.assertTrue(repr(logger).endswith("<new>"))

        logger = Logger()
        self.assertTrue(repr(logger).endswith("<NOT SET>"))

    def test_get_warnings(self):
        logger = Logger()
        logger.messages[logging.WARNING] = ['warning messages']
        self.assertEqual(['warning messages'], logger.get_warnings())
        logger.messages[ExtendedLogger.DATAWARNING] = ['data warning messages']
        self.assertEqual(['warning messages', 'data warning messages'], logger.get_warnings())

    def test_get_errors(self):
        logger = Logger()
        logger.messages[logging.ERROR] = ['error messages']
        self.assertEqual(['error messages'], logger.get_errors())
        logger.messages[ExtendedLogger.DATAERROR] = ['data error messages']
        self.assertEqual(['error messages', 'data error messages'], logger.get_errors())

    def test_get_log_counts(self):
        logger = Logger()
        self.assertEqual({}, logger.get_log_counts())

    def test_get_summary(self):
        logger = Logger()
        logger.get_warnings = MagicMock(return_value=['the warnings'])
        logger.get_errors = MagicMock(return_value=['the errors'])
        logger.get_log_counts = MagicMock(return_value={'the': 'log counts'})

        self.assertEqual({
            'warnings': ['the warnings'],
            'errors': ['the errors'],
            'log_counts': {'the': 'log counts'}
        }, logger.get_summary())

    def test_log(self):
        mock_level_logger = MagicMock()
        mock_logger = type('MockLogger', (object,), {'log': lambda *args, **kwargs: mock_level_logger(*args, **kwargs)})
        logger = Logger('name')
        logger._save_log = MagicMock()
        logger.get_logger = MagicMock(return_value=mock_logger)
        logger._default_args = {'some': 'arg'}
        logger.MAX_SIZE = 15
        logger.SHORT_MESSAGE_SIZE = 10
        message = 20 * 'a'
        short_message = (10 * 'a') + '...'

        logger._log(10, message, {'a': 'kwarg'})
        mock_level_logger.assert_called_with(10, short_message, extra=logger._default_args)

    def test_multiple_init(self):
        logger1 = Logger("Any logger")
        logger2 = Logger("Any other logger")
        logger3 = Logger(logger1.name)
        self.assertIs(logger1.get_logger(), logger3.get_logger())

    def test_info(self):
        logger = Logger("Info logger")
        with self.assertLogs(logger=logger.get_logger(), level=logging.INFO) as result:
            logger.info("test")
        self.assertEqual(result.output, [f"INFO:{logger.name}:test"])

    def test_warning(self):
        logger = Logger("Warning logger")
        with self.assertLogs(logger=logger.get_logger(), level=logging.INFO) as result:
            logger.warning("test")
        self.assertEqual(result.output, [f"WARNING:{logger.name}:test"])

    def test_error(self):
        logger = Logger("Error logger")
        with self.assertLogs(logger=logger.get_logger(), level=logging.INFO) as result:
            logger.error("test")
        self.assertEqual(result.output, [f"ERROR:{logger.name}:test"])

    def test_data_info(self):
        logger = Logger("Data info logger")
        self.assertEqual(logger._data_msg_count['data_info'], 0)
        with self.assertLogs(logger=logger.get_logger(), level=ExtendedLogger.DATAINFO) as result:
            logger.data_info("test")
        self.assertEqual(result.output, [f"DATAINFO:{logger.name}:test"])
        self.assertEqual(logger._data_msg_count['data_info'], 1)

    def test_data_warning(self):
        logger = Logger("Data warning logger")
        self.assertEqual(logger._data_msg_count['data_warning'], 0)
        with self.assertLogs(logger=logger.get_logger(), level=ExtendedLogger.DATAWARNING) as result:
            logger.data_warning("test")
        self.assertEqual(result.output, [f"DATAWARNING:{logger.name}:test"])
        self.assertEqual(logger._data_msg_count['data_warning'], 1)

    def test_data_error(self):
        logger = Logger("Data error logger")
        self.assertEqual(logger._data_msg_count['data_error'], 0)
        # RequestsHandler.LOG_PUBLISHER.publish.reset_mock()
        with self.assertLogs(logger=logger.get_logger(), level=ExtendedLogger.DATAERROR) as result:
            logger.data_error("test")
        self.assertEqual(result.output, [f"DATAERROR:{logger.name}:test"])
        self.assertEqual(logger._data_msg_count['data_error'], 1)

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
        logger.configure(msg, "NEW NAME", handlers=[RequestsHandler()])
        self.assertEqual(logger.name, "NEW NAME")
        self.assertEqual(logger._default_args, msg["header"])
        self.assertEqual(len(logger.messages), 0)

        logger.warning("test")
        RequestsHandler.LOG_PUBLISHER.publish.assert_called_once()
        level, args = RequestsHandler.LOG_PUBLISHER.publish.call_args[0]
        self.assertEqual(level, "WARNING")
        self.assertIsNotNone(args["timestamp"])
        self.assertEqual(args["level"], "WARNING")
        self.assertEqual(args["name"], logger.name)
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
                'collection': 'any entity'
            },
            "some": "other"
        }
        logger.configure(msg, "Another config logger", handlers=[RequestsHandler()])

        logger.warning("test")
        RequestsHandler.LOG_PUBLISHER.publish.assert_called_once()
        level, args = RequestsHandler.LOG_PUBLISHER.publish.call_args[0]
        self.assertEqual(args["name"], logger.name)
        # Test collection fallback
        self.assertEqual(args["entity"], msg['header']['collection'])
        self.assertEqual(logger.name, args["name"])
        self.assertEqual(logger.get_attribute('source'), 'any source')

    @patch("gobcore.quality.issue.Issue")
    @patch("gobcore.logging.logger.Logger.open_offload_file")
    @patch("gobcore.logging.logger.Logger.write_issue")
    def test_add_issue(self, mock_write_issue, mock_open_offload_file, mock_issue):
        logger = Logger()
        any_issue = MagicMock()
        any_issue.json = '{"issue": 1}'
        any_issue.get_unique_id.return_value = 1

        mock_byte_position = 1024

        mock_write_issue.return_value = mock_byte_position

        logger.add_issue(any_issue, 'error')
        # Assert issue offload file is opened
        mock_open_offload_file.assert_called()

        # Assert issue is written to file and data count has been updated
        mock_write_issue.assert_called_with(any_issue)
        self.assertEqual(1, logger._data_msg_count['data_error'])
        self.assertEqual(logger._issues, {1: 1024})

        mocked_issue = MagicMock()
        mock_issue.from_json.return_value = mocked_issue

        mock_offload_file = MagicMock()
        logger._offload_file = mock_offload_file

        # Add the same issue again to make sure it is joined
        logger.add_issue(any_issue, 'error')
        mock_offload_file.seek.assert_has_calls([call(1024), call(0,2)])
        mock_offload_file.readline.assert_called()

        mocked_issue.join_issue.assert_called_with(any_issue)
        mock_write_issue.assert_called_with(mocked_issue)
        # Still one data error
        self.assertEqual(1, logger._data_msg_count['data_error'])

        logger = Logger()
        any_issue.get_unique_id.return_value = 1
        another_issue = MagicMock()
        another_issue.get_unique_id.return_value = 1

        logger.add_issue(any_issue, 'info')
        self.assertEqual(1, logger._data_msg_count['data_info'])

        another_issue.get_unique_id.return_value = 2
        logger.add_issue(another_issue, 'error')
        self.assertEqual(1, logger._data_msg_count['data_error'])

        logger = Logger()
        logger.add_issue(any_issue, 'info')
        self.assertEqual(1, logger._data_msg_count['data_info'])

    @patch("gobcore.logging.logger.Logger.close_offload_file")
    @patch("gobcore.logging.logger.json")
    def test_get_issues(self, mock_json, mock_close):
        logger = Logger()
        logger._issues = {'1': 'any issue', '2': 'any other issue'}

        self.assertEqual(logger._issues, {'1': 'any issue', '2': 'any other issue'})

        mocked_file = mock_open(read_data='any line\nany other line\n')

        with patch('builtins.open', mocked_file):
            issues = logger.get_issues()
            self.assertIsInstance(issues, types.GeneratorType)
            mock_close.assert_not_called()

            _ = [issue for issue in issues]

            mock_close.assert_called()
            mock_json.loads.assert_has_calls([call('any line'), call('any other line')])

    def test_has_issue(self):
        logger = Logger()
        logger._issues = {'1': 'any issue', '2': 'any other issue'}
        self.assertTrue(logger.has_issue())

        logger._issues = {}
        self.assertFalse(logger.has_issue())

    def test_write_issue(self):
        logger = Logger()

        mock_issue = MagicMock()
        mock_issue.json = 'any json'

        mock_file = MagicMock()
        mock_file.tell.return_value = 'any byte offset'
        logger._offload_file = mock_file

        result = logger.write_issue(mock_issue)

        mock_file.tell.assert_called()
        mock_file.write.assert_called_with('any json\n')

        self.assertEqual(result, 'any byte offset')

    @patch("gobcore.logging.logger.os")
    def test_clear_offloaded_issues(self, mock_os):
        logger = Logger()
        logger._issues = {'1': 1}
        logger._data_msg_count = {'any': 'value'}
        logger._offload_file = 'any file'
        logger._offload_filename = 'any filename'

        # Assert all variables have been cleared
        logger.clear_issues()
        self.assertEqual(logger._issues, {})
        self.assertEqual(logger._offload_file, None)
        mock_os.remove.assert_called_with('any filename')

        # Check an OSError is passed without error
        mock_os.remove.side_effect = OSError()
        logger._offload_filename = 'any non existing filename'
        logger.clear_issues()

    @patch("gobcore.logging.logger.get_filename", lambda x, y: f"{y}.{x}")
    @patch("gobcore.logging.logger.get_unique_name", lambda: 'any unique name')
    def test_open_offload_file(self):
        logger = Logger()
        mocked_file = mock_open()
        with patch('builtins.open', mocked_file):
            logger.open_offload_file()
            self.assertEqual(logger._offload_filename, 'issues.any unique name')
            mocked_file.assert_called_once_with('issues.any unique name', 'w+')

    def test_close_offload_file(self):
        logger = Logger()
        mock_file = MagicMock()
        logger._offload_file = mock_file

        logger.close_offload_file()
        mock_file.close.assert_called()


class TestRequestHandler(TestCase):

    def setUp(self):
        RequestsHandler.LOG_PUBLISHER = None

    def tearDown(self):
        pass

    def test_init(self):
        request_handler = RequestsHandler()
        self.assertIsNone(request_handler.LOG_PUBLISHER)

    @patch("gobcore.logging.logger.LogPublisher", MagicMock())
    def test_emit(self):
        request_handler = RequestsHandler()
        record = MagicMock()
        record.name = "name"
        record.levelname = "levelname"
        record.msg = "the message"

        request_handler.emit(record)
        self.assertIsNotNone(request_handler.LOG_PUBLISHER)

        # Reuse log publisher once it is created
        request_handler.LOG_PUBLISHER.publish = MagicMock()
        request_handler.emit(record)
        request_handler.LOG_PUBLISHER.publish.assert_called()


class TestLoggerManager(TestCase):
    class MockThread:
        def __init__(self, id):
            self.ident = id

    @patch("gobcore.logging.logger.Logger")
    @patch("gobcore.logging.logger.threading.current_thread")
    def test_get_logger(self, mock_current_thread, mock_logger):
        mock_current_thread.side_effect = [self.MockThread(1), self.MockThread(2), self.MockThread(1)]

        with patch.object(LoggerManager, "loggers", defaultdict(mock_logger)):
            logger_manager = LoggerManager()

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
        with patch.object(LoggerManager, "get_logger", new_callable=MagicMock()):
            logger_manager = LoggerManager()

            logger_manager.abc()
            logger_manager.get_logger.return_value.abc.assert_called_once()

            logger_manager.ghi(1, 2, 3, kw=4, kw2=5)
            logger_manager.get_logger.return_value.ghi.assert_called_with(1, 2, 3, kw=4, kw2=5)

    def test_name(self):
        logger_manager = LoggerManager()
        logger_manager.loggers.clear()
        self.assertIsNone(logger_manager.name)

        logger_manager.configure({}, name="logger_name")
        self.assertEqual("logger_name", logger_manager.name)

        logger_manager.name = "new name"
        self.assertEqual("new name", logger_manager.name)

        with self.assertRaises(TypeError):
            logger_manager.name = None

        with self.assertRaises(TypeError):
            logger_manager.name = 1234

    def test_configure_context(self):
        # initialised without name, resets to None
        logger_manager = LoggerManager()
        logger_manager.loggers.clear()

        with logger_manager.configure_context({}, "the context name"):
            self.assertEqual(logger_manager.name, "the context name")

        self.assertIsNone(logger_manager.name)

        # initialised with name
        logger_manager.name = "the initial name"

        with logger_manager.configure_context({}, "the context name"):
            self.assertEqual(logger_manager.name, "the context name")

        self.assertEqual(logger_manager.name, "the initial name")
