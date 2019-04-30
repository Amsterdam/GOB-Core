import logging

from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.logging.log_publisher import LogPublisher
from gobcore.logging.logger import Logger, RequestsHandler


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

    def test_configure(self):
        RequestsHandler.LOG_PUBLISHER = MagicMock(spec=LogPublisher)
        RequestsHandler.LOG_PUBLISHER.publish = MagicMock()

        logger = Logger("Config logger")
        msg = {
            "header": {
                'process_id': 'any process_id',
                'source': 'any source',
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
        print(args)
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
