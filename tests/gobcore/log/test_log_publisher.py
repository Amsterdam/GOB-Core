import mock
import unittest

from unittest.mock import MagicMock

from tests.gobcore import fixtures

from gobcore.log import logger


class TestLogPublisher(unittest.TestCase):

    @mock.patch('gobcore.log.GobLogger.info', MagicMock())
    def testConstructor(self):
        dummy_msg = fixtures.get_dummy_msg()
        logger.configure(dummy_msg, "info")

        messages = ["foo", "bar"]
        for msg in messages:
            logger.info(msg)
            logger.info.assert_called_with(msg)

        self.assertEquals(logger.info.call_count, 2)

    @mock.patch('gobcore.log.LOG_PUBLISHER', MagicMock())
    def testLogLevel(self):

        dummy_msg = fixtures.get_dummy_msg()
        logger_name = "TEST"
        loglevel_str = "INFO"
        expected_messages = []

        logger.configure(dummy_msg, logger_name, 20)
        with self.assertLogs(logger_name, level=loglevel_str) as result:
            messages = ["foo", "bar"]
            for msg in messages:
                expected_messages.append(fixtures.construct_logging_output(loglevel_str, logger_name, msg))
                logger.info(msg)
            self.assertListEqual(result.output, expected_messages)

    @mock.patch('gobcore.log.LOG_PUBLISHER', MagicMock())
    def testLogLevelChangeLogLevel(self):
        dummy_msg = fixtures.get_dummy_msg()
        logger_name = "TEST"
        logger.configure(dummy_msg, logger_name, 50)

        # if no logging is found, assertError is thrown.
        with self.assertLogs():
            logger.configure(dummy_msg, logger_name, 10)
            messages = ["foo", "bar"]
            for msg in messages:
                logger.info(msg)

    @mock.patch('gobcore.log.LOG_PUBLISHER', MagicMock())
    def testLogLevelWarningError(self):
        dummy_msg = fixtures.get_dummy_msg()
        logger_name = "TEST"
        expected_messages = []

        logger.configure(dummy_msg, logger_name, 10)

        # if no logging is found, assertError is thrown.
        with self.assertLogs() as result:
            messages = ["foo", "bar"]
            for msg in messages:
                logger.warning(msg)
                logger.error(msg)
                expected_messages.append(fixtures.construct_logging_output("WARNING", logger_name, msg))
                expected_messages.append(fixtures.construct_logging_output("ERROR", logger_name, msg))
            self.assertListEqual(result.output, expected_messages)

    @mock.patch('gobcore.log.LOG_PUBLISHER', MagicMock())
    def testLogGobCoreError(self):
        dummy_msg = fixtures.get_dummy_msg()
        logger_name = "CORE"
        expected_messages = []

        logger.configure(dummy_msg, logger_name, 40)

        # if no logging is found, assertError is thrown.
        with self.assertLogs() as result:
            messages = ["foo", "bar"]
            for msg in messages:
                logger.gob_core_error(msg, dummy_msg, Exception)
                expected_messages.append(fixtures.construct_logging_output("ERROR", logger_name, msg))
            self.assertListEqual(result.output, expected_messages)

    @mock.patch('gobcore.log_publisher.LogPublisher._auto_disconnect',  MagicMock())
    @mock.patch('gobcore.message_broker.message_broker.Connection.connect',  MagicMock())
    @mock.patch('gobcore.message_broker.message_broker.Connection.publish', MagicMock())
    def testLogRequestHandlerIsSet(self):
        dummy_msg = fixtures.get_dummy_msg()
        logger_name = "CORE"
        expected_messages = []

        logger.configure(dummy_msg, logger_name, 40)

        # if no logging is found, assertion is thrown.
        with self.assertLogs() as cm:
            messages = ["foo", "bar"]
            for msg in messages:
                logger.gob_core_error(msg, dummy_msg, Exception)
                expected_messages.append(fixtures.construct_logging_output("ERROR", logger_name, msg))
            self.assertListEqual(cm.output, expected_messages)
