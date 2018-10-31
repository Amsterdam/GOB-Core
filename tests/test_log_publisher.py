import mock
import unittest

from gobcore.log_publisher import LogPublisher


class TestLogPublisher(unittest.TestCase):

    def testConstructor(self):
        # Test if a log publisher can be initialized
        publisher = LogPublisher(None)
        assert(publisher is not None)


    @mock.patch('gobcore.message_broker.message_broker.Connection.is_open', return_value=True)
    @mock.patch('gobcore.message_broker.message_broker.Connection.publish')
    def testPublish(self, patched_publish, pathed_is_open):
        publisher = LogPublisher(None)
        publisher.publish("Level", "Message")
        assert(patched_publish.called)
        assert(pathed_is_open.called)


    @mock.patch('gobcore.message_broker.message_broker.Connection.is_open', return_value=True)
    @mock.patch('gobcore.message_broker.message_broker.Connection.connect')
    def testConnect(self, patched_connect, pathed_is_open):
        publisher = LogPublisher(None)
        publisher.connect()
        assert(patched_connect.called)


    @mock.patch('gobcore.message_broker.message_broker.Connection.is_open', return_value=True)
    @mock.patch('gobcore.message_broker.message_broker.Connection.disconnect')
    def testDisconnect(self, patched_disconnnect, pathed_is_open):
        publisher = LogPublisher(None)
        publisher.disconnect()
        assert(patched_disconnnect.called)
