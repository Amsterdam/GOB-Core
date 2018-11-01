import mock
import unittest

from gobcore.log_publisher import LogPublisher


class TestLogPublisher(unittest.TestCase):

    def testConstructor(self):
        # Test if a log publisher can be initialized
        publisher = LogPublisher(None)
        assert(publisher is not None)

    @mock.patch('gobcore.log_publisher.LogPublisher._auto_disconnect')
    @mock.patch('gobcore.message_broker.message_broker.Connection.connect')
    @mock.patch('gobcore.message_broker.message_broker.Connection.publish')
    def testPublish(self, patched_publish, patched_connect, patched_auto_disconnect):
        publisher = LogPublisher(None)
        publisher.publish("Level", "Message")
        assert(patched_publish.called)


    @mock.patch('gobcore.log_publisher.LogPublisher._auto_disconnect')
    @mock.patch('gobcore.message_broker.message_broker.Connection.connect')
    @mock.patch('gobcore.message_broker.message_broker.Connection.publish')
    def testAutoConnect(self, patched_publish, patched_connect, patched_auto_disconnect):
        publisher = LogPublisher(None)
        publisher.publish("Level", "Message")
        assert(patched_connect.called)
        assert(patched_auto_disconnect.called)
