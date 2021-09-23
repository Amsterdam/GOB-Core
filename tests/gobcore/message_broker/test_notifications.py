from unittest import TestCase
from mock import patch
from unittest.mock import MagicMock, call

from gobcore.message_broker.notifications import listen_to_notifications,\
    contains_notification,\
    send_notification,\
    add_notification,\
    get_notification,\
    EventNotification,\
    _send_notification,\
    _listen_to_notifications,\
    DumpNotification

@patch("gobcore.message_broker.notifications.NOTIFY_EXCHANGE", 'notification exchange')
@patch("gobcore.message_broker.notifications.NOTIFY_BASE_QUEUE", 'base queue')
@patch("gobcore.message_broker.notifications.NOTIFICATION_KEY", 'notification key')
@patch("gobcore.message_broker.notifications.NOTIFICATION_HEADER_FIELDS", ['a', 'b'])
class TestNotifications(TestCase):

    @patch("gobcore.message_broker.notifications._listen_to_notifications")
    def test_listen_to_notifications(self, mock_listen_to_notifications):
        result = listen_to_notifications('any id')
        mock_listen_to_notifications.assert_called_with('notification exchange', 'base queue.any id', '')
        self.assertEqual(result, mock_listen_to_notifications.return_value)

        result = listen_to_notifications('any id', 'any type')
        mock_listen_to_notifications.assert_called_with('notification exchange', 'base queue.any id', 'any type')

    def test_contains_notifications(self):
        result = contains_notification(None)
        self.assertFalse(result, None)

        result = contains_notification({})
        self.assertFalse(result, {})

        result = contains_notification({'any key': 'any value'})
        self.assertFalse(result, None)

        result = contains_notification({'notification key': 'any notification'})
        self.assertTrue(result, 'any notification')

    @patch("gobcore.message_broker.notifications._send_notification")
    def test_send_notifications(self, mock_send_notification):
        send_notification({})
        mock_send_notification.assert_not_called()

        send_notification({'header': {'a': 1, 'c': 3}, 'notification key': {'type': "any type", 'x': 1, 'y': 2}})
        mock_send_notification.assert_called_with('notification exchange',
                                                  notification_type='any type',
                                                  msg={
                                                   'header': {'a': 1, 'b': None},
                                                   'type': "any type",
                                                   'x': 1,
                                                   'y': 2
                                               })

    def test_add_notification(self):
        notification = EventNotification('any applied', 'any last_event')
        msg = {}
        add_notification(msg, notification)
        expect = {
            'notification key': {
                'type': 'events',
                'contents': {
                    'applied': 'any applied',
                    'last_event': 'any last_event'
                }
            }}
        self.assertEqual(msg, expect)

    def test_get_notification(self):
        result = get_notification({})
        self.assertIsNone(result)

        result = get_notification({
            'header': 'any header',
            'type': 'events',
            'contents': {'applied': 'any applied', 'last_event': 'any last_event'}
        })
        self.assertEqual(result.type, 'events')
        self.assertEqual(result.header, 'any header')
        self.assertEqual(result.contents, {'applied': 'any applied', 'last_event': 'any last_event'})

    @patch("gobcore.message_broker.notifications.pika.BasicProperties")
    @patch("gobcore.message_broker.notifications.pika.BlockingConnection")
    @patch("gobcore.message_broker.notifications._create_exchange")
    def test_send_broadcast(self, mock_create_exchange, mock_blocking_connection, mock_basic_properties):
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_blocking_connection.return_value.__enter__.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        _send_notification('any exchange', 'any type', {})
        mock_create_exchange.assert_called_with(channel=mock_channel, exchange='any exchange', durable=True)
        mock_channel.basic_publish.assert_called_with(
            body='{}',
            exchange='any exchange',
            properties=mock_basic_properties(
                delivery_mode=2  # Make messages persistent
            ),
            routing_key='any type')

    @patch("gobcore.message_broker.notifications.pika.BlockingConnection")
    @patch("gobcore.message_broker.notifications._create_exchange")
    @patch("gobcore.message_broker.notifications._create_queue")
    @patch("gobcore.message_broker.notifications._bind_queue")
    def test_listen_to_broadcasts(self, mock_bind_queue, mock_create_queue, mock_create_exchange, mock_blocking_connection):
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_blocking_connection.return_value.__enter__.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        result = _listen_to_notifications('any exchange', 'any queue')
        mock_create_exchange.assert_called_with(channel=mock_channel, exchange='any exchange', durable=True)
        mock_create_queue.assert_called_with(channel=mock_channel, queue='any queue', durable=True)
        mock_bind_queue.assert_called_with(channel=mock_channel, exchange='any exchange', queue='any queue', key='')
        self.assertEqual(result, 'any queue')


class TestDumpNotification(TestCase):

    def test_from_msg(self):
        msg = {
            'type': 'dump',
            'header': {'some': 'header'},
            'contents': {
                'catalog': 'SOME CAT',
                'collection': 'SOME COLL',
            }
        }
        notification = get_notification(msg)
        self.assertIsInstance(notification, DumpNotification)
        self.assertEqual({'some': 'header'}, notification.header)
        self.assertEqual('SOME CAT', notification.contents.get('catalog'))
        self.assertEqual('SOME COLL', notification.contents.get('collection'))
