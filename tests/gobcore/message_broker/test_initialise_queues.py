from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobcore.message_broker.initialise_queues import _create_vhost, _create_exchange, _create_queue, _bind_queue, \
    _initialize_queues, initialize_message_broker, MESSAGE_BROKER_VHOST, CONNECTION_PARAMS, QUEUE_CONFIGURATION, \
    create_queue_with_binding


class TestInitialiseQueues(TestCase):

    @patch("gobcore.message_broker.initialise_queues.requests.put")
    def test_create_vhost(self, mock_put):
        vhost = 'thevhost'
        _create_vhost(vhost)

        mock_put.assert_called_with(
            url='http://localhost:5672/api/vhosts/thevhost',
            headers={'content-type': 'application/json'},
            auth=('guest', 'guest')
        )

        mock_put.return_value.raise_for_status.assert_called_once()

    def test_create_exchange(self):
        channel = MagicMock()

        _create_exchange(channel, 'exchange', 'durable')

        channel.exchange_declare.assert_called_with(
            exchange='exchange',
            exchange_type='topic',
            durable='durable',
        )

    def test_create_queue(self):
        channel = MagicMock()

        _create_queue(channel, 'queue', 'durable')

        channel.queue_declare.assert_called_with(
            queue='queue',
            durable='durable',
        )

    def test_bind_queue(self):
        channel = MagicMock()

        _bind_queue(channel, 'exchange', 'queue', 'key')

        channel.queue_bind.assert_called_with(
            exchange='exchange',
            queue='queue',
            routing_key='key',
        )

    @patch("gobcore.message_broker.initialise_queues.EXCHANGES", ['exchange1', 'exchange2', 'exchange3', 'exchange99'])
    @patch("gobcore.message_broker.initialise_queues._create_exchange")
    @patch("gobcore.message_broker.initialise_queues._create_queue")
    @patch("gobcore.message_broker.initialise_queues._bind_queue")
    def test_initialize_queues(self, mock_bind_queue, mock_create_queue, mock_create_exchange):
        queue_configuration = {
            'exchange1': {
                'queue1': ['key1', 'key2', 'key3']
            },
            'exchange2': {
                'queue2': ['key4'],
                'queue3': ['key5', 'key2'],
            }
        }
        channel = MagicMock()
        _initialize_queues(channel, queue_configuration)

        mock_create_exchange.assert_has_calls([
            call(channel=channel, exchange='exchange1', durable=True),
            call(channel=channel, exchange='exchange2', durable=True),
            call(channel=channel, exchange='exchange3', durable=True),
            call(channel=channel, exchange='exchange99', durable=True),
        ])

        mock_create_queue.assert_has_calls([
            call(channel=channel, queue='queue1', durable=True),
            call(channel=channel, queue='queue2', durable=True),
            call(channel=channel, queue='queue3', durable=True),
        ])

        mock_bind_queue.assert_has_calls([
            call(channel=channel, exchange='exchange1', queue='queue1', key='key1'),
            call(channel=channel, exchange='exchange1', queue='queue1', key='key2'),
            call(channel=channel, exchange='exchange1', queue='queue1', key='key3'),
            call(channel=channel, exchange='exchange2', queue='queue2', key='key4'),
            call(channel=channel, exchange='exchange2', queue='queue3', key='key5'),
            call(channel=channel, exchange='exchange2', queue='queue3', key='key2'),
        ])

    @patch("gobcore.message_broker.initialise_queues._create_exchange")
    @patch("gobcore.message_broker.initialise_queues._create_queue")
    @patch("gobcore.message_broker.initialise_queues._bind_queue")
    @patch("gobcore.message_broker.initialise_queues.pika.BlockingConnection")
    def test_create_queue_with_binding(self, mock_connection, mock_bind, mock_create_queue, mock_create_exchange):
        mocks = MagicMock()
        mocks.attach_mock(mock_create_exchange, 'create_exchange')
        mocks.attach_mock(mock_create_queue, 'create_queue')
        mocks.attach_mock(mock_bind, 'bind_queue')

        create_queue_with_binding('some exchange', 'some queue', 'some key')

        mock_connection.assert_called_with(CONNECTION_PARAMS)
        channel = mock_connection.return_value.__enter__.return_value.channel.return_value

        mocks.assert_has_calls([
            call.create_exchange(channel, 'some exchange', True),
            call.create_queue(channel, 'some queue', True),
            call.bind_queue(channel, 'some exchange', 'some queue', 'some key')
        ])

    @patch("gobcore.message_broker.initialise_queues._create_vhost")
    @patch("gobcore.message_broker.initialise_queues.pika.BlockingConnection")
    @patch("gobcore.message_broker.initialise_queues._initialize_queues")
    def test_initialize_message_broker(self, mock_init_queues, mock_connection, mock_create_vhost):
        initialize_message_broker()

        mock_create_vhost.assert_called_with(MESSAGE_BROKER_VHOST)
        mock_connection.assert_called_with(CONNECTION_PARAMS)

        channel = mock_connection.return_value.__enter__.return_value.channel.return_value
        mock_init_queues.assert_called_with(channel, QUEUE_CONFIGURATION)
