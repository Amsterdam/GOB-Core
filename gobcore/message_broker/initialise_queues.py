"""Message broker initialisation

The message broker serves a number of persistent queues holding persistent messages.

Creating persistent queues is an independent task.

The queues are required by the import and upload modules.
These modules are responsable for importing and uploading and not for creating the queues.

The initialisation of the queues is an integral part of the initialisation and startup of the message broker.

"""
import requests
import pika

from gobcore.message_broker.config import CONNECTION_PARAMS,\
                                          MESSAGE_BROKER, MESSAGE_BROKER_API_PORT, MESSAGE_BROKER_VHOST,\
                                          MESSAGE_BROKER_USER, MESSAGE_BROKER_PASSWORD, QUEUE_CONFIGURATION,\
                                          EXCHANGES


def _create_vhost(vhost):
    """
    Create a virtual host using the RabbtiMQ management interface

    :param vhost: name of the virtual host
    :return:
    """
    response = requests.put(
        url=f"http://{MESSAGE_BROKER}:{MESSAGE_BROKER_API_PORT}/api/vhosts/{vhost}",
        headers={
            "content-type": "application/json"
        },
        auth=(
            MESSAGE_BROKER_USER,
            MESSAGE_BROKER_PASSWORD
        ))
    response.raise_for_status()


def _create_exchange(channel, exchange, durable):
    """
    Create a RabbitMQ exchange

    :param channel: the RabbitMQ connection channel
    :param exchange: the name of the exchange
    :param durable: specifies wether the exchange should be persistent
    :return:
    """
    channel.exchange_declare(
        exchange=exchange,
        exchange_type="topic",
        durable=durable)


def _create_queue(channel, queue, durable):
    """
    Create a RabbitMQ queue

    :param channel: the RabbitMQ connection channel
    :param queue: the name of the queue
    :param durable: specifies wether the queue should be persistent
    :return:
    """
    channel.queue_declare(
        queue=queue,
        durable=durable
    )


def _bind_queue(channel, exchange, queue, key):
    """
    Binds queue to exchange with given key

    :param channel: the RabbitMQ connection channel
    :param exchange: the name of the exchange
    :param queue: the name of the queue
    :param key: the key
    :return:
    """
    channel.queue_bind(
        exchange=exchange,
        queue=queue,
        routing_key=key
    )


def _initialize_queues(channel, queue_configuration):
    # First create all exchanges (some exchanges may not be included in queue_configuration below)
    for exchange in EXCHANGES:
        _create_exchange(channel=channel, exchange=exchange, durable=True)

    for exchange, queues in queue_configuration.items():
        for queue, keys in queues.items():
            _create_queue(channel=channel, queue=queue, durable=True)

            for key in keys:
                _bind_queue(channel=channel, exchange=exchange, queue=queue, key=key)


def create_queue_with_binding(exchange, queue, key):
    """
    Utility function to create a queue and an exchange and their key binding at once.

    :param exchange:
    :param queue:
    :param key:
    :return:
    """
    with pika.BlockingConnection(CONNECTION_PARAMS) as connection:
        channel = connection.channel()
        _create_exchange(channel, exchange, True)
        _create_queue(channel, queue, True)
        _bind_queue(channel, exchange, queue, key)


def initialize_message_broker():
    """
    Initializes the RabbitMQ message broker.

    Creates a virtual host and persistent exxhanges and queues
    :return:
    """
    print(f"Initialize message broker {MESSAGE_BROKER}")

    _create_vhost(MESSAGE_BROKER_VHOST)

    # Add exchanges and queues
    with pika.BlockingConnection(CONNECTION_PARAMS) as connection:

        channel = connection.channel()
        _initialize_queues(channel, QUEUE_CONFIGURATION)
