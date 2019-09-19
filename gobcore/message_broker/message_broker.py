"""Synchronous message broker

Simple synchronous message broker.

Only publication is supported

"""
import json
import pika

from gobcore.message_broker.utils import get_message_from_body
from gobcore.typesystem.json import GobTypeJSONEncoder


class Connection(object):
    """This is an synchronous RabbitMQ connection.

    No automatic reconnection with RabbitMQ is implemented.

    """

    def __init__(self, connection_params, params=None):
        """Create a new Connection

        :param address: The RabbitMQ address
        """

        # The connection parameters for the RabbitMQ Message broker
        self._connection_params = connection_params

        # The Connection and Channel objects
        self._connection = None
        self._channel = None

        # Custom params
        self._params = {
            "load_message": True,
            "stream_contents": False,
            "prefetch_count": 1
        }
        self._params.update(params or {})

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def is_alive(self):
        if self._connection is None or self._channel is None:
            return False
        else:
            return self._connection.is_open and self._channel.is_open

    def connect(self):
        self._connection = pika.BlockingConnection(self._connection_params)
        self._channel = self._connection.channel()

    def publish(self, exchange, key, msg):
        # Check whether a connection has been established
        if self._channel is None or not self._channel.is_open:
            raise Exception("Connection with message broker not available")

        # Convert the message to json
        json_msg = json.dumps(msg, cls=GobTypeJSONEncoder, allow_nan=False)

        self._channel.basic_publish(
            exchange=exchange,
            routing_key=key,
            properties=pika.BasicProperties(
                delivery_mode=2  # Make messages persistent
            ),
            body=json_msg
        )

    def consume(self, exchange, queue, key, message_matched, ack=False, inactivity_timeout=10):
        """Blocking consumption of one single message from a queue without using a callback.

        Pika `BlockingChannel.consume()` method is a generator that yields
        each message as a tuple of method, properties, and body.
        The active generator iterator terminates when the
        consumer is cancelled by client via `BlockingChannel.cancel()` or by
        broker.

        Pika advices us to call `BlockingChannel.cancel()` when you escape out of the
        generator loop.

        If you don't do this, then next call on the same channel to `BlockingChannel.consume()`
        with the exact same (queue, no_ack, exclusive) parameters
        will resume the existing consumer generator; however, calling with
        different parameters will result in an exception.

        :param str exchange: The exchange to use as a search criteria.
        :param str queue: The queue name to consume.
        :param str key: Routing key to use as a search criteria.
        :param callable message_matched: A function with only one parameter msg.
            It should return True or False based on if the msg matches our search criteria.
            That gives us necessary flexibity with defining how we look for messages.
            This also makes this method independent from a business logic
            which can be defined in message_matched function (provided by the consumer).
        :param bool ack: Ack or not ack the consumed message. Default False.
        :param int inactivity_timeout: Inactivity timeout in seconds. Default 10.
            We might want to re-evaluate if this value is suitable for us.
            - If inactivity_timeout is defined (not None), it will cause
            pika `BlockingChannel.consume()` method to yield (None, None, None)
            after the given period of inactivity.
            - If inactivity_timeout is None, then pika consume method blocks
            until the next event arrives. We default inactivity_timeout to
            an arbitrary value to avoid this situation.
        """

        # pika consume yields: tuple(spec.Basic.Deliver, spec.BasicProperties, body as str or unicode)
        for method, properties, body in self._channel.consume(queue, inactivity_timeout=inactivity_timeout):
            # break if no more messages can be consumed after
            # a certain period of time (defined by inactivity_timeout)
            if not method:
                break

            if method.exchange == exchange and method.routing_key == key:
                # extract msg and offload_id from the body so we can analyse it
                msg, offload_id = get_message_from_body(body, self._params)
                # check if msg matches with our search criteria
                if message_matched(msg):
                    # optionally ack the message
                    if ack:
                        self._channel.basic_ack(method.delivery_tag)
                    return method, properties, msg, offload_id

        # we call cancel() to escape out of the generator loop
        self._channel.cancel()
        # No messages found or all available messages do not match our search criteria
        return (None, None, None, None)

    def disconnect(self):
        """Disconnect from RabbitMQ

        Close any open channels
        Close the connection
        Stop any running eventloop

        :return: None
        """
        # Close any open channel
        if self._channel is not None and self._channel.is_open:
            self._channel.close()
        self._channel = None

        # Close any open connection
        if self._connection is not None:
            self._connection.close()
        self._connection = None
