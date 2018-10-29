"""Synchronous message broker

Simple synchronous message broker.

Only publication is supported

"""
import json
import pika

from gobcore.typesystem.json import GobTypeJSONEncoder


class Connection(object):
    """This is an synchronous RabbitMQ connection.

    No automatic reconnection with RabbitMQ is implemented.

    """

    def __init__(self, connection_params):
        """Create a new Connection

        :param address: The RabbitMQ address
        """

        # The connection parameters for the RabbitMQ Message broker
        self._connection_params = connection_params

        # The Connection and Channel objects
        self._connection = None
        self._channel = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def connect(self):
        self._connection = pika.BlockingConnection(self._connection_params)
        self._channel = self._connection.channel()

    def publish(self, queue, key, msg):
        # Check whether a connection has been established
        if self._channel is None:
            raise Exception("Connection with message broker not available")

        # Convert the message to json
        json_msg = json.dumps(msg, cls=GobTypeJSONEncoder, allow_nan=False)

        self._channel.basic_publish(
            exchange=queue["exchange"],
            routing_key=key,
            properties=pika.BasicProperties(
                delivery_mode=2  # Make messages persistent
            ),
            body=json_msg
        )

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
