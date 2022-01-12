"""Synchronous message broker

Simple synchronous message broker.

Only publication is supported

"""
import json
import time

import pika
from pika.connection import Parameters

from gobcore.typesystem.json import GobTypeJSONEncoder


class Connection:
    """This is an synchronous RabbitMQ connection."""

    SEC_SLEEP = 10

    def __init__(self, connection_params: Parameters):
        """Create a new Connection

        :param connection_params: The RabbitMQ connection credentials
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

    def is_alive(self):
        if self._connection is None or self._channel is None:
            return False
        else:
            return self._connection.is_open and self._channel.is_open

    def connect(self):
        self._connection = pika.BlockingConnection(self._connection_params)
        self._channel = self._connection.channel()

    def auto_reconnect(self):
        """Retries establishing a connection to the message broker."""
        while True:
            if self.is_alive():
                break

            print(f'Connection with message broker not available, reconnecting in {self.SEC_SLEEP}...')

            self.disconnect()
            time.sleep(self.SEC_SLEEP)
            self.connect()

    def publish(self, exchange, key, msg):
        # make sure we have a connection to the message broker
        self.auto_reconnect()

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
