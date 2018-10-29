"""LogPublisher

This module contains the LogPublisher class.

A LogPublisher publishes log message on the message broker.

"""
from gobcore.message_broker.message_broker import Connection
from gobcore.message_broker.config import get_queue

from gobcore.message_broker.config import CONNECTION_PARAMS
from gobcore.message_broker.config import LOG_QUEUE


class LogPublisher():

    _connection = None
    _queue = None

    def __init__(self, connection_params=CONNECTION_PARAMS, queue_name=LOG_QUEUE):
        # Open a connection and register the log queue
        self._connection = Connection(connection_params)
        self._queue = get_queue(queue_name)

    def publish(self, level, msg):
        self._connection.publish(self._queue, level, msg)

    def connect(self):
        self._connection.connect()

    def disconnect(self):
        self._connection.disconnect()
