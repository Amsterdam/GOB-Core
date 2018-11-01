"""LogPublisher

This module contains the LogPublisher class.

A LogPublisher publishes log message on the message broker.

"""
import threading
import time

from gobcore.message_broker.message_broker import Connection
from gobcore.message_broker.config import get_queue

from gobcore.message_broker.config import CONNECTION_PARAMS
from gobcore.message_broker.config import LOG_QUEUE


class LogPublisher():

    _connection = None
    _connection_lock = threading.Lock()
    _auto_disconnect_thread = None
    _auto_disconnect_timeout = 0

    def __init__(self, connection_params=CONNECTION_PARAMS, queue_name=LOG_QUEUE):
        # Register the connection params and log queue
        self._connection_params = connection_params
        self._queue = get_queue(queue_name)

    def publish(self, level, msg):
        # Acquire a lock for the connection
        with self._connection_lock:
            # Connect to the message broker, auto disconnect after timeout seconds
            self._auto_connect(timeout=2)
            # Publish the message
            self._connection.publish(self._queue, level, msg)

    def _auto_connect(self, timeout):
        self._auto_disconnect_timeout = timeout
        if self._connection is None:
            # Start a connection if no connection is active
            self._connection = Connection(self._connection_params)
            self._connection.connect()
            # Start auto disconnect thread
            if self._auto_disconnect_thread is not None:
                # Join any previously ended threads
                self._auto_disconnect_thread.join()
            self._auto_disconnect_thread = threading.Thread(target=self._auto_disconnect)
            self._auto_disconnect_thread.start()

    def _disconnect(self):
        if self._connection is not None:
            self._connection.disconnect()
            self._connection = None

    def _auto_disconnect(self):
        while True:
            time.sleep(1)
            with self._connection_lock:
                if self._auto_disconnect_timeout > 0:
                    self._auto_disconnect_timeout -= 1
                else:
                    self._disconnect()
                    break
