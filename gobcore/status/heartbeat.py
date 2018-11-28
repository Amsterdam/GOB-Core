"""Heartbeat

Heartbeats are sent at regular intervals.
Heartbeats are controlled by the messagedriven_service

An atexit method is registered to send a final heartbeat noticing the end of the process

A service is marked dead when no heartbeats have been received after the REPORT_INTERVAL or
when the service has reported itself dead via the atexit method

"""
import datetime
import threading
import atexit

from gobcore.message_broker.message_broker import Connection
from gobcore.message_broker.config import CONNECTION_PARAMS, HEARTBEAT_QUEUE, get_queue

HEARTBEAT_INTERVAL = 60     # Send a heartbeat every 60 seconds


class Heartbeat():

    def __init__(self, name):
        """Hearbeat

        :param name: the name of the service for which heartbeats are sent
        """
        self._name = name

        # Open a connection with the mssage broker
        self._connection = Connection(CONNECTION_PARAMS)
        self._connection.connect()
        self._queue = get_queue(HEARTBEAT_QUEUE)

        # Send an initial heartbeat
        self.send()

        # At exit send a final heartbeat that denotes the end of the process
        atexit.register(self.send)

    def send(self):
        """Send a heartbeat signal

        :return: None
        """
        # The main and eventloop thread should be alive
        threads = [thread for thread in threading.enumerate() if
                   thread.name in ["Eventloop", threading.main_thread().name] and thread.is_alive()]
        is_alive = len(threads) == 2

        status_msg = {
            "name": self._name,
            "is_alive": is_alive,
            "threads": [
                {
                    "name": t.name,
                    "is_alive": t.is_alive()
                } for t in threading.enumerate()],
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

        self._connection.publish(self._queue, self._queue["key"], status_msg)

        # Report visual progress
        print("OK" if is_alive else "ERROR", flush=True)
