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
import socket
import os

from gobcore.message_broker.config import HEARTBEAT_QUEUE, LOG_QUEUE, get_queue

HEARTBEAT_INTERVAL = 60     # Send a heartbeat every 60 seconds


class Heartbeat():

    def __init__(self, connection, name):
        """Hearbeat

        :param connection: the connection to use for the heartbeats
        :param name: the name of the service for which heartbeats are sent
        """
        self._connection = connection
        self._name = name

        self._queue = get_queue(HEARTBEAT_QUEUE)

        # Send an initial heartbeat
        self.send()

        # At exit send a final heartbeat that denotes the end of the process
        atexit.register(self.send)

    def send_on_msg(self, queue_name, key, _):
        """Send heartbeat on handle message

        :param queue_name: queue from which the message is received
        :param key: key for the message
        :param _: not used yet; the message itself
        :return: None
        """
        if queue_name == HEARTBEAT_QUEUE or queue_name == LOG_QUEUE:
            # Do not send a heartbeat on handling a heartbeat or log message
            return
        self.send()

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
            "host": socket.gethostname(),
            "pid": os.getpid(),
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
