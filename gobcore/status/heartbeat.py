"""Heartbeat.

Heartbeats are sent at regular intervals.
Heartbeats are controlled by the messagedriven_service.

An atexit method is registered to send a final heartbeat noticing the end of the process.

A service is marked dead when no heartbeats have been received after the REPORT_INTERVAL or
when the service has reported itself dead via the atexit method
"""


import atexit
import contextlib
import datetime
import os
import socket
import threading
from typing import NewType, Optional

from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import HEARTBEAT_KEY, PROGRESS_KEY, STATUS_EXCHANGE
from gobcore.message_broker.typing import Header, Message, Service
from gobcore.utils import get_host_info

HEARTBEAT_INTERVAL = 60  # Send a heartbeat every 60 seconds

JobStatusType = NewType("JobStatusType", str)

# Job status
STATUS_START = JobStatusType("started")
STATUS_OK = JobStatusType("ended")
STATUS_FAIL = JobStatusType("failed")
STATUS_SCHEDULED = JobStatusType("scheduled")
STATUS_REJECTED = JobStatusType("rejected")
STATUS_END = STATUS_OK


def _is_heartbeat_thread(t: threading.Thread) -> bool:
    """Test if given thread should be reported by heartbeat.

    With this filter we can exclude threads that do not bring value to
    performance management.
    """
    return not t.name.startswith("_")


def _is_application_thread(t: threading.Thread) -> bool:
    """Test if given thread is for is_alive determination."""
    return t.name in ["Eventloop", threading.main_thread().name]


class Heartbeat:
    """Message broker heartbeat."""

    exchange = STATUS_EXCHANGE
    heartbeat_key = HEARTBEAT_KEY
    progress_key = PROGRESS_KEY

    @classmethod
    @contextlib.contextmanager
    def progress(cls, connection: AsyncConnection, service: Service, msg: Message):
        """Send a progress heartbeat.

        Progress is only reported for services that produce a result (service["report"])
        The job and step are taken from the message header
        :param connection: The message broker connection
        :param service: The definition of the service that delivered the message
        :param msg: The message being processed
        :return: None
        """

        def _publish_progress(status: JobStatusType, info_msg: Optional[str] = None):
            if service.get("report") and msg.get("header"):
                jobid = msg["header"].get("jobid")
                stepid = msg["header"].get("stepid")

                if jobid and stepid:
                    # Log progress on stdout
                    print(cls._progress_log_msg(service.get("queue", "unknown_queue"), status, msg["header"]))

                    publish_msg = {
                        # Include header so that any logs get reported on the correct job
                        "header": msg["header"],
                        # Include the specific status fields
                        "jobid": jobid,
                        "stepid": stepid,
                        "status": status,
                        "info_msg": info_msg,
                    }
                    connection.publish(cls.exchange, cls.progress_key, publish_msg)

        try:
            _publish_progress(STATUS_START)
            yield
            _publish_progress(STATUS_OK)
        except Exception as err:
            _publish_progress(STATUS_FAIL, str(err))
            raise err
        finally:
            pass

    @classmethod
    def _progress_log_msg(cls, queue: str, status: JobStatusType, header: Header) -> str:
        """Return a message that contains info about the message and its execution state.

        :param queue: the name of the queue that holds the message
        :param status: the status of the message
        :param header: the message header
        :return: a string with message info
        """
        # Info about message being processed
        msg_info = [f"{key}: {header.get(key)}" for key in ["catalogue", "collection", "application", "source"]]

        # Info about host that processes the message
        host_info = [f"{key}: {value}" for key, value in get_host_info().items()]

        return f"{queue} - {status} - {host_info} - {msg_info}"

    def __init__(self, connection: AsyncConnection, name: str):
        """Initialise Heartbeat.

        :param connection: the connection to use for the heartbeats
        :param name: the name of the service for which heartbeats are sent
        """
        self._connection = connection
        self._name = name

        self._exchange = Heartbeat.exchange
        self._heartbeat_key = Heartbeat.heartbeat_key

        # Send an initial heartbeat
        self.send()

        # At exit send a final heartbeat that denotes the end of the process
        atexit.register(self.send)

    @property
    def threads(self) -> list[threading.Thread]:
        """Threads that heartbeat should report on."""
        return list(filter(_is_heartbeat_thread, threading.enumerate()))

    def send(self):
        """Send a heartbeat signal.

        :return: None
        """
        application_threads = filter(_is_application_thread, self.threads)

        # All application threads should be alive
        is_alive = all(t.is_alive() for t in application_threads)

        status_msg = {
            "name": self._name,
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "is_alive": is_alive,
            "threads": [{"name": t.name, "is_alive": t.is_alive()} for t in self.threads],
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }

        self._connection.publish(self._exchange, self._heartbeat_key, status_msg)

        # Report visual progress
        print("OK" if is_alive else "ERROR", flush=True)
