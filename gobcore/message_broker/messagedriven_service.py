import sys
import threading
import time
from typing import Any, Optional

from gobcore.logging.logger import logger, StdoutHandler, RequestsHandler
from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import CONNECTION_PARAMS
from gobcore.message_broker.initialise_queues import initialize_message_broker
from gobcore.message_broker.notifications import contains_notification, send_notification
from gobcore.message_broker.typing import Service, ServiceDefinition
from gobcore.quality.issue import process_issues
from gobcore.status.heartbeat import Heartbeat, HEARTBEAT_INTERVAL
from gobcore.utils import get_logger_name

CHECK_CONNECTION = 5                # Check connection every n seconds
RUNS_IN_OWN_THREAD = "own_thread"   # Service that runs in a separate thread
LOG_HANDLERS = [StdoutHandler(), RequestsHandler()]

# Assure that heartbeats are sent at every HEARTBEAT_INTERVAL
assert HEARTBEAT_INTERVAL % CHECK_CONNECTION == 0


def _on_message(connection: AsyncConnection, service: Service, msg: dict[str, Any]) -> bool:
    """Called on every message receipt

    :param connection: the connection with the message broker
    :param service: the service definition for the message
    :param msg: the contents of the message

    :return:
    """
    with (
        Heartbeat.progress(connection, service, msg),
        logger.configure_context(msg, get_logger_name(service), LOG_HANDLERS)
    ):
        # execute handler
        if result_msg := service["handler"](msg):
            process_issues(result_msg)

            if contains_notification(result_msg):
                send_notification(result_msg)

            # If a report_queue is defined, report the result message
            if report := service.get("report"):
                connection.publish(report["exchange"], report["key"], result_msg)

    # Don't acknowledge messages which explicitely return False, in all other cases do acknowledge.
    return result_msg is not False


class MessagedrivenService:
    """Start a connection with a the message broker and the given definition

    servicedefenition is a dict of dicts:

    ```
    SERVICEDEFINITION = {
        'unique_key': {
            'exchange': 'name_of_the_exchange_to_listen_to',
            'queue': 'name_of_the_queue_to_listen_to',
            'key': 'name_of_the_key_to_listen_to'
            'handler': 'method_to_invoke_on_message',
             # optional report functionality
            'report': {
                'exchange': 'name_of_the_exchange_to_report_to',
                'queue': 'name_of_the_queue_to_report_to',
                'key': 'name_of_the_key_to_report_to'
            }
            # optional logger configuration
            'logger': 'name of the logger to be configured'
        }
    }
    ```

    start the service with:

    ```
    from gobcore.message_broker.messagedriven_service import MessagedrivenService

    MessagedrivenService(SERVICEDEFINITION).start()

    """
    def __init__(self, services: ServiceDefinition, name: str, params: Optional[dict] = None):
        self.services = services
        self.name = name
        self.params = params or {}
        self.thread_per_service = self.params.get('thread_per_service', False)
        self.threads = []
        self.keep_running = True  # This variable is used for testing only
        self.check_connection = CHECK_CONNECTION
        self.heartbeat_interval = HEARTBEAT_INTERVAL

        self._init()

    def _init(self):
        """Initializes the message broker

        This method is idempotent. If the message broker has already been initialised it will be noticed and
        the initialisation becomes a noop

        :return:
        """
        try:
            initialize_message_broker()
        except Exception as e:
            print(f"Error: Failed to initialize message broker, {str(e)}")
            sys.exit(1)

        print("Succesfully initialized message broker")

        # Call all queue functions that need setting up after the message broker is initialized
        # For example, in a SERVICEDEFINITION, you'll find 'queue': lambda: listen_to_notifications(....)
        for service_id, service in self.services.items():
            if callable(service['queue']):
                service['queue'] = service['queue']()

    def _start_threads(self, queues: list[str]):
        for queue in queues:
            self._start_thread([queue])

    def _start_thread(self, queues: list[str]) -> threading.Thread:
        thread = threading.Thread(target=self._listen, args=(queues,), name="QueueHandler")
        thread.start()

        self.threads.append({
            'thread': thread,
            'queues': queues,
        })

        return thread

    def _on_message(self, connection: AsyncConnection, exchange: str, queue: str, key: str, msg: dict) -> bool:
        """Called on every message receipt

        :param connection: the connection with the message broker
        :param exchange: the message broker exchange
        :param queue: the message broker queue
        :param key: the identification of the message (e.g. fullimport.proposal)
        :param msg: the contents of the message

        :return:
        """
        print(f"{key} accepted from {queue}, start handling")
        service = self._get_service(queue)

        return _on_message(connection, service, msg)

    def _listen(self, queues: list[str]):
        with AsyncConnection(CONNECTION_PARAMS, self.params) as connection:
            # Subscribe to the queues, handle messages in the on_message function (runs in another thread)
            connection.subscribe(queues, self._on_message)

            id = ', '.join([queue for queue in queues])

            # Repeat forever
            while self.keep_running and connection.is_alive():
                time.sleep(self.check_connection)

            print(f"Queue connection for {id} stopped.")

    def start(self):
        asynchronous_queues = [service['queue'] for service in self.services.values()
                               if self.thread_per_service or service.get(RUNS_IN_OWN_THREAD)]
        synchronous_queues = [service['queue'] for service in self.services.values()
                              if not service['queue'] in asynchronous_queues]

        if asynchronous_queues:
            self._start_threads(asynchronous_queues)

        if synchronous_queues:
            self._start_thread(synchronous_queues)

        self._heartbeat_loop()

    def _heartbeat_loop(self):
        with AsyncConnection(CONNECTION_PARAMS, self.params) as connection:
            heartbeat = Heartbeat(connection, self.name)

            n = 0

            while self.keep_running and connection.is_alive():
                time.sleep(self.check_connection)

                for thread in self.threads:
                    if not thread['thread'].is_alive():
                        print("ERROR: died thread found")
                        # Create new thread
                        thread['thread'] = self._start_thread(thread['queues'])

                n += self.check_connection

                if n >= self.heartbeat_interval and all([t['thread'].is_alive() for t in self.threads]):
                    heartbeat.send()
                    n = 0

    def _get_service(self, queue: str) -> Service:
        """Gets the service for the specified queue

        :param queue:
        :return:
        """
        return next(s for s in self.services.values() if s["queue"] == queue)


def messagedriven_service(services: ServiceDefinition, name: str, params: Optional[dict] = None):
    # For backwards compatibility
    MessagedrivenService(services, name, params).start()
