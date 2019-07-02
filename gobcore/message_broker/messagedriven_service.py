import sys
import time
import traceback
import threading

from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.status.heartbeat import Heartbeat, HEARTBEAT_INTERVAL, STATUS_OK, STATUS_START, STATUS_FAIL
from gobcore.message_broker.config import CONNECTION_PARAMS
from gobcore.message_broker.initialise_queues import initialize_message_broker

CHECK_CONNECTION = 5    # Check connection every n seconds

# Assure that heartbeats are sent at every HEARTBEAT_INTERVAL
assert(HEARTBEAT_INTERVAL % CHECK_CONNECTION == 0)


def _on_message(connection, service, msg):
    """Called on every message receipt

    :param connection: the connection with the message broker
    :param service: the service definition for the message
    :param msg: the contents of the message

    :return:
    """
    handler = service['handler']
    result_msg = None
    try:
        Heartbeat.progress(connection, service, msg, STATUS_START)
        result_msg = handler(msg)
        Heartbeat.progress(connection, service, msg, STATUS_OK)
    except Exception as err:
        Heartbeat.progress(connection, service, msg, STATUS_FAIL, str(err))
        # Print error message, the message that caused the error and a short stacktrace
        stacktrace = traceback.format_exc(limit=-5)
        print("FATAL ERROR: Message processing has failed, further processing stopped", str(err), stacktrace)

    # If a report_queue is defined, report the result message (if any)
    if 'report' in service and result_msg is not None:
        report = service['report']
        connection.publish(report['exchange'], report['key'], result_msg)

    # Remove the message from the queue by returning true
    return True


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
        }
    }
    ```

    start the service with:

    ```
    from gobcore.message_broker.messagedriven_service import MessagedrivenService

    MessagedrivenService(SERVICEDEFINITION).start()

    """
    def __init__(self, services, name, params={}):
        self.services = services
        self.name = name
        self.params = params
        self.thread_per_service = self.params.get('thread_per_service', False)
        self.threads = []
        self.keep_running = True

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

    def _start_threads(self, queues: list):
        for queue in queues:
            thread = self._start_thread(queue)

            self.threads.append({
                'thread': thread,
                'queue': queue,
            })

    def _start_thread(self, queue):
        thread = threading.Thread(target=self._listen, args=([queue],))
        thread.start()
        return thread

    def _on_message(self, connection, exchange, queue, key, msg):
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

    def _listen(self, queues: list):
        with AsyncConnection(CONNECTION_PARAMS, self.params) as connection:
            # Subscribe to the queues, handle messages in the on_message function (runs in another thread)
            connection.subscribe(queues, self._on_message)

            # Repeat forever
            print("Queue connection for servicedefinition started")

            while self.keep_running and connection.is_alive():
                time.sleep(CHECK_CONNECTION)

            print("Queue connection for servicedefinition has stopped")

    def start(self):
        queues = [service['queue'] for _, service in self.services.items()]

        if self.thread_per_service:
            self._start_threads(queues)
        else:
            self._listen(queues)

        self._heartbeat_loop()

    def _heartbeat_loop(self):
        with AsyncConnection(CONNECTION_PARAMS, self.params) as connection:
            heartbeat = Heartbeat(connection, self.name)

            n = 0

            while self.keep_running and connection.is_alive():
                time.sleep(CHECK_CONNECTION)

                for thread in self.threads:
                    if not thread['thread'].is_alive():
                        # Create new thread
                        thread['thread'] = self._start_thread(thread['queue'])

                n += CHECK_CONNECTION

                if n >= HEARTBEAT_INTERVAL:
                    heartbeat.send()
                    n = 0

    def _get_service(self, queue):
        """Gets the service for the specified queue

        :param queue:
        :return:
        """
        return next(s for s in self.services.values() if s["queue"] == queue)


def messagedriven_service(services, name, params={}):
    # For backwards compatibility
    MessagedrivenService(services, name, params).start()
