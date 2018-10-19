import sys
import time

from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import CONNECTION_PARAMS
from gobcore.message_broker.initialise_queues import initialize_message_broker

keep_running = True

CHECK_CONNECTION = 5    # Check connection every n seconds
REPORT_INTERVAL = 60    # Report statistics every n seconds


def _get_service(services, exchange, queue, key):
    """Gets the service for the specified exchange, queue and key combination

    :param services:
    :param exchange:
    :param queue:
    :param key:
    :return:
    """
    return next(s for s in services.values() if
                s["exchange"] == exchange and
                s["queue"] == queue and
                (s["key"] == key or s["key"] == "#"))


def _on_message(connection, service, msg):
    """Called on every message receipt

    :param connection: the connection with the message broker
    :param service: the service definition for the message
    :param msg: the contents of the message

    :return:
    """
    handler = service['handler']
    try:
        result_msg = handler(msg)
    except RuntimeError:
        return False

    # If a report_queue
    if 'report' in service:
        report = service['report']
        connection.publish(report, report['key'], result_msg)

    return True


def _init():
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


def messagedriven_service(services):
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
    from gobcore.message_broker.messagedriven_service import messagedriven_service

    messagedriven_services(SERVICEDEFINITION)

    """

    def on_message(connection, exchange, queue, key, msg):
        """Called on every message receipt

        :param connection: the connection with the message broker
        :param exchange: the message broker exchange
        :param queue: the message broker queue
        :param key: the identification of the message (e.g. fullimport.proposal)
        :param msg: the contents of the message

        :return:
        """
        print(f"{key} accepted from {queue}, start handling")
        service = _get_service(services, exchange, queue, key)
        return _on_message(connection, service, msg)

    # Start by initializing the message broker (idempotent)
    _init()

    with AsyncConnection(CONNECTION_PARAMS) as connection:
        # Subscribe to the queues, handle messages in the on_message function (runs in another thread)
        queues = []
        for key, service in services.items():

            queues.append({
                "exchange": service['exchange'],
                "name": service['queue'],
                "key": service['key']
            })
            print(f"Listening to messages {service['key']} on queue {service['queue']}")

        connection.subscribe(queues, on_message)

        # Repeat forever
        print("Queue connection for servicedefinition started")
        n = 0
        while keep_running and connection.is_alive():
            time.sleep(CHECK_CONNECTION)
            n += CHECK_CONNECTION
            if n > REPORT_INTERVAL:
                # Report some statistics or whatever is useful
                print(".", flush=True)
                n = 0

        print("Queue connection for servicedefinition has stopped")
