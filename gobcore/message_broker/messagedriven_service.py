import time

from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import CONNECTION_PARAMS

keep_running = True


def _get_service(services, exchange, queue, key):
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
        while keep_running:
            time.sleep(60)
            # Report some statistics or whatever is useful
            print(".")
