import time

from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import CONNECTION_PARAMS

keep_running = True


def messagedriven_service(service_definition):
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

    with AsyncConnection(CONNECTION_PARAMS) as connection:
        # Subscribe to the queues, handle messages in the on_message function (runs in another thread)
        for key, service in service_definition.items():

            queue_in = {
                "exchange": service['exchange'],
                "name": service['queue'],
                "key": service['key']
            }

            on_message = _get_on_message(service)
            connection.subscribe([queue_in], on_message)

            print(f"Listening to messages {key} on {service['queue']}")

        # Repeat forever
        print("Queue connection for servicedefinition started")
        while keep_running:
            time.sleep(60)
            # Report some statistics or whatever is useful
            print(".")


def _get_on_message(service_implementation):
    """Create the on_message message-handler for this service_defintion """

    def on_message(connection, queue, key, msg):
        """Called on every message receipt

        :param connection: the connection with the message broker
        :param queue: the message broker queue
        :param key: the identification of the message (e.g. fullimport.proposal)
        :param msg: the contents of the message

        :return:
        """
        handler = service_implementation['handler']
        print(f"{key} accepted from {queue['name']}, start handling")

        try:
            result_msg = handler(msg)
        except RuntimeError:
            return False

        # If a report_queue
        if 'report' in service_implementation:
            report = service_implementation['report']
            connection.publish(report, report['key'], result_msg)

        return True

    return on_message
