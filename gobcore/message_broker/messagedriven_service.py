import time

from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, CONNECTION_PARAMS

keep_running = True


def messagedriven_service(service_definition):
    """Start a connection with a the message broker and the given definition

    servicedefenition is a dict of dicts:

    ```
    SERVICEDEFINITION = {
        'key_of_the_message_to_listen_to': {
            'queue': 'name_of_the_queue_to_listen_to',
            'handler': 'method_to_invoke_on_message',
            'report_back': 'key_of_the_return_message',
            'report_queue': 'name_of_the_queue_to_report_to'
        }
    }
    ```

    start the service with:

    ```
    from gobcore.message_broker.messagedriven_service import messagedriven_service

    messagedriven_services(SERVICEDEFINITION)

    """

    on_message = _get_on_message(service_definition)

    with AsyncConnection(CONNECTION_PARAMS) as connection:
        # Subscribe to the queues, handle messages in the on_message function (runs in another thread)
        for key, service in service_definition.items():

            queue_in = {
                "exchange": WORKFLOW_EXCHANGE,
                "name": service['queue'],
                "key": key
            }

            connection.subscribe([queue_in], on_message)
            print(f"Listening to messages {key} on {service['queue']}")

        # Repeat forever
        print("Queue connection for servicedefinition started")
        while keep_running:
            time.sleep(60)
            # Report some statistics or whatever is useful
            print(".")


def _get_on_message(service_definition):
    """Create the on_message message-handler for this service_defintion """

    def on_message(connection, queue, key, msg):
        """Called on every message receipt

        :param connection: the connection with the message broker
        :param queue: the message broker queue
        :param key: the identification of the message (e.g. fullimport.proposal)
        :param msg: the contents of the message

        :return:
        """

        service_impl = service_definition[key]
        handler = service_impl['handler']

        report_queque = {
            "exchange": WORKFLOW_EXCHANGE,
            "name": service_impl['report_queue'],
            "key": service_impl['report_back']
        }

        report_back = service_impl['report_back']

        print(f"{key} accepted from {queue['name']}, start handling")

        try:
            result_msg = handler(msg)
        except RuntimeError:
            return False

        connection.publish(report_queque, report_back, result_msg)
        return True

    return on_message
