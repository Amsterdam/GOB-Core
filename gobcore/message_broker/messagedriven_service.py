import time

from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, CONNECTION_PARAMS

keep_running = True


def messagedriven_service(service_defenition):
    """Start a connection with a the message broker and the given definition

    servicedefenition is a dict of dicts:

    ```
    SERVICEDEFINITION = {
        'message_to_listen_to': {
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

    with AsyncConnection(CONNECTION_PARAMS) as connection:
        # Subscribe to the queues, handle messages in the on_message function (runs in another thread)
        for key, service in service_defenition.items():
            queue = {
                "exchange": WORKFLOW_EXCHANGE,
                "name": service['queue'],
                "key": key
            }

            on_message = _get_on_message(service)

            connection.subscribe([queue], on_message)
            print(f"Listening to messages {key} on {service['queue']}")

        # Repeat forever
        print("Queue connection for servicedefinition started")
        while keep_running:
            time.sleep(60)
            # Report some statistics or whatever is useful
            print(".")


def _get_on_message(single_service):
    """Prepare the generic on_message method with single_service specific handler, report_back_queue and
    report back message"""

    def on_message(connection, queue, key, msg):
        """Called on every message receipt

        :param connection: the connection with the message broker
        :param queue: the message broker queue
        :param key: the identification of the message (e.g. fullimport.proposal)
        :param msg: the contents of the message
        :return:
        """

        print(f"{key} accepted from {queue['name']}, start handling")

        # Get the handler for the specific workflow
        handle = single_service['handler']

        try:
            result_msg = handle(msg)
        except RuntimeError:
            return False

        # Get the key to report back the results
        report_back = single_service['report_back']

        report_queque = {
            "exchange": WORKFLOW_EXCHANGE,
            "name": single_service['report_queue'],
            "key": single_service['report_back']
        }

        connection.publish(report_queque, report_back, result_msg)
        return True

    return on_message
