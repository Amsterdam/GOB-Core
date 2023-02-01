"""Start a workflow."""


import json

import pika

from gobcore.message_broker.config import CONNECTION_PARAMS, WORKFLOW_EXCHANGE, WORKFLOW_QUEUE, WORKFLOW_REQUEST_KEY


def start_workflow(workflow, arguments):
    """Start a workflow for the given specs.

    Puts a message on the bus to request execution of the give workflow

    :param workflow_spec:
    :return:
    """
    # Store all arguments in the header if no header specified
    header = arguments.get("header", arguments)
    contents = arguments.get("contents", {})
    contents_ref = arguments.get("contents_ref", None)

    msg = {"header": header, "contents": contents, "workflow": workflow}

    if contents_ref:
        msg["contents_ref"] = contents_ref

    with pika.BlockingConnection(CONNECTION_PARAMS) as connection:
        channel = connection.channel()

        # Convert the message to JSON
        json_msg = json.dumps(msg)

        # Publish a workflow-request message on the workflow-exchange for the given workflow
        channel.basic_publish(
            exchange=WORKFLOW_EXCHANGE,
            routing_key=WORKFLOW_REQUEST_KEY,
            properties=pika.BasicProperties(delivery_mode=2),  # Make messages persistent
            body=json_msg,
        )


def retry_workflow(msg):
    """Retry to start the given workflow message.

    The message is sent to a queue that delays the message for a specified interval.
    The message is resent until the remaining retry time that is specified in the message is less or equal to zero

    :param msg: workflow message
    :return:
    """
    SINGLE_RETRY_TIME = 1 * 60  # Retry every 1 minute
    RETRY_TIME = "retry_time"

    workflow = msg["workflow"]

    # Check if any retry time is left
    remaining_retry_time = workflow.get(RETRY_TIME, 0)
    if remaining_retry_time <= 0:
        # No time left, do not send the workflow message
        return False

    # reduce remaining retry time
    remaining_retry_time -= SINGLE_RETRY_TIME
    workflow[RETRY_TIME] = remaining_retry_time

    # Log retry
    print(f"Retry workflow, {remaining_retry_time} time left", workflow)

    with pika.BlockingConnection(CONNECTION_PARAMS) as connection:
        # Create a delay queue if it does not yet exist (idempotent action)
        delay_queue = f"{WORKFLOW_QUEUE}_delay"
        delay_channel = connection.channel()
        delay_channel.queue_declare(
            queue=delay_queue,
            durable=True,
            arguments={"x-dead-letter-exchange": WORKFLOW_EXCHANGE, "x-dead-letter-routing-key": WORKFLOW_REQUEST_KEY},
        )

        # Convert the message to json
        json_msg = json.dumps(msg)

        # Publish a delayed workflow-request message
        delay_channel.basic_publish(
            exchange="",
            routing_key=delay_queue,
            properties=pika.BasicProperties(
                delivery_mode=2, expiration=str(SINGLE_RETRY_TIME * 1000)  # Make messages persistent  # msecs
            ),
            body=json_msg,
        )

    return True
