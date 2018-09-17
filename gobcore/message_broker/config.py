"""Workflow configuration

Definition of the message broker queues, exchanges and routing keys.

Two main queues are defined:
    WORKFLOW - the normal import-upload traffic.
               Proposals (units of work) are received and routed by the workflow manager.
    LOG - the logging output of each GOB process is routed over this queue

"""
import os
import pika

MESSAGE_BROKER = os.getenv("MESSAGE_BROKER_ADDRESS", "localhost")
MESSAGE_BROKER_USER = os.getenv("MESSAGE_BROKER_USERNAME", "guest")
MESSAGE_BROKER_PASSWORD = os.getenv("MESSAGE_BROKER_PASSWORD", "guest")

CONNECTION_PARAMS = pika.ConnectionParameters(
    host=MESSAGE_BROKER,
    virtual_host="gob",
    credentials=pika.PlainCredentials(username=MESSAGE_BROKER_USER,
                                      password=MESSAGE_BROKER_PASSWORD)
)

WORKFLOW_EXCHANGE = "gob.workflow"
LOG_EXCHANGE = "gob.log"

PROPOSAL_QUEUE = WORKFLOW_EXCHANGE + '.proposal'
REQUEST_QUEUE = WORKFLOW_EXCHANGE + '.request'
LOG_QUEUE = LOG_EXCHANGE + '.all'

QUEUES = [
    {
        "exchange": WORKFLOW_EXCHANGE,
        "name": PROPOSAL_QUEUE,
        "key": "*.proposal"
    },
    {
        "exchange": WORKFLOW_EXCHANGE,
        "name": REQUEST_QUEUE,
        "key": "*.request"
    },
    {
        "exchange": LOG_EXCHANGE,
        "name": LOG_QUEUE,
        "key": "#"
    }
]


def get_queue(queue_name):
    for queue in QUEUES:
        if queue['name'] == queue_name:
            return queue

    return None
