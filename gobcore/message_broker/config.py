"""Workflow configuration

Definition of the message broker queues, exchanges and routing keys.

Two main queues are defined:
    WORKFLOW - the normal import-upload traffic.
               Proposals (units of work) are received and routed by the workflow manager.
    LOG - the logging output of each GOB process is routed over this queue

"""
import os
import pika
import tempfile

GOB_SHARED_DIR = os.getenv("GOB_SHARED_DIR", tempfile.gettempdir())

MESSAGE_BROKER = os.getenv("MESSAGE_BROKER_ADDRESS", "localhost")
MESSAGE_BROKER_PORT = os.getenv("MESSAGE_BROKER_PORT", 15672)
MESSAGE_BROKER_VHOST = os.getenv("MESSAGE_BROKER_VHOST", "gob")
MESSAGE_BROKER_USER = os.getenv("MESSAGE_BROKER_USERNAME", "guest")
MESSAGE_BROKER_PASSWORD = os.getenv("MESSAGE_BROKER_PASSWORD", "guest")

CONNECTION_PARAMS = pika.ConnectionParameters(
    host=MESSAGE_BROKER,
    virtual_host=MESSAGE_BROKER_VHOST,
    credentials=pika.PlainCredentials(username=MESSAGE_BROKER_USER,
                                      password=MESSAGE_BROKER_PASSWORD),
    heartbeat_interval=600,
    blocked_connection_timeout=300
)

WORKFLOW_EXCHANGE = "gob.workflow"
LOG_EXCHANGE = "gob.log"
STATUS_EXCHANGE = "gob.status"

PROPOSAL_QUEUE = WORKFLOW_EXCHANGE + '.proposal'
REQUEST_QUEUE = WORKFLOW_EXCHANGE + '.request'
IMPORT_QUEUE = WORKFLOW_EXCHANGE + '.import'
EXPORT_QUEUE = WORKFLOW_EXCHANGE + '.export'
HEARTBEAT_QUEUE = STATUS_EXCHANGE + '.heartbeat'
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
        "exchange": WORKFLOW_EXCHANGE,
        "name": IMPORT_QUEUE,
        "key": "import.start"
    },
    {
        "exchange": WORKFLOW_EXCHANGE,
        "name": EXPORT_QUEUE,
        "key": "export.start"
    },
    {
        "exchange": LOG_EXCHANGE,
        "name": LOG_QUEUE,
        "key": "#"
    },
    {
        "exchange": STATUS_EXCHANGE,
        "name": HEARTBEAT_QUEUE,
        "key": "HEARTBEAT"
    }
]


def get_queue(queue_name):
    for queue in QUEUES:
        if queue['name'] == queue_name:
            return queue

    return None
