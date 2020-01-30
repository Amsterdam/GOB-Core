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


def _build_key(*args):
    return ".".join(args)


def _build_queuename(*args):
    return _build_key(*args, 'queue')


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
    heartbeat_interval=1200,
    blocked_connection_timeout=600
)

WORKFLOW_EXCHANGE = "gob.workflow"
LOG_EXCHANGE = "gob.log"
STATUS_EXCHANGE = "gob.status"
AUDIT_LOG_EXCHANGE = "gob.audit"

EXCHANGES = [
    WORKFLOW_EXCHANGE,
    LOG_EXCHANGE,
    STATUS_EXCHANGE,
    AUDIT_LOG_EXCHANGE,
]

CHECK_RELATION = 'check_relation'
COMPARE = 'compare'
EXPORT = 'export'
EXPORT_TEST = 'export_test'
FULLUPDATE = 'fullupdate'
APPLY = 'apply'
IMPORT = 'import'
PREPARE = 'prepare'
RELATE = 'relate'
WORKFLOW = 'workflow'
RELATE_UPDATE_VIEW = 'relate_update_view'

JOBSTEP = 'jobstep'
TASK = 'task'
RESULT = 'result'
REQUEST = 'request'
COMPLETE = 'complete'
JOBSTEP_RESULT = _build_key(JOBSTEP, RESULT)
TASK_RESULT = _build_key(TASK, RESULT)
TASK_COMPLETE = _build_key(TASK, COMPLETE)
TASK_REQUEST = _build_key(TASK, REQUEST)

HEARTBEAT = 'heartbeat'
PROGRESS = 'progress'
STATUS = 'status'
LOGS = 'logs'

HEARTBEAT_QUEUE = _build_queuename(STATUS_EXCHANGE, HEARTBEAT)
PROGRESS_QUEUE = _build_queuename(STATUS_EXCHANGE, PROGRESS)
LOG_QUEUE = _build_queuename(LOG_EXCHANGE, LOGS)
AUDIT_LOG_QUEUE = _build_queuename(AUDIT_LOG_EXCHANGE, LOGS)
EVERYTHING_KEY = '#'
HEARTBEAT_KEY = _build_key(STATUS, HEARTBEAT)
PROGRESS_KEY = _build_key(STATUS, PROGRESS)

CHECK_RELATION_RESULT_KEY = _build_key(CHECK_RELATION, JOBSTEP_RESULT)
COMPARE_RESULT_KEY = _build_key(COMPARE, JOBSTEP_RESULT)
EXPORT_RESULT_KEY = _build_key(EXPORT, JOBSTEP_RESULT)
EXPORT_TEST_RESULT_KEY = _build_key(EXPORT_TEST, JOBSTEP_RESULT)
FULLUPDATE_RESULT_KEY = _build_key(FULLUPDATE, JOBSTEP_RESULT)
APPLY_RESULT_KEY = _build_key(APPLY, JOBSTEP_RESULT)
IMPORT_RESULT_KEY = _build_key(IMPORT, JOBSTEP_RESULT)
PREPARE_RESULT_KEY = _build_key(PREPARE, JOBSTEP_RESULT)
RELATE_RESULT_KEY = _build_key(RELATE, JOBSTEP_RESULT)
RELATE_UPDATE_VIEW_RESULT_KEY = _build_key(RELATE_UPDATE_VIEW, JOBSTEP_RESULT)

ALL_JOBSTEP_RESULTS_KEY = _build_key('*', JOBSTEP_RESULT)
ALL_TASK_RESULTS_KEY = _build_key('*', TASK_RESULT)

# Queues and keys for prepare tasks
PREPARE_TASK_RESULT_KEY = _build_key(PREPARE, TASK_RESULT)
PREPARE_TASK_COMPLETE_KEY = _build_key(PREPARE, TASK_COMPLETE)
PREPARE_TASK_REQUEST_KEY = _build_key(PREPARE, TASK_REQUEST)
PREPARE_TASK_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, PREPARE, TASK)
PREPARE_COMPLETE_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, PREPARE, COMPLETE)

CHECK_RELATION_REQUEST_KEY = _build_key(CHECK_RELATION, REQUEST)
COMPARE_REQUEST_KEY = _build_key(COMPARE, REQUEST)
EXPORT_REQUEST_KEY = _build_key(EXPORT, REQUEST)
EXPORT_TEST_REQUEST_KEY = _build_key(EXPORT_TEST, REQUEST)
FULLUPDATE_REQUEST_KEY = _build_key(FULLUPDATE, REQUEST)
APPLY_REQUEST_KEY = _build_key(APPLY, REQUEST)
IMPORT_REQUEST_KEY = _build_key(IMPORT, REQUEST)
PREPARE_REQUEST_KEY = _build_key(PREPARE, REQUEST)
RELATE_REQUEST_KEY = _build_key(RELATE, REQUEST)
RELATE_UPDATE_VIEW_REQUEST_KEY = _build_key(RELATE_UPDATE_VIEW, REQUEST)
TASK_REQUEST_KEY = _build_key(TASK, REQUEST)
WORKFLOW_REQUEST_KEY = _build_key(WORKFLOW, REQUEST)

CHECK_RELATION_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, CHECK_RELATION)
COMPARE_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, COMPARE)
EXPORT_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, EXPORT)
EXPORT_TEST_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, EXPORT_TEST)
FULLUPDATE_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, FULLUPDATE)
APPLY_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, APPLY)
IMPORT_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, IMPORT)
PREPARE_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, PREPARE)
RELATE_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, RELATE)
RELATE_UPDATE_VIEW_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, RELATE_UPDATE_VIEW)
TASK_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, TASK)
WORKFLOW_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, WORKFLOW)

JOBSTEP_RESULT_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, JOBSTEP_RESULT)
TASK_RESULT_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, TASK_RESULT)


"""
Queue configuration is leading in the setup of the message broker.

Configuration defines the exchanges that will be created, and the keys the exchange should route to which queues.
The message broker will be initialised based on this configuration.
"""
QUEUE_CONFIGURATION = {
    WORKFLOW_EXCHANGE: {
        CHECK_RELATION_QUEUE: [
            CHECK_RELATION_REQUEST_KEY,
        ],
        COMPARE_QUEUE: [
            COMPARE_REQUEST_KEY,
        ],
        EXPORT_QUEUE: [
            EXPORT_REQUEST_KEY,
        ],
        EXPORT_TEST_QUEUE: [
            EXPORT_TEST_REQUEST_KEY,
        ],
        FULLUPDATE_QUEUE: [
            FULLUPDATE_REQUEST_KEY,
        ],
        APPLY_QUEUE: [
            APPLY_REQUEST_KEY,
        ],
        IMPORT_QUEUE: [
            IMPORT_REQUEST_KEY,
        ],
        PREPARE_QUEUE: [
            PREPARE_REQUEST_KEY,
        ],
        RELATE_QUEUE: [
            RELATE_REQUEST_KEY,
        ],
        TASK_QUEUE: [
            TASK_REQUEST_KEY,
        ],
        WORKFLOW_QUEUE: [
            WORKFLOW_REQUEST_KEY,
        ],
        JOBSTEP_RESULT_QUEUE: [
            ALL_JOBSTEP_RESULTS_KEY,
        ],
        TASK_RESULT_QUEUE: [
            ALL_TASK_RESULTS_KEY,
        ],
        PREPARE_TASK_QUEUE: [
            PREPARE_TASK_REQUEST_KEY,
        ],
        PREPARE_COMPLETE_QUEUE: [
            PREPARE_TASK_COMPLETE_KEY,
        ],
        RELATE_UPDATE_VIEW_QUEUE: [
            RELATE_UPDATE_VIEW_REQUEST_KEY,
        ]
    },
    STATUS_EXCHANGE: {
        HEARTBEAT_QUEUE: [
            HEARTBEAT_KEY,
        ],
        PROGRESS_QUEUE: [
            PROGRESS_KEY,
        ],
    },
    LOG_EXCHANGE: {
        LOG_QUEUE: [
            EVERYTHING_KEY,
        ],
    },
    AUDIT_LOG_EXCHANGE: {
        AUDIT_LOG_QUEUE: [
            EVERYTHING_KEY,
        ]
    }
}
