"""Workflow configuration

Definition of the message broker queues, exchanges and routing keys.

Two main queues are defined:
    WORKFLOW - the normal import-upload traffic.
               Proposals (units of work) are received and routed by the workflow manager.
    LOG - the logging output of each GOB process is routed over this queue

"""
import os
import pika
import logging
from pathlib import Path

pika_logger = logging.getLogger("pika")
pika_logger.propagate = False
pika_logger.setLevel(logging.ERROR)


def _build_key(*args):
    return ".".join(args)


def _build_queuename(*args):
    return _build_key(*args, 'queue')


GOB_SHARED_DIR = os.getenv("GOB_SHARED_DIR", Path.home().joinpath("gob-volume"))

MESSAGE_BROKER = os.getenv("MESSAGE_BROKER_ADDRESS", "localhost")
MESSAGE_BROKER_PORT = os.getenv("MESSAGE_BROKER_PORT", 5672)
MESSAGE_BROKER_MANAGEMENT_PORT = os.getenv("MESSAGE_BROKER_MANAGEMENT_PORT", 15672)
MESSAGE_BROKER_VHOST = os.getenv("MESSAGE_BROKER_VHOST", "gob")
MESSAGE_BROKER_USER = os.getenv("MESSAGE_BROKER_USERNAME", "guest")
MESSAGE_BROKER_PASSWORD = os.getenv("MESSAGE_BROKER_PASSWORD", "guest")

CONNECTION_PARAMS = pika.ConnectionParameters(
    host=MESSAGE_BROKER,
    port=MESSAGE_BROKER_PORT,
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
ISSUE_EXCHANGE = "gob.issue"
EVENT_EXCHANGE = "gob.event"

EXCHANGES = [
    WORKFLOW_EXCHANGE,
    LOG_EXCHANGE,
    STATUS_EXCHANGE,
    AUDIT_LOG_EXCHANGE,
    ISSUE_EXCHANGE,
    EVENT_EXCHANGE,
]

NEVER_DROP_MESSAGES_EXHANGES = [
    EVENT_EXCHANGE,  # Should never drop events
]

COMPARE = 'compare'
EXPORT = 'export'
EXPORT_TEST = 'export_test'
FULLUPDATE = 'fullupdate'
APPLY = 'apply'
IMPORT = 'import'
PREPARE = 'prepare'
WORKFLOW = 'workflow'
RELATE = 'relate'
RELATE_PREPARE = 'relate_prepare'
RELATE_PROCESS = 'relate_process'
RELATE_CHECK = 'relate_check'
RELATE_UPDATE_VIEW = 'relate_update_view'
END_TO_END_TEST = 'e2e_test'
END_TO_END_CHECK = 'e2e_check'
END_TO_END_EXECUTE = 'e2e_execute'
END_TO_END_WAIT = 'e2e_wait'
DATA_CONSISTENCY_TEST = 'data_consistency_test'
BRP_REGRESSION_TEST = 'brp_regression_test'
DISTRIBUTE = 'distribute'

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
ISSUES = 'issues'

HEARTBEAT_QUEUE = _build_queuename(STATUS_EXCHANGE, HEARTBEAT)
PROGRESS_QUEUE = _build_queuename(STATUS_EXCHANGE, PROGRESS)
LOG_QUEUE = _build_queuename(LOG_EXCHANGE, LOGS)
AUDIT_LOG_QUEUE = _build_queuename(AUDIT_LOG_EXCHANGE, LOGS)
ISSUE_QUEUE = _build_queuename(ISSUE_EXCHANGE, ISSUES)
EVERYTHING_KEY = '#'
HEARTBEAT_KEY = _build_key(STATUS, HEARTBEAT)
PROGRESS_KEY = _build_key(STATUS, PROGRESS)

COMPARE_RESULT_KEY = _build_key(COMPARE, JOBSTEP_RESULT)
EXPORT_RESULT_KEY = _build_key(EXPORT, JOBSTEP_RESULT)
EXPORT_TEST_RESULT_KEY = _build_key(EXPORT_TEST, JOBSTEP_RESULT)
FULLUPDATE_RESULT_KEY = _build_key(FULLUPDATE, JOBSTEP_RESULT)
APPLY_RESULT_KEY = _build_key(APPLY, JOBSTEP_RESULT)
IMPORT_RESULT_KEY = _build_key(IMPORT, JOBSTEP_RESULT)
PREPARE_RESULT_KEY = _build_key(PREPARE, JOBSTEP_RESULT)
RELATE_PREPARE_RESULT_KEY = _build_key(RELATE_PREPARE, JOBSTEP_RESULT)
RELATE_PROCESS_RESULT_KEY = _build_key(RELATE_PROCESS, JOBSTEP_RESULT)
RELATE_UPDATE_VIEW_RESULT_KEY = _build_key(RELATE_UPDATE_VIEW, JOBSTEP_RESULT)
RELATE_CHECK_RESULT_KEY = _build_key(RELATE_CHECK, JOBSTEP_RESULT)
END_TO_END_TEST_RESULT_KEY = _build_key(END_TO_END_TEST, JOBSTEP_RESULT)
END_TO_END_CHECK_RESULT_KEY = _build_key(END_TO_END_CHECK, JOBSTEP_RESULT)
END_TO_END_EXECUTE_RESULT_KEY = _build_key(END_TO_END_EXECUTE, JOBSTEP_RESULT)
END_TO_END_WAIT_RESULT_KEY = _build_key(END_TO_END_WAIT, JOBSTEP_RESULT)
DATA_CONSISTENCY_TEST_RESULT_KEY = _build_key(DATA_CONSISTENCY_TEST, JOBSTEP_RESULT)
BRP_REGRESSION_TEST_RESULT_KEY = _build_key(BRP_REGRESSION_TEST, JOBSTEP_RESULT)
DISTRIBUTE_RESULT_KEY = _build_key(DISTRIBUTE, JOBSTEP_RESULT)

ALL_JOBSTEP_RESULTS_KEY = _build_key('*', JOBSTEP_RESULT)
ALL_TASK_RESULTS_KEY = _build_key('*', TASK_RESULT)

# Queues and keys for prepare tasks
PREPARE_TASK_RESULT_KEY = _build_key(PREPARE, TASK_RESULT)
PREPARE_TASK_COMPLETE_KEY = _build_key(PREPARE, TASK_COMPLETE)
PREPARE_TASK_REQUEST_KEY = _build_key(PREPARE, TASK_REQUEST)
PREPARE_TASK_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, PREPARE, TASK)
PREPARE_COMPLETE_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, PREPARE, COMPLETE)

COMPARE_REQUEST_KEY = _build_key(COMPARE, REQUEST)
EXPORT_REQUEST_KEY = _build_key(EXPORT, REQUEST)
EXPORT_TEST_REQUEST_KEY = _build_key(EXPORT_TEST, REQUEST)
FULLUPDATE_REQUEST_KEY = _build_key(FULLUPDATE, REQUEST)
APPLY_REQUEST_KEY = _build_key(APPLY, REQUEST)
IMPORT_REQUEST_KEY = _build_key(IMPORT, REQUEST)
PREPARE_REQUEST_KEY = _build_key(PREPARE, REQUEST)
RELATE_PREPARE_REQUEST_KEY = _build_key(RELATE_PREPARE, REQUEST)
RELATE_PROCESS_REQUEST_KEY = _build_key(RELATE_PROCESS, REQUEST)
RELATE_CHECK_REQUEST_KEY = _build_key(RELATE_CHECK, REQUEST)
RELATE_UPDATE_VIEW_REQUEST_KEY = _build_key(RELATE_UPDATE_VIEW, REQUEST)
TASK_REQUEST_KEY = _build_key(TASK, REQUEST)
WORKFLOW_REQUEST_KEY = _build_key(WORKFLOW, REQUEST)
END_TO_END_TEST_REQUEST_KEY = _build_key(END_TO_END_TEST, REQUEST)
END_TO_END_CHECK_REQUEST_KEY = _build_key(END_TO_END_CHECK, REQUEST)
END_TO_END_EXECUTE_REQUEST_KEY = _build_key(END_TO_END_EXECUTE, REQUEST)
END_TO_END_WAIT_REQUEST_KEY = _build_key(END_TO_END_WAIT, REQUEST)
DATA_CONSISTENCY_TEST_REQUEST_KEY = _build_key(DATA_CONSISTENCY_TEST, REQUEST)
BRP_REGRESSION_TEST_REQUEST_KEY = _build_key(BRP_REGRESSION_TEST, REQUEST)
DISTRIBUTE_REQUEST_KEY = _build_key(DISTRIBUTE, REQUEST)

COMPARE_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, COMPARE)
EXPORT_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, EXPORT)
EXPORT_TEST_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, EXPORT_TEST)
FULLUPDATE_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, FULLUPDATE)
APPLY_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, APPLY)
IMPORT_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, IMPORT)
PREPARE_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, PREPARE)
RELATE_PREPARE_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, RELATE_PREPARE)
RELATE_PROCESS_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, RELATE_PROCESS)
RELATE_CHECK_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, RELATE_CHECK)
RELATE_UPDATE_VIEW_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, RELATE_UPDATE_VIEW)
TASK_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, TASK)
WORKFLOW_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, WORKFLOW)
END_TO_END_TEST_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, END_TO_END_TEST)
END_TO_END_CHECK_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, END_TO_END_CHECK)
END_TO_END_EXECUTE_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, END_TO_END_EXECUTE)
END_TO_END_WAIT_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, END_TO_END_WAIT)
DATA_CONSISTENCY_TEST_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, DATA_CONSISTENCY_TEST)
BRP_REGRESSION_TEST_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, BRP_REGRESSION_TEST)
DISTRIBUTE_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, DISTRIBUTE)

JOBSTEP_RESULT_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, JOBSTEP_RESULT)
TASK_RESULT_QUEUE = _build_queuename(WORKFLOW_EXCHANGE, TASK_RESULT)

"""
Queue configuration is leading in the setup of the message broker.

Configuration defines the exchanges that will be created, and the keys the exchange should route to which queues.
The message broker will be initialised based on this configuration.
"""
QUEUE_CONFIGURATION = {
    WORKFLOW_EXCHANGE: {
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
        RELATE_PREPARE_QUEUE: [
            RELATE_PREPARE_REQUEST_KEY,
        ],
        RELATE_PROCESS_QUEUE: [
            RELATE_PROCESS_REQUEST_KEY,
        ],
        RELATE_CHECK_QUEUE: [
            RELATE_CHECK_REQUEST_KEY,
        ],
        RELATE_UPDATE_VIEW_QUEUE: [
            RELATE_UPDATE_VIEW_REQUEST_KEY,
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
        END_TO_END_TEST_QUEUE: [
            END_TO_END_TEST_REQUEST_KEY,
        ],
        END_TO_END_CHECK_QUEUE: [
            END_TO_END_CHECK_REQUEST_KEY,
        ],
        END_TO_END_EXECUTE_QUEUE: [
            END_TO_END_EXECUTE_REQUEST_KEY,
        ],
        END_TO_END_WAIT_QUEUE: [
            END_TO_END_WAIT_REQUEST_KEY,
        ],
        DATA_CONSISTENCY_TEST_QUEUE: [
            DATA_CONSISTENCY_TEST_REQUEST_KEY,
        ],
        BRP_REGRESSION_TEST_QUEUE: [
            BRP_REGRESSION_TEST_REQUEST_KEY,
        ],
        DISTRIBUTE_QUEUE: [
            DISTRIBUTE_REQUEST_KEY,
        ],
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
        ]
    },
    AUDIT_LOG_EXCHANGE: {
        AUDIT_LOG_QUEUE: [
            EVERYTHING_KEY,
        ]
    },
    ISSUE_EXCHANGE: {
        ISSUE_QUEUE: [
            EVERYTHING_KEY,
        ]
    }
}
