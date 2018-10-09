"""Logging

This module contains the logging methods.

Logging is published on the message broker to allow for central collecting and overall reporting

Todo:
    The technical logging should be routed to the technical logging environment (eg Kibana)

"""
import logging
import datetime

from gobcore.message_broker.config import LOG_QUEUE
from gobcore.message_broker import publish


LOGFORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
LOGLEVEL = logging.INFO


class RequestsHandler(logging.Handler):

    def emit(self, record):
        """Emits a log record on the message broker

        :param record: log record
        :return: None
        """
        log_msg = {
            "timestamp": datetime.datetime.now().replace(microsecond=0).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "msg": record.msg,
            "formatted_msg": self.format(record)
        }

        log_msg['process_id'] = getattr(record, 'process_id', None)
        log_msg['source'] = getattr(record, 'source', None)
        log_msg['entity'] = getattr(record, 'entity', None)
        log_msg['data'] = getattr(record, 'data', None)

        publish(LOG_QUEUE, record.levelname, log_msg)


def get_logger(name):
    """Returns a logger instance

    :param name: The name of the logger instance. This name will be part of every log record
    :return: logger
    """
    logger = logging.getLogger(name)

    logger.setLevel(LOGLEVEL)

    handler = RequestsHandler()
    formatter = logging.Formatter(LOGFORMAT)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    # Temporary loggin also to stdout
    stdout_handler = logging.StreamHandler()
    logger.addHandler(stdout_handler)

    return logger
