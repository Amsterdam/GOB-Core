"""Logging

This module contains the logging methods.

Logging is published on the message broker to allow for central collecting and overall reporting

Todo:
    The technical logging should be routed to the technical logging environment (eg Kibana)

"""
import logging
import datetime

from gobcore.log_publisher import LogPublisher

LOG_PUBLISHER = None


class RequestsHandler(logging.Handler):

    def emit(self, record):
        """Emits a log record on the message broker

        :param record: log record
        :return: None
        """
        log_msg = {
            "timestamp": datetime.datetime.now().isoformat(),
            "level": record.levelname,
            "name": record.name,
            "msg": record.msg,
            "formatted_msg": self.format(record)
        }

        log_msg['process_id'] = getattr(record, 'process_id', None)
        log_msg['id'] = getattr(record, 'id', None)
        log_msg['source'] = getattr(record, 'source', None)
        log_msg['application'] = getattr(record, 'application', None)
        log_msg['destination'] = getattr(record, 'destination', None)
        log_msg['catalogue'] = getattr(record, 'catalogue', None)
        log_msg['entity'] = getattr(record, 'entity', None)
        log_msg['data'] = getattr(record, 'data', None)

        global LOG_PUBLISHER
        if LOG_PUBLISHER is None:
            # Instantiate a log publisher
            LOG_PUBLISHER = LogPublisher()

        LOG_PUBLISHER.publish(record.levelname, log_msg)


class GobLogger:
    """
    GOB logger, used for application logging for the GOB system.
    Holds information to give context to subsequent logging.
    """

    _logger = {}
    LOGFORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
    LOGLEVEL = logging.INFO

    def info(self, msg, kwargs={}):
        GobLogger._logger[self._name].info(msg, extra={**self._default_args, **kwargs})

    def warning(self, msg, kwargs={}):
        GobLogger._logger[self._name].warning(msg, extra={**self._default_args, **kwargs})

    def error(self, msg, kwargs={}):
        GobLogger._logger[self._name].error(msg, extra={**self._default_args, **kwargs})

    def gob_core_error(self, msg, cause_msg, err):
        """Log the error message

        :param msg: Description of the error
        :param cause_msg: The message that caused the error
        :param err: The exception that has been raised
        :return: None
        """
        name = "CORE"
        self.set_logger(name)

        # Include the header to associate the log message with the correct processid
        GobLogger._logger[name].error(msg, extra={
            **cause_msg.get('header', {}),
            "data": {
                "error": str(err)  # Include a short error description
            }
        })

    def configure(self, msg, name, loglevel=None):
        """Configure the logger to store the relevant information for subsequent logging.
        Should be called at the start of processing of each new item.

        :param msg: the processed message
        :param name: the name of the process
        :param loglevel: the level of logging from NOTSET to CRITICAL
        """
        self._name = name
        self._default_args = {
            'process_id': msg['header']['process_id'],
            'source': msg['header']['source'],
            'application': msg['header']['application'],
            'catalogue': msg['header']['catalogue'],
            'entity': msg['header']['entity']
        }
        self.set_logger(name, loglevel)

    def set_logger(self, name, loglevel=None):
        """Sets logger if there are no loggers present with that name.

        :param name: The name of the logger instance. This name will be part of every log record
        :param loglevel: the level of logging from NOTSET to CRITICAL
        :return: None
        """

        if GobLogger._logger.get(name) is None:
            GobLogger._logger[name] = self.get_logger(name, loglevel)
            return

        if loglevel is not None:
            GobLogger._logger[name].setLevel(loglevel)

    def get_logger(self, name, loglevel=None):
        """Returns a logger instance
        get_logger creates and adds a loghandler with the given name
        Only one log handler should exist for the given name

        :param name: The name of the logger instance. This name will be part of every log record
        :param loglevel: the level of logging from NOTSET to CRITICAL
        :return: log
        """
        log = logging.getLogger(name)

        set_log_level = GobLogger.LOGLEVEL if loglevel is None else loglevel
        log.setLevel(set_log_level)

        handler = RequestsHandler()
        formatter = logging.Formatter(GobLogger.LOGFORMAT)
        handler.setFormatter(formatter)

        log.addHandler(handler)

        # Temporary logging also to stdout
        stdout_handler = logging.StreamHandler()
        log.addHandler(stdout_handler)

        return log


logger = GobLogger()
