"""Logger

Provides for a generic logger

A Logger instance is instantiated with the name of the module that is using it.

The instance can be configured with a message that is being processed
Each log message will so be populated with the message details

"""
import datetime
import io
import json
import logging
import os
import threading
from collections import defaultdict
from typing import Optional

from gobcore.logging.log_publisher import LogPublisher

from gobcore.utils import gettotalsizeof, get_unique_name, get_filename


class RequestsHandler(logging.Handler):

    LOG_PUBLISHER = None

    def emit(self, record):
        """Emits a log record on the message broker

        :param record: log record
        :return: None
        """
        log_msg = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "level": record.levelname,
            "name": record.name,
            "msg": record.msg,
            "formatted_msg": self.format(record),
        }

        optional_attrs = ["process_id", "id", "source", "application", "destination", "catalogue", "entity", "data",
                          "jobid", "stepid"]
        for optional_attr in optional_attrs:
            log_msg[optional_attr] = getattr(record, optional_attr, None)

        if RequestsHandler.LOG_PUBLISHER is None:
            # Instantiate a log publisher only once
            RequestsHandler.LOG_PUBLISHER = LogPublisher()

        RequestsHandler.LOG_PUBLISHER.publish(record.levelname, log_msg)


class ExtendedLogger(logging.Logger):
    """
    ExtendedLogger was created to be able to log with custom log levels to
    separate data logging from process logging within GOB.
    """

    # Custom log levels for data logging, based on the regular logging levels
    DATAINFO = logging.INFO + 1
    DATAWARNING = logging.WARNING + 1
    DATAERROR = logging.ERROR + 1

    @classmethod
    def initialize(cls):
        # Add custom logging levels for Data info, warnings and errors to distinguish between data and process logging
        logging.addLevelName(ExtendedLogger.DATAINFO, 'DATAINFO')
        logging.addLevelName(ExtendedLogger.DATAWARNING, 'DATAWARNING')
        logging.addLevelName(ExtendedLogger.DATAERROR, 'DATAERROR')

    def data_info(self, msg, *args, **kwargs):
        if self.isEnabledFor(self.DATAINFO):
            self._log(self.DATAINFO, msg, args, **kwargs)

    def data_warning(self, msg, *args, **kwargs):
        if self.isEnabledFor(self.DATAWARNING):
            self._log(self.DATAWARNING, msg, args, **kwargs)

    def data_error(self, msg, *args, **kwargs):
        if self.isEnabledFor(self.DATAERROR):
            self._log(self.DATAERROR, msg, args, **kwargs)


class Logger:
    """
    GOB logger, used for application logging for the GOB system.
    Holds information to give context to subsequent logging.
    """

    LOGFORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
    LOGLEVEL = logging.INFO

    # Save these messages to report at end of msg handling
    _SAVE_LOGS = ['warning', 'error', 'data_warning', 'data_error']

    # Save issues to file
    _ISSUES_FOLDER = "issues"   # The name of the folder where the offloaded issues are stored

    MAX_SIZE = 10000
    SHORT_MESSAGE_SIZE = 1000

    _logger = {}

    def __init__(self, name=None):
        if name is not None:
            self.set_name(name)
        self._default_args = {}
        self._offload_file = None
        self._offload_filename = None

        self._clear_logs()
        self.clear_issues()

    def clear_issues(self):
        self._issues = {}
        self._data_msg_count = defaultdict(int)
        self._offload_file = None

        # Remove the offloaded issues
        if self._offload_filename:
            try:
                os.remove(self._offload_filename)
            except OSError:
                pass

    def open_offload_file(self):
        self._offload_filename = get_filename(get_unique_name(), self._ISSUES_FOLDER)
        self._offload_file = open(self._offload_filename, 'w+')

    def close_offload_file(self):
        if self._offload_file:
            self._offload_file.close()

    def add_issue(self, issue, level):
        if not self._offload_file:
            self.open_offload_file()

        id = issue.get_unique_id()
        if self._issues.get(id):
            from gobcore.quality.issue import Issue

            # Get the pointer to the issue in the offload file, seek the line and remove any comma or newline
            issue_offset = self._issues.get(id)
            self._offload_file.seek(issue_offset)

            existing_issue_string = self._offload_file.readline().rstrip("\n")

            # Join this issue with an already existing issue for the same check, attribute and entity
            existing_issue = Issue.from_json(existing_issue_string)
            existing_issue.join_issue(issue)

            # Return to the end of the file and write the new issue
            self._offload_file.seek(0, io.SEEK_END)
            self._issues[id] = self.write_issue(existing_issue)
        else:
            # Write this issue and save the byte offset to the issue
            self._issues[id] = self.write_issue(issue)
            self._data_msg_count['data_' + level] += 1

    def write_issue(self, issue):
        # Store the position in the file before the issue and return it to store a pointer
        issue_offset = self._offload_file.tell()

        self._offload_file.write(issue.json + "\n")

        # Return the current position in the file
        return issue_offset

    def has_issue(self) -> bool:
        return bool(self._issues)

    def get_issues(self):
        if self._issues:
            # First close the open file since no more issues will be added
            self.close_offload_file()

            with open(self._offload_filename, 'r') as offload_file:
                issue_byte_positions = list(self._issues.values())
                issue_byte_positions.sort()
                for issue_position in issue_byte_positions:
                    offload_file.seek(issue_position)
                    issue = json.loads(offload_file.readline().rstrip("\n"))
                    yield issue

    def _clear_logs(self):
        self.messages = {key: [] for key in Logger._SAVE_LOGS}

    def _save_log(self, level, msg):
        if level in Logger._SAVE_LOGS:
            self.messages[level].append(msg)

    def get_warnings(self):
        return self.messages['warning'] + self.messages['data_warning']

    def get_errors(self):
        return self.messages['error'] + self.messages['data_error']

    def get_log_counts(self):
        return self._data_msg_count

    def get_summary(self):
        return {
            'warnings': self.get_warnings(),
            'errors': self.get_errors(),
            'log_counts': self.get_log_counts(),
        }

    def _log(self, level, msg, kwargs=None):
        """
        Logs the message at the given level

        If the msg is larger than MAX_SIZE the logging is skipped
        :param level: info, warning, error, ...
        :param msg:
        :param kwargs:
        :return: None
        """
        logger = getattr(Logger._logger[self._name], level)
        assert logger, f"Error: invalid logging level specified ({level})"
        extra = {**self._default_args, **kwargs} if kwargs else {**self._default_args}
        size = gettotalsizeof(msg) + gettotalsizeof(extra)
        if size > self.MAX_SIZE:
            msg = f"{msg[:self.SHORT_MESSAGE_SIZE]}..."
            extra = self._default_args
        logger(msg, extra=extra)
        self._save_log(level, msg)

    def info(self, msg, kwargs=None):
        self._log('info', msg, kwargs)

    def warning(self, msg, kwargs=None):
        self._log('warning', msg, kwargs)

    def error(self, msg, kwargs=None):
        self._log('error', msg, kwargs)

    def data_info(self, msg, kwargs=None):
        self._data_msg_count['data_info'] += 1
        self._log('data_info', msg, kwargs)

    def data_warning(self, msg, kwargs=None):
        self._data_msg_count['data_warning'] += 1
        self._log('data_warning', msg, kwargs)

    def data_error(self, msg, kwargs=None):
        self._data_msg_count['data_error'] += 1
        self._log('data_error', msg, kwargs)

    def set_name(self, name):
        self._name = name

    def get_name(self):
        return getattr(self, '_name', None)

    def set_default_args(self, default_args):
        self._default_args = default_args

    def get_attribute(self, attribute):
        return self._default_args.get(attribute)

    def _message_broker_handler(self) -> logging.Handler:
        handler = RequestsHandler()
        formatter = logging.Formatter(Logger.LOGFORMAT)
        handler.setFormatter(formatter)
        return handler

    def configure(self, msg, name=None, handler: Optional[logging.Handler] = None):
        """Configure the logger to store the relevant information for subsequent logging.
        Should be called at the start of processing new item.

        :param msg: the processed message
        :param name: the name of the process that processes the message
        :param handler: Optional handler to send logging to
        """
        if name is not None:
            self.set_name(name)
            self._init_logger(name, handler)

        header = msg.get("header", {})
        self.set_default_args({
            'process_id': header.get('process_id'),
            'source': header.get('source'),
            'destination': header.get('destination'),
            'application': header.get('application'),
            'catalogue': header.get('catalogue'),
            'entity': header.get('entity', header.get('collection')),
            'jobid': header.get('jobid'),
            'stepid': header.get('stepid')
        })
        self._clear_logs()

    def _init_logger(self, name: str, handler: Optional[logging.Handler] = None):
        """Sets and initializes a logger instance for the given name

        :param name: The name of the logger instance. This name will be part of every log record
        :return: None
        """
        # init_logger creates and adds a loghandler with the given name
        # Only one log handler should exist for the given name
        if Logger._logger.get(name) is not None:
            return

        logger = logging.getLogger(name)
        logger.setLevel(Logger.LOGLEVEL)

        if handler:
            logger.addHandler(handler)
        else:
            logger.addHandler(self._message_broker_handler())

        Logger._logger[name] = logger


class LoggerManager:
    """LoggerManager

    Manages loggers per thread. Each thread has its own 'global' logger. LoggerManager proxies all calls to the logger
    to the appropriate Logger instance for that thread.
    """
    loggers = {}

    def get_logger(self):
        """Returns existing Logger for thread, or creates a new instance.

        :return:
        """
        id = threading.current_thread().ident

        if id not in LoggerManager.loggers:
            LoggerManager.loggers[id] = Logger()

        return LoggerManager.loggers[id]

    def __getattr__(self, name):
        """Proxy method.

        :param name:
        :return:
        """
        def method(*args, **kwargs):
            return getattr(self.get_logger(), name)(*args, **kwargs)
        return method


logger = LoggerManager()

# Initialize the extended logger to add custom log levels to the logging module
ExtendedLogger.initialize()

# Use our ExtendedLogger with data logging as the default logger
logging.setLoggerClass(ExtendedLogger)
