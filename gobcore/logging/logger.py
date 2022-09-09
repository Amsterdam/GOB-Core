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
import sys
import threading
from collections import defaultdict
from typing import ClassVar, TYPE_CHECKING, Union

from gobcore.logging.log_publisher import LogPublisher

from gobcore.utils import gettotalsizeof, get_unique_name, get_filename


if TYPE_CHECKING:
    from gobcore.quality.issue import Issue  # pragma: no cover


class StdoutHandler(logging.StreamHandler):

    def __init__(self):
        super().__init__(stream=sys.stdout)
        self.name = "StdoutHandler"


class RequestsHandler(logging.Handler):

    LOGFORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
    LOG_PUBLISHER = None

    def __init__(self):
        super().__init__()
        self.name = "RequestsHandler"
        self.formatter = logging.Formatter(self.LOGFORMAT)

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


class Logger:
    """
    GOB logger, used for application logging for the GOB system.
    Holds information to give context to subsequent logging.
    """
    LOGLEVEL = logging.INFO

    # Save these messages to report at end of msg handling
    _SAVE_LOGS = (logging.WARNING, logging.ERROR, ExtendedLogger.DATAWARNING, ExtendedLogger.DATAERROR)

    # Save issues to file
    _ISSUES_FOLDER = "issues"   # The name of the folder where the offloaded issues are stored

    MAX_SIZE = 10_000
    SHORT_MESSAGE_SIZE = 1_000

    _logger: ClassVar[dict[str, logging.Logger]] = {}

    def __init__(self, name: str = None):
        self.name = name
        self.handlers = (StdoutHandler, RequestsHandler)

        if name is not None:
            self._init_logger()

        self._default_args = {}
        self._offload_file = None
        self._offload_filename = None
        self._issues = {}
        self._data_msg_count = defaultdict(int)
        self.messages = defaultdict(list)

    def clear_issues(self):
        self._issues.clear()
        self._data_msg_count.clear()

        # Remove the offloaded issues
        self._offload_file = None
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

    def add_issue(self, issue: "Issue", level: str):
        if not self._offload_file:
            self.open_offload_file()

        id_ = issue.get_unique_id()
        if self._issues.get(id_):
            from gobcore.quality.issue import Issue

            # Get the pointer to the issue in the offload file, seek the line and remove any comma or newline
            issue_offset = self._issues.get(id_)
            self._offload_file.seek(issue_offset)

            existing_issue_string = self._offload_file.readline().rstrip("\n")

            # Join this issue with an already existing issue for the same check, attribute and entity
            existing_issue = Issue.from_json(existing_issue_string)
            existing_issue.join_issue(issue)

            # Return to the end of the file and write the new issue
            self._offload_file.seek(0, io.SEEK_END)
            self._issues[id_] = self.write_issue(existing_issue)
        else:
            # Write this issue and save the byte offset to the issue
            self._issues[id_] = self.write_issue(issue)
            self._data_msg_count['data_' + level] += 1  # level comes from QA_LEVEL

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

    def _save_log(self, level: int, msg: str):
        if level in Logger._SAVE_LOGS:
            self.messages[level].append(msg)

    def get_warnings(self) -> list[str]:
        return self.messages[logging.WARNING] + self.messages[ExtendedLogger.DATAWARNING]

    def get_errors(self) -> list[str]:
        return self.messages[logging.ERROR] + self.messages[ExtendedLogger.DATAERROR]

    def get_log_counts(self) -> dict:
        return dict(self._data_msg_count)  # possible empty defaultdict

    def get_summary(self) -> dict[str, Union[list[str], dict]]:
        return {
            'warnings': self.get_warnings(),
            'errors': self.get_errors(),
            'log_counts': self.get_log_counts(),
        }

    def _log(self, level: int, msg: str, kwargs=None):
        """
        Logs the message at the given level

        If the msg is larger than MAX_SIZE the logging is skipped
        :param level: info, warning, error, ...
        :param msg:
        :param kwargs:
        :return: None
        """
        extra = self._default_args | (kwargs or {})

        size = gettotalsizeof(msg) + gettotalsizeof(extra)
        if size > self.MAX_SIZE:
            msg = f"{msg[:self.SHORT_MESSAGE_SIZE]}..."
            extra = self._default_args

        self.get_logger().log(level, msg, extra=extra)
        self._save_log(level, msg)

    def info(self, msg: str, kwargs: dict = None):
        self._log(logging.INFO, msg, kwargs)

    def warning(self, msg: str, kwargs: dict = None):
        self._log(logging.WARNING, msg, kwargs)

    def error(self, msg: str, kwargs: dict = None):
        self._log(logging.ERROR, msg, kwargs)

    def data_info(self, msg: str, kwargs: dict = None):
        self._data_msg_count['data_info'] += 1
        self._log(ExtendedLogger.DATAINFO, msg, kwargs)

    def data_warning(self, msg: str, kwargs: dict = None):
        self._data_msg_count['data_warning'] += 1
        self._log(ExtendedLogger.DATAWARNING, msg, kwargs)

    def data_error(self, msg: str, kwargs: dict = None):
        self._data_msg_count['data_error'] += 1
        self._log(ExtendedLogger.DATAERROR, msg, kwargs)

    def get_attribute(self, attribute):
        return self._default_args.get(attribute)

    def configure(self, msg: dict, name: str = None, standalone: bool = False):
        """Configure the logger to store the relevant information for subsequent logging.
        Should be called at the start of processing new item.

        :param msg: the processed message
        :param name: the name of the process that processes the message
        :param standalone: standalone mode, only use stdout handler
        """
        if name is None and self.name is None:
            raise ValueError("Name not supllied in either Logger or Logger.configure")

        if standalone:
            self.handlers = (StdoutHandler, )

        if name:
            self.name = name
            self._init_logger()

        self.messages.clear()

        if header := msg.get("header", {}):
            self._default_args = {
                'process_id': header.get('process_id'),
                'source': header.get('source'),
                'destination': header.get('destination'),
                'application': header.get('application'),
                'catalogue': header.get('catalogue'),
                'entity': header.get('entity', header.get('collection')),
                'jobid': header.get('jobid'),
                'stepid': header.get('stepid')
            }

    def _init_logger(self):
        assert self.name is not None, "Name not set"

        if self.name in self._logger:
            return

        new_logger = logging.getLogger(self.name)
        new_logger.setLevel(self.LOGLEVEL)

        for handler in self.handlers:
            new_logger.addHandler(handler())

        self.set_logger(new_logger)

    def get_logger(self) -> logging.Logger:
        return self._logger[self.name]

    @classmethod
    def set_logger(cls, inst: logging.Logger):
        cls._logger[inst.name] = inst


class LoggerManager:
    """LoggerManager

    Manages loggers per thread. Each thread has its own 'global' logger. LoggerManager proxies all calls to the logger
    to the appropriate Logger instance for that thread.
    """
    loggers: ClassVar[defaultdict[int, Logger]] = defaultdict(Logger)

    @classmethod
    def get_logger(cls) -> Logger:
        """Returns existing Logger for thread, or creates a new instance."""
        return cls.loggers[threading.current_thread().ident]

    @classmethod
    def __getattr__(cls, item):
        """Proxy method. Returns result from executed item."""
        def method(*args, **kwargs):
            return getattr(cls.get_logger(), item)(*args, **kwargs)

        return method

    @property
    def name(self) -> str:
        return self.get_logger().name


logger = LoggerManager()

# Initialize the extended logger to add custom log levels to the logging module
ExtendedLogger.initialize()

# Use our ExtendedLogger with data logging as the default logger
logging.setLoggerClass(ExtendedLogger)
