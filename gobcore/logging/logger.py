"""Logger

Provides for a generic logger

A Logger instance is instantiated with the name of the module that is using it.

The instance can be configured with a message that is being processed
Each log message will so be populated with the message details

"""
import logging
import datetime
import threading

from gobcore.logging.log_publisher import LogPublisher

from gobcore.utils import gettotalsizeof

# Custom log levels for data logging
DATAINFO = 21
DATAWARNING = 31
DATAERROR = 41


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
    def data_info(self, msg, *args, **kwargs):
        if self.isEnabledFor(DATAINFO):
            self._log(DATAINFO, msg, args, **kwargs)

    def data_warning(self, msg, *args, **kwargs):
        if self.isEnabledFor(DATAWARNING):
            self._log(DATAWARNING, msg, args, **kwargs)

    def data_error(self, msg, *args, **kwargs):
        if self.isEnabledFor(DATAERROR):
            self._log(DATAERROR, msg, args, **kwargs)


class Logger:
    """
    GOB logger, used for application logging for the GOB system.
    Holds information to give context to subsequent logging.
    """

    LOGFORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
    LOGLEVEL = logging.INFO

    _SAVE_LOGS = ['warning', 'error']  # Save these messages to report at end of msg handling

    MAX_SIZE = 10000
    SHORT_MESSAGE_SIZE = 1000

    _logger = {}

    def __init__(self, name=None):
        if name is not None:
            self.set_name(name)
        self._default_args = {}
        self._clear_logs()
        self.clear_issues()

    def clear_issues(self):
        self._issues = {}

    def add_issue(self, issue):
        id = issue.get_unique_id()
        if self._issues.get(id):
            # Join this issue with an already existing issue for the same check, attribute and entity
            self._issues[id].join_issue(issue)
        else:
            # Add this issue as a new issue
            self._issues[id] = issue

    def get_issues(self):
        return list(self._issues.values())

    def _clear_logs(self):
        self.messages = {key: [] for key in Logger._SAVE_LOGS}

    def _save_log(self, level, msg):
        if level in Logger._SAVE_LOGS:
            self.messages[level].append(msg)

    def get_warnings(self):
        return self.messages['warning']

    def get_errors(self):
        return self.messages['error']

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
        self._log('data_info', msg, kwargs)

    def data_warning(self, msg, kwargs=None):
        self._log('data_warning', msg, kwargs)

    def data_error(self, msg, kwargs=None):
        self._log('data_error', msg, kwargs)

    def set_name(self, name):
        self._name = name
        self._init_logger(name)

    def get_name(self):
        return self._name

    def set_default_args(self, default_args):
        self._default_args = default_args

    def get_attribute(self, attribute):
        return self._default_args.get(attribute)

    def configure(self, msg, name=None):
        """Configure the logger to store the relevant information for subsequent logging.
        Should be called at the start of processing new item.

        :param msg: the processed message
        :param name: the name of the process that processes the message
        """
        if name is not None:
            self.set_name(name)

        header = msg.get("header", {})
        self.set_default_args({
            'process_id': header.get('process_id'),
            'source': header.get('source'),
            'destination': header.get('destination'),
            'application': header.get('application'),
            'catalogue': header.get('catalogue'),
            'entity': header.get('entity'),
            'jobid': header.get('jobid'),
            'stepid': header.get('stepid')
        })
        self._clear_logs()

    def _init_logger(self, name):
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

        handler = RequestsHandler()
        formatter = logging.Formatter(Logger.LOGFORMAT)
        handler.setFormatter(formatter)

        logger.addHandler(handler)

        # Temporary logging also to stdout
        stdout_handler = logging.StreamHandler()
        logger.addHandler(stdout_handler)

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

# Use our ExtendedLogger with data logging as the default logger
logging.setLoggerClass(ExtendedLogger)

# Add custom logging levels for Data info, warnings and errors to distinguish between data and process logging
logging.addLevelName(DATAINFO, 'DATAINFO')
logging.addLevelName(DATAWARNING, 'DATAWARNING')
logging.addLevelName(DATAERROR, 'DATAERROR')
