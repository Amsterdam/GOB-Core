"""Logger

Provides for a generic logger

A Logger instance is instantiated with the name of the module that is using it.

The instance can be configured with a message that is being processed
Each log message will so be populated with the message details

"""
import logging
import datetime

from gobcore.logging.log_publisher import LogPublisher


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

        optional_attrs = ["process_id", "id", "source", "application", "destination", "catalogue", "entity", "data"]
        for optional_attr in optional_attrs:
            log_msg[optional_attr] = getattr(record, optional_attr, None)

        if RequestsHandler.LOG_PUBLISHER is None:
            # Instantiate a log publisher only once
            RequestsHandler.LOG_PUBLISHER = LogPublisher()

        RequestsHandler.LOG_PUBLISHER.publish(record.levelname, log_msg)


class Logger:
    """
    GOB logger, used for application logging for the GOB system.
    Holds information to give context to subsequent logging.
    """

    LOGFORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
    LOGLEVEL = logging.INFO

    _logger = {}

    def __init__(self, name=None):
        if name is not None:
            self.set_name(name)
        self._default_args = {}

    def info(self, msg, kwargs={}):
        Logger._logger[self._name].info(msg, extra={**self._default_args, **kwargs})

    def warning(self, msg, kwargs={}):
        Logger._logger[self._name].warning(msg, extra={**self._default_args, **kwargs})

    def error(self, msg, kwargs={}):
        Logger._logger[self._name].error(msg, extra={**self._default_args, **kwargs})

    def set_name(self, name):
        self._name = name
        self._init_logger(name)

    def set_default_args(self, default_args):
        self._default_args = default_args

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
            'application': header.get('application'),
            'catalogue': header.get('catalogue'),
            'entity': header.get('entity')
        })

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


# Export a Logger instance
# This instance needs to be configured with a message and a name
logger = Logger()
