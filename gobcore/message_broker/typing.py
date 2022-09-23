from typing import TypedDict, Callable, Union, Any


class Exchange(TypedDict, total=False):
    exchange: str


class Queue(TypedDict):
    queue: Union[str, Callable[..., Any]]


class QueueOptional(TypedDict, total=False):
    queue: str


class Key(TypedDict, total=False):
    key: str


class Logger(TypedDict, total=False):
    logger: str


class Handler(TypedDict):
    handler: Callable[[dict], dict]


class ReportDetails(Exchange, QueueOptional, Key):
    pass


class Report(TypedDict, total=False):
    report: ReportDetails


# OwnThread = TypedDict("OwnThread", {RUNS_IN_OWN_THREAD: bool}, total=False)
class OwnThread(TypedDict, total=False):
    own_thread: bool


class Service(Exchange, Queue, Key, Handler, Logger, Report, OwnThread):
    pass


ServiceDefinition = dict[str, Service]
