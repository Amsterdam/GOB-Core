from typing import Any, Callable, TypedDict, Union


class Exchange(TypedDict, total=False):
    exchange: str


class Queue(TypedDict):
    queue: str


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


class PassArgsStandalone(TypedDict, total=False):
    pass_args_standalone: list[str]


class Service(Exchange, Queue, Key, Handler, Logger, Report, OwnThread, PassArgsStandalone):
    pass


ServiceDefinition = dict[str, Service]

Message = dict[str, dict[str, Any]]
