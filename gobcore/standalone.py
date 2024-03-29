import argparse
import json
import logging
from pathlib import Path

from gobcore.logging.logger import StdoutHandler, logger
from gobcore.message_broker.offline_contents import _CONTENTS, load_message
from gobcore.message_broker.typing import Message, ServiceDefinition
from gobcore.utils import get_logger_name

LOG_HANDLERS: list[logging.Handler] = [StdoutHandler()]


def parent_argument_parser() -> tuple[argparse.ArgumentParser, argparse.Action]:
    """Set up parent argument parser, to which subparsers can be added.

    Add any 'handler'-functions to the returned subparser, call parse_args() on
    the parent parser when all handlers are added.

    :return: The parent parser and the subparser, to add subcommands to.
    """
    parser = argparse.ArgumentParser(
        description="Start standalone GOB Tasks",
    )
    parser.add_argument("--message-data", required=False, help="Message data used by the handler.")
    parser.add_argument(
        "--message-result-path", default="/airflow/xcom/return.json", help="Path to store result message."
    )
    subparsers = parser.add_subparsers(title="handlers", help="Which handler to run.", dest="handler", required=True)
    return parser, subparsers


def run_as_standalone(args: argparse.Namespace, service_definition: ServiceDefinition) -> int:
    """Run application in standalone mode.

    Finds the handler to run from the arguments given. For 'start commands' the
    message is constructed from arguments, for example with a catalogue and
    collection. 'Non-start commands' are instructed with a message received
    from a handler called in a previous task.

    :param args: Arguments as parsed by arg parse.
    :param service_definition: A dict with keys which maps to handlers.
    :return: the resulting message data from the handler.
    """
    service = service_definition[args.handler]
    pass_args = service.get("pass_args_standalone", [])
    message = _build_message(args, pass_args)
    # Load offloaded 'contents_ref'-data into message
    message_in, _ = load_message(message, params={"stream_contents": True})

    with logger.configure_context(message_in, get_logger_name(service), LOG_HANDLERS):
        message_out = service["handler"](message_in)

    # Make sure we dont serialize a content streamer
    message_out.pop(_CONTENTS, None)

    _write_message(message_out, Path(args.message_result_path))

    if errors := _get_errors(message_out):
        print(errors)  # TODO: logger.error?
        return 1

    return 0


def _build_message(args: argparse.Namespace, extra_args: list[str]) -> Message:
    """Create a message from argparse arguments.

    Defaults to None if attribute has no value.

    :param args: Parsed arguments
    :return: A message with keys as required by different handlers.
    """
    # These can be set from the command line
    header_args = [
        "catalogue",
        "collection",
        "entity",
        "attribute",
        "application",
    ] + extra_args
    header = {arg: getattr(args, arg, None) for arg in header_args}

    if args.message_data is not None:
        message = json.loads(args.message_data)
        override_header_attrs = {k: v for k, v in header.items() if v is not None}

        # Returns message_data, but overrides header attributes set from command line
        return {
            **message,
            "header": {
                **message.get("header", {}),
                **override_header_attrs,
            },
        }

    return {
        "header": header,
    }


def _get_errors(message: Message) -> list[str]:
    """Return a list with errors if the result message has any.

    :param message: The message to check
    :return: The errors from the 'summary' in the message.
    """
    if "summary" not in message:
        return []

    return message["summary"].get("errors", [])


def _write_message(message_out: Message, write_path: Path) -> None:
    """Write message data to a file. Ensures parent directories exist.

    :param message_out: Message data to be written
    :param write_path: Path to write message data to. To use airflow's xcom,
        use `/airflow/xcom/return.json` as a path.
    """
    print(f"Writing message data to {write_path}")
    write_path.parent.mkdir(parents=True, exist_ok=True)
    write_path.write_text(json.dumps(message_out))
