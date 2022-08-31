import argparse
import json
from gobcore.logging.logger import logger
from pathlib import Path
from typing import Dict, Any, Callable, Tuple

from gobcore.utils import get_logger_name
from gobcore.message_broker.offline_contents import offload_message, load_message
from gobcore.message_broker.utils import to_json, from_json

Message = Dict[str, Any]


def parent_argument_parser() -> Tuple[argparse.ArgumentParser, argparse._SubParsersAction]:
    """Setup parent argument parser, to which subparsers can be added.

    Add any 'handler'-functions to the returned subparser, call parse_args() on
    the parent parser when all handlers are added.

    :return: The parent parser and the subparser, to add subcommands to.
    """
    parser = argparse.ArgumentParser(
        description='Start standalone GOB Tasks',
    )
    parser.add_argument(
        "--message-data",
        required=False,
        help="Message data used by the handler."
    )
    parser.add_argument(
        "--message-result-path",
        default="/airflow/xcom/return.json",
        help="Path to store result message."
    )
    subparsers = parser.add_subparsers(
        title="handlers",
        help="Which handler to run.",
        dest="handler",
        required=True
    )
    return parser, subparsers


def run_as_standalone(
        args: argparse.Namespace, service_definition: dict[str, Any]
) -> int:
    """Runs application in standalone mode.

    Finds the handler to run from the arguments given. For 'start commands' the
    message is constructed from arguments, for example with a catalogue and
    collection. 'Non-start commands' are instructed with a message received
    from a handler called in a previous task.

    :param args: Arguments as parsed by arg parse.
    :param service_definition: A dict with keys which maps to handlers.
    :return: the resulting message data from the handler.
    """
    message = _build_message(args)
    print(f"Loading incoming message: {message}")
    # Load offloaded 'contents_ref'-data into message
    message_in, offloaded_filename = load_message(
        msg=message,
        converter=from_json,
        params={"stream_contents": False}
    )
    handler = _get_handler(args.handler, service_definition)
    logger.configure(message_in, get_logger_name(handler))
    message_out = handler(message_in)
    message_out_offloaded = offload_message(
        msg=message_out,
        converter=to_json,
        force_offload=True
    )

    _write_message(message_out_offloaded, Path(args.message_result_path))
    if errors := _get_errors(message_out):
        print(errors)  # TODO: logger.error?
        return 1

    return 0


def _build_message(args: argparse.Namespace) -> Message:
    """Create a message from argparse arguments.

    Defaults to None if attribute has no value.

    :param args: Parsed arguments
    :return: A message with keys as required by different handlers.
    """
    # Just pass on message if there is message data.
    if args.message_data is not None:
        return json.loads(args.message_data)

    header = {
        'catalogue': getattr(args, "catalogue", None),
        'collection': getattr(args, "collection", None),
        'entity': getattr(args, "collection", None),
        'attribute': getattr(args, "attribute", None),
        'application': getattr(args, "application", None),
    }

    # Prevent this value from being None, as that breaks handlers.
    # When mode is not passed, handlers switch to their own default
    if hasattr(args, "mode"):
        header["mode"] = getattr(args, "mode")

    return {
        "header": header,
    }


def _get_errors(message) -> list[str]:
    """Returns a list with errors if the result message has any.

    :param message: The message to check
    :return: The errors from the 'summary' in the message.
    """
    if "summary" not in message:
        return []

    return message["summary"].get("errors", [])


def _get_handler(handler: str, mapping: Dict[str, Any]) -> Callable:
    """Returns handler from a dictionary which is formatted like:

    mapping = {
        "handler_name": {
            "handler": some_callable
        }
    }

    This mapping usually is SERVICEDEFINITION.

    :param handler: name of the handler to lookup in the mapping.
    :param mapping: mapping formatted as described above.
    :returns: A callable.
    """
    if handler not in mapping:
        raise KeyError(f"Handler '{handler}' not defined.")

    handler_config = mapping.get(handler)
    # Apply optional keyword arguments and return partial function.
    return handler_config["handler"]


def _write_message(message_out: Message, write_path: Path) -> None:
    """Write message data to a file. Ensures parent directories exist.

    :param message_out: Message data to be written
    :param write_path: Path to write message data to. To use airflow's xcom,
        use `/airflow/xcom/return.json` as a path.
    """
    print(f"Writing message data to {write_path}")
    write_path.parent.mkdir(parents=True, exist_ok=True)
    write_path.write_text(json.dumps(message_out))
