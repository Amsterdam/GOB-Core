from functools import partial

import json

import argparse

from gobcore.message_broker.utils import to_json, from_json
from gobcore.message_broker.offline_contents import offload_message, load_message

from pathlib import Path
from typing import Dict, Any, Callable, Tuple

Message = Dict[str, Any]


def parent_argument_parser() -> Tuple[argparse.ArgumentParser, argparse._SubParsersAction]:
    parser = argparse.ArgumentParser(
        description='Start standalone GOB Tasks',
    )
    parser.add_argument(
         "--message-data",
         default="{}",
         help="Message data used by the handler."
     )
    parser.add_argument(
        "--message-result-path",
        default="/airflow/xcom/return.json",
        help="Path to store result message."
    )
    subparsers = parser.add_subparsers(
        title="subcommands",
        help="Which handler to run.",
        dest="handler",
        required=True
    )
    return parser, subparsers


def run_as_standalone(
        args: argparse.Namespace, service_definition: dict[str, Any]
) -> Message:
    message = _build_message(args)
    print(f"Loading incoming message: {message}")
    # Load offloaded 'contents_ref'-data into message
    message_in, offloaded_filename = load_message(
        msg=message,
        converter=from_json,
        params={"stream_contents": False}
    )
    print("Fully loaded incoming message, including data:")
    print(message_in)
    handler = _get_handler(args.handler, service_definition)
    message_out = handler(message_in)
    message_out_offloaded = offload_message(
        msg=message_out,
        converter=to_json,
        force_offload=True
    )

    # Write message data over xcom
    _write_message(message_out_offloaded, Path(args.message_result_path))
    return message_out_offloaded


def _build_message(args: argparse.Namespace) -> Message:
    """Create a message from argparse arguments.

    Defaults to None if attribute has no value.

    :param args: Parsed arguments
    :return: A message with keys as required by different handlers.
    """
    header = {
        'catalogue': getattr(args, "catalogue", None),
        'collection': getattr(args, "collection", None),
        'entity': getattr(args, "collection", None),
        'attribute': getattr(args, "attribute", None),
        'application': getattr(args, "application", None),
    }
    # Prevent this value from being None, just leave it away instead.
    if hasattr(args, "mode"):
        header["mode"] = getattr(args, "mode", None)

    contents_ref = {}
    summary = {}
    # Message data as passed with --message-data
    if hasattr(args, "message_data"):
        message_data = json.loads(args.message_data)
        contents_ref = message_data.get("contents_ref", {})
        summary = message_data.get("summary", {})

    return {
        "header": header,
        "contents_ref": contents_ref,
        "summary": summary
    }


def _get_handler(handler: str, mapping: Dict[str, Any]) -> Callable:
    """Returns handler from a dictionary which is formatted like:

    mapping = {
        "handler_name": {
            "handler": some_callable
            "handler_kwargs": {"raise_exception": True}  # Optional
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
    return partial(
        handler_config["handler"],
        **handler_config.get("handler_kwargs", {})
    )


def _write_message(message_out: Dict[str, Any], write_path: Path) -> None:
    """Write message data to a file. Ensures parent directories exist.

    :param message_out: Message data to be written
    :param write_path: Path to write message data to. To use airflow's xcom,
        use `/airflow/xcom/return.json` as a path.
    """
    print(f"Writing message data to {write_path}")
    write_path.parent.mkdir(parents=True, exist_ok=True)
    write_path.write_text(json.dumps(message_out))
