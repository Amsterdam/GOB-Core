import json

import argparse

from gobcore.logging.logger import logger
from gobcore.message_broker.utils import to_json, from_json
from gobcore.message_broker.offline_contents import offload_message, load_message

from pathlib import Path
from typing import List, Dict, Any, Callable


def default_parser(handlers: List[str]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m gobupload",
        description="GOB Upload, Compare and Relate"
    )

    # Upload task arguments
    parser.add_argument(
        "handler",
        choices=handlers,  # migrate is upload specific
        help="Which handler to run."
    )
    # make this in generiek
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
    return parser


def run_as_standalone(args: argparse.Namespace, handler: Callable, message_data: dict):
    """Run as stand-alone application.

    Parses and processes the cli arguments to a result message.
    Logging is sent to stdout.

    example: python -m gobimport import gebieden wijken DGDialog

    :return: result message
    """
    # Figure out what message is
    # message_in = construct_message(args)
    message_in, offloaded_filename = load_message(
        # msg=json.loads(args.message_data),
        msg=message_data,
        converter=from_json,
        params={"stream_contents": False}
    )

    logger.configure(message_in, str(args.handler).upper())

    # CALLBACK/HANDLER code
    message_out = handler(message_in)
    # END CALLBACK/HANDLER

    message_out_offloaded = offload_message(
        msg=message_out,
        converter=to_json,
        force_offload=True
    )
    print(f"Writing message data to {args.message_write_path}")
    write_message(message_out_offloaded, Path(args.message_write_path))
    return message_out_offloaded


def write_message(message_out: Dict[str, Any], write_path: Path) -> None:
    """Write message data to a file. Ensures parent directories exist.

    :param message_out: Message data to be written
    :param write_path: Path to write message data to. To use airflow's xcom,
        use `/airflow/xcom/return.json` as a path.
    """
    print(f"Writing message data to {write_path}")
    write_path.parent.mkdir(parents=True, exist_ok=True)
    write_path.write_text(json.dumps(message_out))
