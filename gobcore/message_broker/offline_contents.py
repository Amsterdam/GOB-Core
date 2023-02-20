"""Offload message contents

Large messages are stored outside the message broker.
This prevents the message broker from transferring large messages

"""
import gc
from typing import Callable, Any

import ijson.backends.yajl2_c as ijson  # force fastest backend
import os

from orjson import orjson

from gobcore.typesystem.json import GobTypeORJSONEncoder
from gobcore.utils import gettotalsizeof, get_filename, get_unique_name

_MAX_CONTENTS_SIZE = 8192                   # Any message contents larger than this size is stored offline
_CONTENTS = "contents"                      # The name of the message attribute to check for its contents
_CONTENTS_REF = "contents_ref"              # The name of the attribute for the reference to the offloaded contents
_MESSAGE_BROKER_FOLDER = "message_broker"   # The name of the folder where the offloaded contents are stored


class ContentsWriter:

    def __init__(self, destination: str = None):
        """The entities are written to the file as jsonlines. (see https://jsonlines.org/)"""
        unique_name = get_unique_name()
        self.filename = get_filename(unique_name, destination or _MESSAGE_BROKER_FOLDER)
        self.default = GobTypeORJSONEncoder()

    def __enter__(self):
        if os.path.exists(self.filename):
            raise FileExistsError(self.filename)

        self.file = open(self.filename, "ab")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()

        if exc_type is not None:
            os.remove(self.filename)

    def write(self, entity: dict, **kwargs):
        """
        Write a serialized entity to file seperated by '\n'

        :param entity:
        :param kwargs: optional kwargs passed to orjson.dumps
        :return:
        """
        self.file.write(
            orjson.dumps(
                entity,
                default=self.default,
                option=orjson.OPT_PASSTHROUGH_DATETIME | orjson.OPT_APPEND_NEWLINE,
                **kwargs
            )
        )


class ContentsReader:

    def __init__(self, filename):
        """
        Initialize a contents reader with the name of the file to be read

        :param filename:
        """
        self.filename = filename

    @property
    def _has_contents(self):
        try:
            return os.stat(self.filename).st_size > 0
        except OSError:
            return False

    def items(self):
        if self._has_contents:
            with open(self.filename, "rb") as fp:
                yield from ijson.items(fp, multiple_values=True, prefix="")


def offload_message(msg, converter, force_offload: bool = False):
    """Offload message content when deemed necessary.

    :param msg:
    :param converter:
    :param force_offload: Always offload message, even if content size is below
        threshold.
    :return: A message, if offloaded, without "_contents" and with "_contents_ref"
    """
    if _CONTENTS in msg:
        contents = msg[_CONTENTS]
        size = gettotalsizeof(contents)
        if force_offload or size > _MAX_CONTENTS_SIZE:
            unique_name = get_unique_name()
            filename = get_filename(unique_name, _MESSAGE_BROKER_FOLDER)
            try:
                with open(filename, 'w') as file:
                    file.write(converter(contents))
            except IOError as e:
                # When write fails, returns the msg untouched
                print(f"Offload failed ({str(e)})", filename)
                return msg
            # Replace the contents by a reference
            msg[_CONTENTS_REF] = unique_name
            del msg[_CONTENTS]
    return msg


def load_message(msg, converter: Callable[[str], str] = None, params: dict[str, Any] = None):
    """Load the message contents if it has been offloaded

    :param msg:
    :param converter:
    :param params:
    :return:
    """
    unique_name = None
    params = params or {}

    if _CONTENTS_REF in msg:
        unique_name = msg[_CONTENTS_REF]
        filename = get_filename(unique_name, _MESSAGE_BROKER_FOLDER)

        if params['stream_contents']:
            msg[_CONTENTS] = ContentsReader(filename).items()
        else:
            with open(filename, 'r') as file:
                msg[_CONTENTS] = converter(file.read())

        del msg[_CONTENTS_REF]
    return msg, unique_name


def end_message(msg, unique_name):
    """Remove the offloaded contents after a message has been succesfully handled

    end_message can always be called, it can never fail

    :param unique_name: the id of any offloaded contents
    :return: None
    """
    if unique_name:
        filename = get_filename(unique_name, _MESSAGE_BROKER_FOLDER)
        try:
            os.remove(filename)
        except Exception as e:
            print(f"Remove failed ({str(e)})", filename)

    # Clear message and run garbage collection
    msg.clear()
    gc.collect()
