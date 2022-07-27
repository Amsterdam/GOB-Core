"""Offload message contents

Large messages are stored outside the message broker.
This prevents the message broker from transferring large messages

"""
import gc
import json
import ijson
import os

from gobcore.typesystem.json import GobTypeJSONEncoder
from gobcore.utils import gettotalsizeof, get_filename, get_unique_name

_MAX_CONTENTS_SIZE = 8192                   # Any message contents larger than this size is stored offline
_CONTENTS = "contents"                      # The name of the message attribute to check for its contents
_CONTENTS_READER = "contents_reader"        # Message contents is exposed by a ContentsReader instance
_CONTENTS_REF = "contents_ref"              # The name of the attribute for the reference to the offloaded contents
_MESSAGE_BROKER_FOLDER = "message_broker"   # The name of the folder where the offloaded contents are stored


class ContentsWriter:

    def __init__(self, destination: str = None):
        """
        Opens a file
        The entities are written to the file as an array
        """
        unique_name = get_unique_name()
        destination = destination or _MESSAGE_BROKER_FOLDER
        self.filename = get_filename(unique_name, destination)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_type is not None:
            os.remove(self.filename)

    def open(self):
        self.file = open(self.filename, 'w')
        self.file.write("[")
        self.empty = True

    def write(self, entity):
        """
        Write an entity to the file

        Separate entities with a comma
        :param entity:
        :return:
        """
        if not self.empty:
            self.file.write(",\n")
        self.file.write(json.dumps(entity, cls=GobTypeJSONEncoder, allow_nan=False))
        self.empty = False

    def close(self):
        """
        Terminates the array and closes the file
        :return:
        """
        self.file.write("]")
        self.file.close()


class ContentsReader:

    def __init__(self, filename):
        """
        Initialize a contents reader with the name of the file to be read

        :param filename:
        """
        self.filename = filename

        self.file = open(self.filename, "r")
        # Use prefix='item' to get per entity (contents = [entity, entity, ...])
        self._items = ijson.items(self.file, prefix="item")

    def items(self):
        for item in self._items:
            yield item
        self.close()

    def close(self):
        self.file.close()


def offload_message(msg, converter):
    """Offload message content

    :param msg:
    :param converter:
    :return:
    """
    if _CONTENTS in msg:
        contents = msg[_CONTENTS]
        size = gettotalsizeof(contents)
        if size > _MAX_CONTENTS_SIZE:
            unique_name = get_unique_name()
            filename = get_filename(unique_name, _MESSAGE_BROKER_FOLDER)
            try:
                with open(filename, 'w') as file:
                    file.write(converter(contents))
            except IOError as e:
                # When the write fails, returns the msg untouched
                print(f"Offload failed ({str(e)})", filename)
                return msg
            # Replace the contents by a reference
            msg[_CONTENTS_REF] = unique_name
            del msg[_CONTENTS]
    return msg


def load_message(msg, converter, params):
    """Load the message contents if it has been offloaded

    :param msg:
    :param converter:
    :return:
    """
    unique_name = None
    if _CONTENTS_REF in msg:
        unique_name = msg[_CONTENTS_REF]
        filename = get_filename(unique_name, _MESSAGE_BROKER_FOLDER)
        if params['stream_contents']:
            reader = ContentsReader(filename)
            msg[_CONTENTS] = reader.items()
            msg[_CONTENTS_READER] = reader
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
        reader = msg.get(_CONTENTS_READER)
        if reader is not None:
            reader.close()

        filename = get_filename(unique_name, _MESSAGE_BROKER_FOLDER)
        try:
            os.remove(filename)
        except Exception as e:
            print(f"Remove failed ({str(e)})", filename)

    # Clear message and run garbage collection
    for key in list(msg.keys()):
        del msg[key]
    gc.collect()
