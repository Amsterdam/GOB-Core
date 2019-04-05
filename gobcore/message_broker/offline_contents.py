"""Offload message contents

Large messages are stored outside the message broker.
This prevents the message broker from transferring large messages

"""
import gc
import uuid
import json
import ijson
import os
from datetime import datetime
from pathlib import Path

from gobcore.message_broker.config import GOB_SHARED_DIR
from gobcore.typesystem.json import GobTypeJSONEncoder
from gobcore.utils import gettotalsizeof

_MAX_CONTENTS_SIZE = 4096                   # Any message contents larger than this size is stored offline
_CONTENTS = "contents"                      # The name of the message attribute to check for its contents
_CONTENTS_REF = "contents_ref"              # The name of the attribute for the reference to the offloaded contents
_MESSAGE_BROKER_FOLDER = "message_broker"   # The name of the folder where the offloaded contents are stored


class ContentsWriter:

    def __init__(self):
        """
        Opens a file
        The entities are written to the file as an array
        """
        unique_name = _get_unique_name()
        self.filename = _get_filename(unique_name)

    def __enter__(self):
        self.file = open(self.filename, 'w')
        self.file.write("[")
        self.empty = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_type is not None:
            os.remove(self.filename)

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

    def __enter__(self):
        """
        Opens the file and provides for a generator for the items in the contents array

        :return:
        """
        self.file = open(self.filename, "r")
        # Use prefix='item' to get per entity (contents = [entity, entity, ...])
        self.items = ijson.items(self.file, prefix="item")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        After the contents has been read, closes the file and removes it

        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.file.close()
        try:
            os.remove(self.filename)
        except Exception as e:
            print(f"Remove failed ({str(e)})")


def _get_unique_name():
    """Returns a unique name to store the offloaded message contents

    :return:
    """
    now = datetime.utcnow().strftime("%Y%m%d.%H%M%S")  # Start with a timestamp
    unique = str(uuid.uuid4())  # Add a random uuid
    return f"{now}.{unique}"


def _get_filename(name):
    """Gets the full filename given a the name of a file

    The filename resolves to the file in the message broker folder

    :param name:
    :return:
    """
    dir = os.path.join(GOB_SHARED_DIR, _MESSAGE_BROKER_FOLDER)
    # Create the path if the path not yet exists
    path = Path(dir)
    path.mkdir(exist_ok=True)
    return os.path.join(dir, name)


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
            unique_name = _get_unique_name()
            filename = _get_filename(unique_name)
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


def load_message(msg, converter):
    """Load the message contents if it has been offloaded

    :param msg:
    :param converter:
    :return:
    """
    unique_name = None
    if _CONTENTS_REF in msg:
        unique_name = msg[_CONTENTS_REF]
        filename = _get_filename(unique_name)
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
        try:
            filename = _get_filename(unique_name)
            os.remove(filename)
        except Exception as e:
            print(f"Remove failed ({str(e)})", filename)

    # Clear message and run garbage collection
    for item in msg.keys():
        msg[item] = None
    gc.collect()
