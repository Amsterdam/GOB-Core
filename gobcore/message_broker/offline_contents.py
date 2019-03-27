"""Offload message contents

Large messages are stored outside the message broker.
This prevents the message broker from transferring large messages

"""
import gc
import uuid

import os
from datetime import datetime
from pathlib import Path

from gobcore.message_broker.config import GOB_SHARED_DIR

from gobcore.utils import getapproxsizeof

_MAX_CONTENTS_SIZE = 2048                   # Any message contents larger than this size is stored offline
_CONTENTS = "contents"                      # The name of the message attribute to check for its contents
_CONTENTS_REF = "contents_ref"              # The name of the attribute for the reference to the offloaded contents
_MESSAGE_BROKER_FOLDER = "message_broker"   # The name of the folder where the offloaded contents are stored


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
        size = getapproxsizeof(contents)
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
