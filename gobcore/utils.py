from datetime import datetime
from itertools import chain
import os
from pathlib import Path
import socket
from sys import getsizeof
import uuid

from gobcore.message_broker.config import GOB_SHARED_DIR


def gettotalsizeof(o):
    """ Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, dict, set and frozenset.
    """
    all_handlers = {
        tuple: iter,
        list: iter,
        dict: lambda d: chain.from_iterable(d.items()),
        set: iter,
        frozenset: iter,
    }
    seen = set()  # track which object id's have already been seen
    default_size = getsizeof(0)  # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:  # do not double count the same object
            return 0

        seen.add(id(o))
        s = getsizeof(o, default_size)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)


class ProgressTicker():
    """
    Simple progress ticker
    """

    def __init__(self, name, report_interval):
        """

        :param name: the name of the process
        :param report_interval: the interval at which to print a short progress message
        """
        self._name = name
        self._report_interval = report_interval
        self._count = 0

    def __enter__(self):
        print(f"Start {self._name}")
        return self

    def __exit__(self, *args):
        print(f"End {self._name} - {self._count}")

    def tick(self):
        self._count += 1
        if self._count % self._report_interval == 0:
            print(f"{self._name} - {self._count}")


def get_hostname():
    """
    :return: a string containing the hostname of the machine where the Python interpreter is currently executing
    """
    return socket.gethostname()


def get_ip_address():
    """
    :return: a string containing the IPv4 address of the hostname
    """
    return socket.gethostbyname(get_hostname())


def get_dns():
    """
    :return: a string containing the IPv4 address of the dns resolver or None if not found
    """
    try:
        with open('/etc/resolv.conf') as fp:
            for line in fp.readlines():
                columns = line.split()
                if len(columns) >= 2 and columns[0] == 'nameserver':
                    return columns[1:][0]
    except Exception:
        pass


def get_host_info():
    """
    :return: a dictionary containing hostname, ip adress and dns of the machine
             where the Python interpreter is currently executing
    """
    return {
        'name': get_hostname(),
        'address': get_ip_address(),
        'dns': get_dns()
    }


def get_unique_name():
    """Returns a unique name for files to offload content

    :return:
    """
    now = datetime.utcnow().strftime("%Y%m%d.%H%M%S")  # Start with a timestamp
    unique = str(uuid.uuid4())  # Add a random uuid
    return f"{now}.{unique}"


def get_filename(name: str, offload_folder: str) -> str:
    """Returns the full filename given a the name of a file, creates parent folders if they don't exist.

    :param name: filename
    :param offload_folder: folder where file should be
    :return: full path as str
    """
    path = Path(GOB_SHARED_DIR, offload_folder)
    path.mkdir(exist_ok=True, parents=True)
    return str(path / name)
