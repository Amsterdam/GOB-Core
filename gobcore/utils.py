import socket
import uuid
from datetime import datetime
from itertools import chain
from pathlib import Path
from sys import getsizeof
from typing import Optional

from gobcore.message_broker.config import GOB_SHARED_DIR
from gobcore.message_broker.typing import Service


def gettotalsizeof(o):
    """Return the approximate memory footprint an object and all of its contents.

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


class ProgressTicker:
    """Simple progress ticker."""

    def __init__(self, name, report_interval):
        """Initialize ProgressTicker.

        :param name: the name of the process
        :param report_interval: the interval at which to print a short progress message
        """
        self._name = name
        self._report_interval = report_interval
        self._count = 0

    def __enter__(self):
        """Return ProgressTicker instance."""
        print(f"Start {self._name}")
        return self

    def __exit__(self, *args):
        """Progress ticker ended."""
        print(f"End {self._name} - {self._count}")

    def tick(self):
        """Count and print progress message when count is a multiple of report_interval."""
        self._count += 1
        if self._count % self._report_interval == 0:
            print(f"{self._name} - {self._count}")


def get_hostname() -> str:
    """Return the hostname of the machine where the Python interpreter is currently executing."""
    return socket.gethostname()


def get_ip_address() -> str:
    """Return IPv4 address of the host."""
    return socket.gethostbyname(get_hostname())


def get_dns() -> Optional[str]:
    """Return the IPv4 address of the DNS resolver or None if not found."""
    try:
        with open("/etc/resolv.conf") as fp:
            for line in fp.readlines():
                columns = line.split()
                if len(columns) >= 2 and columns[0] == "nameserver":
                    return columns[1:][0]
            return None  # pragma: no cover
    except Exception:
        return None


def get_host_info() -> dict[str, Optional[str]]:
    """Return a dictionary containing hostname, IP address and DNS resolver of the host."""
    return {"name": get_hostname(), "address": get_ip_address(), "dns": get_dns()}


def get_unique_name():
    """Return a unique name for files to offload content.

    :return:
    """
    now = datetime.utcnow().strftime("%Y%m%d.%H%M%S")  # Start with a timestamp
    unique = str(uuid.uuid4())  # Add a random uuid
    return f"{now}.{unique}"


def get_filename(name: str, offload_folder: str) -> str:
    """Return the full file path given a name of a file.

    Additionally, creates parent folders if they don't exist.

    :param name: filename
    :param offload_folder: folder where file should be
    :return: full path as str
    """
    path = Path(GOB_SHARED_DIR, offload_folder)
    path.mkdir(exist_ok=True, parents=True)
    return str(path / name)


def get_logger_name(service: Service) -> str:
    """Create a name for a logger from the service definition.

    This is defined by the `logger` key, with a fallback to the queue name.
    The returned value must be a string, nameless loggers are not allowed.

    :param service: A service, as defined in SERVICEDEFINITION.
    :return: a name to configure a logger with.
    """
    name = service.get("logger", service["queue"].split(".")[-2])

    if not isinstance(name, str):
        raise TypeError(f"Name must be str type, got: {type(name)}")

    return name.upper()
