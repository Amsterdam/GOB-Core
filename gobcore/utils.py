from sys import getsizeof
from itertools import chain


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
