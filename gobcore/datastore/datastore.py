from abc import ABC, abstractmethod


ORACLE = 'oracle'
POSTGRES = 'postgres'
OBJECTSTORE = 'objectstore'
WFS = 'wfs'
FILE = 'file'
SFTP = 'sftp'


class Datastore(ABC):

    @abstractmethod
    def __init__(self, connection_config: dict, read_config: dict = None):
        """

        :param connection_config: The datastore connection config
        :param read_config:
        """
        self.user = None
        self.connection_config = connection_config
        self.read_config = read_config or {}

    @abstractmethod
    def connect(self):
        pass  # pragma: no cover

    @abstractmethod
    def query(self, query):
        pass  # pragma: no cover

    def read(self, query):
        return [row for row in self.query(query)]
