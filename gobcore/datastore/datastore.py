from abc import ABC, abstractmethod
from typing import List

ORACLE = 'oracle'
POSTGRES = 'postgresql'
OBJECTSTORE = 'objectstore'
WFS = 'wfs'
FILE = 'file'
SFTP = 'sftp'
SQL_SERVER = 'sqlserver'
BAG_EXTRACT = 'bagextract'


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
    def disconnect(self):
        pass  # pragma: no cover

    @abstractmethod
    def query(self, query, **kwargs):
        pass  # pragma: no cover

    def read(self, query):
        return [row for row in self.query(query)]

    def can_put_file(self) -> bool:
        """Whether this Datastore supports putting files

        :return:
        """
        return isinstance(self, PutEnabledDatastore)

    def can_list_file(self) -> bool:
        """Whether this Datastore supports listing its files

        :return:
        """
        return isinstance(self, ListEnabledDatastore)

    def can_delete_file(self) -> bool:
        """Whether this Datastore supports deleting files

        :return:
        """
        return isinstance(self, DeleteEnabledDatastore)


class ListEnabledDatastore(ABC):
    """"ListEnabledDatastore interface"""

    @abstractmethod
    def list_files(self, path=None) -> List[str]:  # pragma: no cover
        """Returns a list of filenames

        :param path:
        :return:
        """
        pass


class PutEnabledDatastore:
    """PutEnabledDatastore interface"""

    @abstractmethod
    def put_file(self, local_file_path: str, dst_path: str):  # pragma: no cover
        """

        :param local_file_path:
        :param dst_path:
        :return:
        """
        pass


class DeleteEnabledDatastore:
    """DeleteEnabledDatastore interface"""

    @abstractmethod
    def delete_file(self, filename: str):  # pragma: no cover
        """

        :param filename:
        :return:
        """
        pass
