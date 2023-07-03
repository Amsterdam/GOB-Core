from abc import ABC, abstractmethod
from typing import List

from gobcore.datastore.datastore import Datastore


class SqlDatastore(Datastore, ABC):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super().__init__(connection_config, read_config)

        self.connection = None

    def disconnect(self):
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass
            self.connection = None

    @abstractmethod
    def write_rows(self, table: str, rows: List[list]) -> None:
        pass  # pragma: no cover

    @abstractmethod
    def execute(self, query: str) -> None:
        pass  # pragma: no cover

    @abstractmethod
    def copy_expert(self, query: str, data: str) -> None:
        pass  # pragma: no cover

    @abstractmethod
    def list_tables_for_schema(self, schema: str) -> List[str]:
        pass  # pragma: no cover

    @abstractmethod
    def rename_schema(self, schema: str, new_name: str) -> None:
        pass  # pragma: no cover

    def drop_schema(self, schema: str) -> None:
        """Drops schema with all its contents

        :param schema:
        :return:
        """
        query = f"DROP SCHEMA IF EXISTS {schema} CASCADE"
        self.execute(query)

    def create_schema(self, schema: str) -> None:
        """Creates schema if not exists

        :param schema:
        :return:
        """
        query = f"CREATE SCHEMA IF NOT EXISTS {schema}"
        self.execute(query)

    def drop_table(self, table: str, cascade=True) -> None:
        """Drops table

        :param cascade:
        :param table:
        :return:
        """
        query = f"DROP TABLE IF EXISTS {table}{' CASCADE' if cascade else ''}"
        self.execute(query)
