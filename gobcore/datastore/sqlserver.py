import pyodbc
import os

from typing import List

from gobcore.datastore.sql import SqlDatastore

# Can be ODBC driver name or path such as /usr/local/lib/libtdsodbc.so
SQLSERVER_ODBC_DRIVER = os.getenv('SQLSERVER_ODBC_DRIVER', 'ODBC Driver 18 for SQL Server')


class SqlServerDatastore(SqlDatastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super(SqlServerDatastore, self).__init__(connection_config, read_config)

    def connect(self):
        connstring = (
            f"DRIVER={{{SQLSERVER_ODBC_DRIVER}}};"
            f"SERVER={self.connection_config['host']},{self.connection_config['port']};"
            f"DATABASE={self.connection_config['database']};"
            f"ENCRYPT=optional;"  # v18 default is yes, not supported by DECOS
            f"UID={self.connection_config['username']};"
            f"PWD={self.connection_config['password']}"
        )

        self.connection = pyodbc.connect(connstring, autocommit=True)

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def query(self, query, **kwargs):
        cursor = self.connection.cursor()
        with self.connection:
            cursor.execute(query)

            for row in cursor.fetchall():
                # Convert tuple to dict
                yield {t[0]: row[i] for i, t in enumerate(row.cursor_description)}

    def write_rows(self, table: str, rows: List[list]) -> None:
        raise NotImplementedError(f"Please implement write_rows for {self.__class__}")

    def execute(self, query: str) -> None:
        raise NotImplementedError(f"Please implement execute for {self.__class__}")

    def list_tables_for_schema(self, schema: str) -> List[str]:
        raise NotImplementedError(f"Please implement write_rows for {self.__class__}")

    def rename_schema(self, schema: str, new_name: str) -> None:
        raise NotImplementedError(f"Please implement rename_schema for {self.__class__}")
