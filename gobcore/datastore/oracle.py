from typing import List

import re
import os
import cx_Oracle

from sqlalchemy.engine.url import URL
from gobcore.datastore.sql import SqlDatastore
from gobcore.exceptions import GOBException

ORACLE_DRIVER = 'oracle+cx_oracle'


class OracleDatastore(SqlDatastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super(OracleDatastore, self).__init__(connection_config, read_config)

        self.connection_config['drivername'] = ORACLE_DRIVER
        self.connection_config['url'] = self.get_url()

    def get_url(self):
        url = URL(**{k: v for k, v in self.connection_config.items() if k != 'name'})

        # The Oracle driver can accept a service name instead of a SID
        service_name_pattern = re.compile(r"^\w+\.\w+\.\w+$")
        if service_name_pattern.match(self.connection_config["database"]):
            # Replace the SID by the service name
            url = str(url).replace(self.connection_config["database"],
                                   '?service_name=' + self.connection_config['database'])
        return url

    def connect(self):
        """Connect to the datasource

        The cx_Oracle library is used to connect to the data source for databases

        :return: a connection to the given database
        """
        # Set the NLS_LANG variable to UTF-8 to get the correct encoding
        os.environ["NLS_LANG"] = ".UTF8"

        try:
            self.user = f"({self.connection_config['username']}@{self.connection_config['database']})"
            self.connection = cx_Oracle.Connection(
                f"{self.connection_config['username']}/{self.connection_config['password']}"
                f"@{self.connection_config['host']}:{self.connection_config['port']}"
                f"/{self.connection_config['database']}")
        except KeyError as e:
            raise GOBException(f'Missing configuration for source {self.connection_config["name"]}. Error: {e}')

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def _makedict(self, cursor):
        """Convert cx_oracle query result to be a dictionary
        """
        cols = [d[0].lower() for d in cursor.description]

        def createrow(*args):
            return dict(zip(cols, args))

        return createrow

    def _output_type_handler(self, cursor, name, defaultType, size, precision, scale):
        if defaultType == cx_Oracle.CLOB:
            return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)

    def query(self, query):
        """Reads from the database

        The cx_Oracle library is used to connect to the data source for databases

        :return: a list of data
        """
        FETCH_PER = 100  # Fetch contents in chunks, default chunk size = 100

        cursor = self.connection.cursor()
        cursor.arraysize = FETCH_PER
        self.connection.outputtypehandler = self._output_type_handler
        cursor.execute(query)
        cursor.rowfactory = self._makedict(cursor)

        for row in cursor:
            yield row

    def write_rows(self, table: str, rows: List[list]) -> None:
        raise NotImplementedError("Please implement write_rows for OracleDatastore")

    def execute(self, query: str) -> None:
        raise NotImplementedError("Please implement execute for OracleDatastore")

    def list_tables_for_schema(self, schema: str) -> List[str]:
        raise NotImplementedError("Please implement list_tables_for_schema for OracleDatastore")

    def rename_schema(self, schema: str, new_name: str) -> None:
        raise NotImplementedError("Please implement rename_schema for OracleDatastore")
