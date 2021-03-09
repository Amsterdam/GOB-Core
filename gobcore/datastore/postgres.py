import psycopg2
from psycopg2.extras import DictCursor
from typing import List, Optional

from psycopg2 import Error
from psycopg2.extras import execute_values

from gobcore.datastore.sql import SqlDatastore
from gobcore.exceptions import GOBException

POSTGRES_DRIVER = 'postgresql'


class PostgresDatastore(SqlDatastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super(PostgresDatastore, self).__init__(connection_config, read_config)

        self.connection_config['drivername'] = POSTGRES_DRIVER
        self.connection = None

    def connect(self):
        try:
            self.user = f"({self.connection_config['username']}@{self.connection_config['database']})"
            self.connection = psycopg2.connect(
                database=self.connection_config['database'],
                user=self.connection_config['username'],
                password=self.connection_config['password'],
                host=self.connection_config['host'],
                port=self.connection_config['port'],
                sslmode='require',
            )
        except psycopg2.OperationalError as e:
            raise GOBException(f'Database connection for source {self.connection_config["name"]} {self.user} failed. '
                               f'Error: {e}.')
        except KeyError as e:
            raise GOBException(f'Missing configuration for source {self.connection_config["name"]}. Error: {e}')

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def query(self, query):
        """Query Postgres

        :param query:
        :return:
        """
        try:
            with self.connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                self.connection.commit()

                for row in cursor:
                    yield row
        except psycopg2.Error as e:
            raise GOBException(f'Error executing query: {query[:80]}. Error: {e}')

    def write_rows(self, table: str, rows: List[list], columns: Optional[list] = None) -> None:
        """
        Writes rows to Postgres table using the optimised execute_values function from psycopg2, which
        combines all inserts into one query.

        :param connection:
        :param table:
        :param rows:
        :param columns: columns in each row, first column item is the unique id
        :return:
        """
        if columns:
            id_name = columns[0]
            query = f"INSERT INTO {table} ({','.join(columns)}) VALUES %s " \
                f"ON CONFLICT({id_name}) " \
                f"DO UPDATE SET " \
                f"{','.join([ col + '=EXCLUDED.' + col for col in columns[1:]])}"
        else:
            query = f"INSERT INTO {table} VALUES %s"
        try:
            with self.connection.cursor() as cursor:
                execute_values(cursor, query, rows)
                self.connection.commit()
        except Error as e:
            raise GOBException(f'Error writing rows to table {table}. Error: {e}')

    def execute(self, query: str) -> None:
        """Executes Postgres query

        :param query:
        :return:
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
        except Error as e:
            raise GOBException(f'Error executing query: {query[:80]}. Error: {e}')

    def list_tables_for_schema(self, schema: str) -> List[str]:
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}'"
        result = self.query(query)
        return [row['table_name'] for row in result]

    def rename_schema(self, schema: str, new_name: str) -> None:
        query = f'ALTER SCHEMA "{schema}" RENAME TO "{new_name}"'
        self.execute(query)
