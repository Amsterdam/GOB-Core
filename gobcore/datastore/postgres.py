from contextlib import contextmanager
from collections.abc import Iterator
from typing import Any

import psycopg2
from psycopg2.extras import DictCursor, execute_values
from psycopg2.extensions import cursor

from gobcore.datastore.sql import SqlDatastore
from gobcore.exceptions import GOBException

POSTGRES_DRIVER = 'postgresql'


class PostgresDatastore(SqlDatastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super(PostgresDatastore, self).__init__(connection_config, read_config)

        self.connection_config['drivername'] = POSTGRES_DRIVER
        self.connection: psycopg2.connection | None = None

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

    @contextmanager
    def transaction_cursor(self, **kwargs) -> cursor:
        """
        Wraps cursor in a transaction. Rollback on any exception, otherwise commit.

        psycopg2 >= 2.9 disallows nested use of connection contextmanager.
        """
        try:
            with self.connection.cursor(**kwargs) as cur:
                yield cur
            self.connection.commit()
        except psycopg2.Error as e:
            self.connection.rollback()
            raise GOBException(f"Error executing query: {e}")

    def query(self, query: str, **kwargs) -> Iterator[dict[str, Any]]:
        """Query Postgres

        :param query:
        :return:
        """
        arraysize = kwargs.pop('arraysize', None)

        with self.transaction_cursor(cursor_factory=DictCursor, **kwargs) as cur:
            if arraysize:
                cur.arraysize = arraysize

            cur.execute(query)
            while results := cur.fetchmany():
                yield from results

    def write_rows(self, table: str, rows: list[list]) -> int:
        """
        Writes rows to Postgres table using the optimised execute_values function from psycopg2, which
        combines all inserts into one query.

        :param table:
        :param rows:
        :return:
        """
        with self.transaction_cursor() as cur:
            execute_values(cur, f"INSERT INTO {table} VALUES %s", rows)

        return len(rows)

    def execute(self, query: str) -> None:
        """Executes Postgres query

        :param query:
        :return:
        """
        with self.transaction_cursor() as cur:
            cur.execute(query)

    def copy_from_stdin(self, query: str, data: str) -> None:
        """Executes Postgres copy from stdin

        :params query, data
        :return:
        """
        with self.transaction_cursor() as cur:
            cur.copy_expert(query, data)

    def list_tables_for_schema(self, schema: str) -> list[str]:
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}'"
        result = self.query(query, name=None)
        return [row['table_name'] for row in result]

    def rename_schema(self, schema: str, new_name: str) -> None:
        query = f'ALTER SCHEMA "{schema}" RENAME TO "{new_name}"'
        self.execute(query)

    def is_extension_enabled(self, extension_name: str) -> bool:
        query = f"SELECT 1 FROM pg_extension WHERE extname='{extension_name}'"

        return len(list(self.query(query))) > 0

    def get_version(self) -> str:
        return next(self.query("SHOW server_version"))[0]
