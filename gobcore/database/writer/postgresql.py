"""
Contains database state-altering functions for Postgres: insert, drop and create functions.
"""

from typing import List

from gobcore.exceptions import GOBException
from psycopg2 import Error
from psycopg2.extras import execute_values


def write_rows_to_postgresql(connection, table: str, rows: List[list]) -> None:
    """
    Writes rows to Postgres table using the optimised execute_values function from psycopg2, which
    combines all inserts into one query.

    :param connection:
    :param table:
    :param rows:
    :return:
    """
    query = f"INSERT INTO {table} VALUES %s"
    try:
        cursor = connection.cursor()
        execute_values(cursor, query, rows)
        connection.commit()
        cursor.close()
    except Error as e:
        raise GOBException(f'Error writing rows to table {table}. Error: {e}')


def execute_postgresql_query(connection, query: str) -> None:
    """Executes Postgres query

    :param connection:
    :param query:
    :return:
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()
    except Error as e:
        raise GOBException(f'Error executing query: {query}. Error: {e}')


def drop_schema(connection, schema: str) -> None:
    """Drops schema with all its contents

    :param connection:
    :param schema:
    :return:
    """
    query = f"DROP SCHEMA IF EXISTS {schema} CASCADE"
    execute_postgresql_query(connection, query)


def create_schema(connection, schema: str) -> None:
    """Creates schema if not exists

    :param connection:
    :param schema:
    :return:
    """
    query = f"CREATE SCHEMA IF NOT EXISTS {schema}"
    execute_postgresql_query(connection, query)


def drop_table(connection, table: str) -> None:
    """Drops table

    :param connection:
    :param table:
    :return:
    """
    query = f"DROP TABLE IF EXISTS {table}"
    execute_postgresql_query(connection, query)
