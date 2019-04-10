from gobcore.exceptions import GOBException
from psycopg2 import Error
from psycopg2.extras import DictCursor


def query_postgresql(connection, query: str):
    """Query Postgres

    :param connection:
    :param query:
    :return:
    """
    try:
        cursor = connection.cursor(cursor_factory=DictCursor)
        cursor.execute(query)
        connection.commit()

        for row in cursor:
            yield row
        cursor.close()
    except Error as e:
        raise GOBException(f'Error executing query: {query[:80]}. Error: {e}')
