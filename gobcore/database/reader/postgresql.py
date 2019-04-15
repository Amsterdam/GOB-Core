from gobcore.exceptions import GOBException
from psycopg2 import Error
from psycopg2.extras import DictCursor


def query_postgresql(connection, query):
    """Query Postgres

    :param connection:
    :param query:
    :return:
    """
    if isinstance(query, list):
        query = "\n".join(query)
    try:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(query)
            connection.commit()

            for row in cursor:
                yield row
    except Error as e:
        raise GOBException(f'Error executing query: {query[:80]}. Error: {e}')
