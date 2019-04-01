import cx_Oracle

from gobcore.exceptions import GOBEmptyResultException


def makedict(cursor):
    """Convert cx_oracle query result to be a dictionary
    """
    cols = [d[0].lower() for d in cursor.description]

    def createrow(*args):
        return dict(zip(cols, args))

    return createrow


def output_type_handler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)


def read_from_oracle(connection, query):
    """Reads from the database

    The cx_Oracle library is used to connect to the data source for databases

    :return: a list of data
    """
    cursor = connection.cursor()
    connection.outputtypehandler = output_type_handler
    cursor.execute("\n".join(query))
    cursor.rowfactory = makedict(cursor)

    data = [obj for obj in cursor.fetchall()]

    if len(data) == 0:
        raise GOBEmptyResultException('No results found for database query')

    return data
