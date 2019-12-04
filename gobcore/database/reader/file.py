import pandas


def query_file(connection):
    """Reads from the file connection

    The pandas library is used to iterate through the items

    :return: a list of dicts
    """
    def convert_row(row):
        def convert(v):
            return None if pandas.isnull(v) else v

        return {k: convert(v) for k, v in row.items()}

    for _, row in connection.iterrows():
        yield convert_row(row)


def read_from_file(connection):
    """Reads from the file connection

    The pandas library is used to iterate through the items

    :return: a list of dicts
    """
    return [row for row in query_file(connection)]
