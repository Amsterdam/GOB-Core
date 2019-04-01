def read_from_file(connection):
    """Reads from the file connection

    The pandas library is used to iterate through the items

    :return: a list of dicts
    """
    data = []
    for index, row in connection.iterrows():
        data.append(row)

    return data
