import io
import os
import pandas
import re


from objectstore.objectstore import get_full_container_list, get_object


def query_objectstore(connection, config):
    """Reads from the objectstore

    The Amsterdam/objectstore library is used to connect to the container

    :return: a list of data
    """
    # Allow the container name to be set in the config or else get it from the env
    container_name = config.get('container', os.getenv("CONTAINER_BASE", "acceptatie"))
    file_filter = config.get("file_filter", ".*")
    file_type = config.get("file_type")

    # Use the container base env variable
    result = get_full_container_list(connection, container_name)
    pattern = re.compile(file_filter)

    for row in result:
        if pattern.match(row["name"]):
            file_info = dict(row)    # File information
            if file_type in ["XLS", "CSV", "UVA2"]:
                obj = get_object(connection, row, container_name)
                if file_type == "XLS":
                    # Include (non-empty) Excel rows
                    _read = _read_xls
                elif file_type == "UVA2":
                    _read = _read_uva2
                else:
                    _read = _read_csv
                yield from _read(obj, file_info, config)
            else:
                # Include file attributes
                yield file_info


def read_from_objectstore(connection, config):
    """Reads from the objectstore

    The Amsterdam/objectstore library is used to connect to the container

    :return: a list of data
    """
    return [row for row in query_objectstore(connection, config)]


def _yield_rows(iterrows, file_info, config):
    """
    Yield rows from an iterrows object

    :param iterrows:
    :param file_info:
    :param config: Any operators to apply on the result ("lowercase_keys")
    :return:
    """
    for _, row in iterrows:
        empty = True
        for key, value in row.items():
            # Convert any NULL values to None values
            if pandas.isnull(value):
                row[key] = None
            else:
                # Not NULL => row is not empty
                empty = False
        if not empty:
            row["_file_info"] = file_info
            if "lowercase_keys" in config.get("operators", []):
                row = {key.lower(): value for key, value in row.items()}
            yield row


def _read_xls(obj, file_info, config):
    """Read XLS(X) object

    Read Excel object and return the (non-empty) rows

    :param obj: An Objectstore object
    :param file_info: File information (name, last_modified, ...)
    :return: The list of non-empty rows
    """
    io_obj = io.BytesIO(obj)
    excel = pandas.read_excel(io=io_obj)

    return _yield_rows(excel.iterrows(), file_info, config)


def _read_csv(obj, file_info, config):
    """Read CSV object

    Read CSV object and return the (non-empty) rows

    :param obj: An Objectstore object
    :param file_info: File information (name, last_modified, ...)
    :param config: CSV config parameters ("delimiter" character)
    :return: The list of non-empty rows
    """
    io_obj = io.BytesIO(obj)
    csv = pandas.read_csv(io_obj,
                          delimiter=config.get("delimiter", ","),
                          encoding=config.get("encoding", "UTF-8"),
                          dtype=str)

    return _yield_rows(csv.iterrows(), file_info, config)


def _read_uva2(obj, file_info, config):
    """Read UVA2 object

    Read UVA2 object and return the (non-empty) rows

    :param obj: An Objectstore object
    :param file_info: File information (name, last_modified, ...)
    :param config: CSV config parameters ("delimiter" character)
    :return: The list of non-empty rows
    """
    io_obj = io.BytesIO(obj)
    csv = pandas.read_csv(io_obj,
                          delimiter=config.get("delimiter", ";"),
                          encoding=config.get("encoding", "ascii"),
                          skiprows=config.get("skiprows", 3),
                          dtype=str)

    return _yield_rows(csv.iterrows(), file_info, config)
