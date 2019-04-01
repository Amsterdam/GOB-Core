"""Implementation of a database input connectors

The following connectors are implemented in this module:
    Database - Connects to a (oracle) database using connection details

"""
import os

import cx_Oracle

from gobcore.exceptions import GOBException


def connect_to_oracle(config):
    """Connect to the datasource

    The cx_Oracle library is used to connect to the data source for databases

    :return: a connection to the given database
    """
    # Set the NLS_LANG variable to UTF-8 to get the correct encoding
    os.environ["NLS_LANG"] = ".UTF8"

    try:
        user = f"({config['username']}@{config['database']})"
        connection = cx_Oracle.Connection(
            f"{config['username']}/{config['password']}@{config['host']}:{config['port']}/{config['database']}")
    except KeyError as e:
        raise GOBException(f'Missing configuration for source {config["name"]}. Error: {e}')
    return connection, user
