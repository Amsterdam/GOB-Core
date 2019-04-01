"""Implementation of a database input connectors

The following connectors are implemented in this module:
    Database - Connects to a (oracle) database using connection details

"""
import os

from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError

from gobcore.exceptions import GOBException


def connect_to_database(config):
    """Connect to the datasource

    The SQLAlchemy library is used to connect to the data source for databases

    :return: a connection to the given database
    """
    # Set the NLS_LANG variable to UTF-8 to get the correct encoding
    os.environ["NLS_LANG"] = ".UTF8"

    try:
        user = f"({config['username']}@{config['database']})"
        engine = create_engine(config['url'])
        connection = engine.connect()

    except DBAPIError as e:
        raise GOBException(f'Database connection for source {config["name"]} {user} failed. Error: {e}.')
    except KeyError as e:
        raise GOBException(f'Missing configuration for source {config["name"]}. Error: {e}')
    else:
        return connection, user
