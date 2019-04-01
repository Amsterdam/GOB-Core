"""Implementation of Objectstore input connectors

The following connectors are implemented in this module:
    Objectstore - Connects to Objectstore using connection details

"""
from objectstore.objectstore import get_connection

from gobcore.exceptions import GOBException


def connect_to_objectstore(config):
    """Connect to the objectstore

    The Amsterdam/objectstore library is used to connect to the objectstore

    :return: a connection to the given objectstore
    """
    # Get the objectstore config based on the source application name

    try:
        user = f"({config['USER']}@{config['TENANT_NAME']})"
        connection = get_connection(config)

    except KeyError as e:
        raise GOBException(f'Missing configuration for source {config["name"]}. Error: {e}')
    except Exception as e:
        raise GOBException(f"Objectstore connection for source {config['name']} {user} failed. Error: {e}.")
    else:
        return connection, user
