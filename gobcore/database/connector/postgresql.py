"""Implementation of a Postgresql input connector

The following connectors are implemented in this module:
    Postgresql - Connects to a postgres database using connection details

"""
import psycopg2

from gobcore.exceptions import GOBException


def connect_to_postgresql(config):
    try:
        user = f"({config['username']}@{config['database']})"
        connection = psycopg2.connect(
            database=config['database'],
            user=config['username'],
            password=config['password'],
            host=config['host'],
            port=config['port'],
        )
    except psycopg2.OperationalError as e:
        raise GOBException(f'Database connection for source {config["name"]} {user} failed. Error: {e}.')
    except KeyError as e:
        raise GOBException(f'Missing configuration for source {config["name"]}. Error: {e}')
    else:
        return connection, user
