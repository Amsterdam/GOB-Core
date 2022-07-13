"""
We use the objectstore to get/upload the latest and greatest.

import objectstore

give example config:

    OBJECTSTORE = dict(
       VERSION='2.0',
       AUTHURL='https://identity.stack.cloudvps.com/v2.0',
       TENANT_NAME='BGE000081_Handelsregister',
       TENANT_ID='0efc828b88584759893253f563b35f9b',
       USER=os.getenv('OBJECTSTORE_USER', 'handelsregister'),
       PASSWORD=os.getenv('HANDELSREGISTER_OBJECTSTORE_PASSWORD'),
       REGION_NAME='NL',
    )

    connection = objectstore.get_connection(OBJECTSTORE)

    loop over all items in 'container/dirname'

    meta_data = connection.get_full_container_list(connection, 'dirname')

    now you can loop over meta data of files

    for file_info in meta_data:
        if file_info['name'].endswith(expected_file):

        LOG.debug('Downloading: %s', (expected_file))

        new_data = objectstore.get_object(
            connection, o_info, container)

"""
import io
import logging
import os
import re
from typing import Tuple, Iterator, Union

import pandas
from swiftclient.client import Connection

from gobcore.datastore.datastore import Datastore, ListEnabledDatastore, PutEnabledDatastore, DeleteEnabledDatastore
from gobcore.exceptions import GOBException

LOG = logging.getLogger('objectstore')

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("swiftclient").setLevel(logging.WARNING)


OBJECTSTORE = {
    'VERSION': '2.0',
    'AUTHURL': 'https://identity.stack.cloudvps.com/v2.0',
    'TENANT_NAME': os.getenv('TENANT_NAME'),
    'TENANT_ID': os.getenv('TENANT_ID'),
    'USER': os.getenv('OBJECTSTORE_USER'),
    'PASSWORD': os.getenv('OBJECTSTORE_PASSWORD'),
    'REGION_NAME': 'NL'
}


def get_connection(store_settings: dict = None) -> Connection:
    """
    get an objectstore connection
    """
    store = store_settings or OBJECTSTORE

    os_options = {
        'tenant_id': store['TENANT_ID'],
        'region_name': store['REGION_NAME'],
    }

    # when we are running in cloudvps we should use internal urls
    use_internal = os.getenv('OBJECTSTORE_LOCAL', '')
    if use_internal:
        os_options['endpoint_type'] = 'internalURL'

    connection = Connection(
        authurl=store['AUTHURL'],
        user=store['USER'],
        key=store['PASSWORD'],
        tenant_name=store['TENANT_NAME'],
        auth_version=store['VERSION'],
        os_options=os_options
    )

    return connection


def get_full_container_list(conn, container, **kwargs) -> list:
    marker = kwargs.pop('marker', None)
    limit = kwargs.pop('limit', 10_000)

    while True:
        _, objects = conn.get_container(container, marker=marker, limit=limit, **kwargs)

        for object_info in objects:
            yield object_info

        if not objects or len(objects) != limit:
            break

        marker = objects[-1]['name']


def get_object(connection, object_meta_data: dict, dirname: str, chunk_size: int = 50_000_000,
               **kwargs) -> Union[Iterator[bytes], bytes]:
    """
    Download object from objectstore using chunks with `chunk_size`. (default 50mb)
    Use chunks to workaround bug: https://bugs.python.org/issue42853 (fixed in 3.9.7)
    object_meta_data is an object retured when using 'get_full_container_list'
    """
    return connection.get_object(dirname, object_meta_data['name'], resp_chunk_size=chunk_size, **kwargs)[1]


def put_object(connection, container: str, object_name: str, contents, content_type: str, **kwargs):
    """
    Put file to objectstore

    container == "path/in/store"
    object_name = "your_file_name.txt"
    contents=thefiledata (fileobject) open('ourfile', 'rb')
    content_type='csv'  / 'application/json' .. etc
    """
    connection.put_object(container, object_name, contents=contents, content_type=content_type, **kwargs)


def delete_object(connection, container: str, object_meta_data: dict, **kwargs):
    """
    Delete single object from objectstore
    """
    connection.delete_object(container, object_meta_data['name'], **kwargs)


class ObjectDatastore(Datastore, ListEnabledDatastore, PutEnabledDatastore, DeleteEnabledDatastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super(ObjectDatastore, self).__init__(connection_config, read_config)

        self.connection = None
        self.container_name = self.read_config.get('container', os.getenv("CONTAINER_BASE", "acceptatie"))

    def connect(self):
        try:
            self.user = f"({self.connection_config['USER']}@{self.connection_config['TENANT_NAME']})"
            self.connection = get_connection(self.connection_config)

        except KeyError as e:
            raise GOBException(f'Missing configuration for source {self.connection_config["name"]}. Error: {e}')
        except Exception as e:
            raise GOBException(f"Objectstore connection for source {self.connection_config['name']} failed. "
                               f"Error: {e}.")

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def query(self, query, **kwargs):
        """Reads from the objectstore

        The Amsterdam/objectstore library is used to connect to the container

        :return: a list of data
        """
        # Allow the container name to be set in the config or else get it from the env
        file_filter = self.read_config.get("file_filter", ".*")
        file_type = self.read_config.get("file_type")

        # Use the container base env variable
        result = get_full_container_list(self.connection, self.container_name)
        pattern = re.compile(file_filter)

        for row in result:
            if pattern.match(row["name"]):
                file_info = dict(row)  # File information
                if file_type in ["XLS", "CSV", "UVA2"]:
                    obj = get_object(self.connection, row, self.container_name)
                    if file_type == "XLS":
                        # Include (non-empty) Excel rows
                        _read = self._read_xls
                    elif file_type == "UVA2":
                        _read = self._read_uva2
                    else:
                        _read = self._read_csv
                    yield from _read(obj, file_info, self.read_config)
                else:
                    # Include file attributes
                    yield file_info

    def _read_xls(self, obj, file_info, config):
        """Read XLS(X) object

        Read Excel object and return the (non-empty) rows

        :param obj: An Objectstore object
        :param file_info: File information (name, last_modified, ...)
        :return: The list of non-empty rows
        """
        io_obj = io.BytesIO(obj.read())
        excel = pandas.read_excel(io=io_obj, keep_default_na=False, dtype=str, na_values='', engine='openpyxl')

        return self._yield_rows(excel.iterrows(), file_info, config)

    def _read_csv(self, obj, file_info, config):
        """Read CSV object

        Read CSV object and return the (non-empty) rows

        :param obj: An Objectstore object
        :param file_info: File information (name, last_modified, ...)
        :param config: CSV config parameters ("delimiter" character)
        :return: The list of non-empty rows
        """
        io_obj = io.BytesIO(obj.read())
        csv = pandas.read_csv(io_obj,
                              delimiter=config.get("delimiter", ","),
                              encoding=config.get("encoding", "UTF-8"),
                              dtype=str)

        return self._yield_rows(csv.iterrows(), file_info, config)

    def _read_uva2(self, obj, file_info, config):
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

        return self._yield_rows(csv.iterrows(), file_info, config)

    def _yield_rows(self, iterrows, file_info, config):
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

    def put_file(self, local_file_path: str, dst_path: str):
        with open(local_file_path, 'rb') as file:
            self.connection.put_object(self.container_name, dst_path, contents=file)

    def list_files(self, path=None):
        for f in get_full_container_list(self.connection, self.container_name):

            if path is None or f['name'].startswith(path):
                yield f['name']

    def list_filesizes(self, path=None) -> Tuple[str, int]:
        """Yield combination of filepath and filesize."""
        for f in get_full_container_list(self.connection, self.container_name):
            if path is None or f['name'].startswith(path):
                yield f['name'], int(f['bytes'])

    def delete_file(self, filename: str):
        self.connection.delete_object(self.container_name, filename)
