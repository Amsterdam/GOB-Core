import io
import re
import os
import pandas

from objectstore.objectstore import get_connection, get_full_container_list, get_object
from gobcore.datastore.datastore import Datastore

from gobcore.exceptions import GOBException


class ObjectDatastore(Datastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super(ObjectDatastore, self).__init__(connection_config, read_config)

        self.connection = None

    def connect(self):
        try:
            self.user = f"({self.connection_config['USER']}@{self.connection_config['TENANT_NAME']})"
            self.connection = get_connection(self.connection_config)

        except KeyError as e:
            raise GOBException(f'Missing configuration for source {self.connection_config["name"]}. Error: {e}')
        except Exception as e:
            raise GOBException(f"Objectstore connection for source {self.connection_config['name']} failed. "
                               f"Error: {e}.")

    def query(self, query):
        """Reads from the objectstore

        The Amsterdam/objectstore library is used to connect to the container

        :return: a list of data
        """
        # Allow the container name to be set in the config or else get it from the env
        container_name = self.read_config.get('container', os.getenv("CONTAINER_BASE", "acceptatie"))
        file_filter = self.read_config.get("file_filter", ".*")
        file_type = self.read_config.get("file_type")

        # Use the container base env variable
        result = get_full_container_list(self.connection, container_name)
        pattern = re.compile(file_filter)

        for row in result:
            if pattern.match(row["name"]):
                file_info = dict(row)  # File information
                if file_type in ["XLS", "CSV", "UVA2"]:
                    obj = get_object(self.connection, row, container_name)
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
        io_obj = io.BytesIO(obj)
        excel = pandas.read_excel(io=io_obj)

        return self._yield_rows(excel.iterrows(), file_info, config)

    def _read_csv(self, obj, file_info, config):
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

    def put_file(self, src, dest, **kwargs):
        assert 'container_name' in kwargs, "container_name should be provided"

        with open(src, 'rb') as file:
            self.connection.put_object(kwargs['container_name'], dest, contents=file)
