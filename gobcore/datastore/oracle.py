from typing import List

import re
import os
import cx_Oracle
import tempfile
import shutil

from sqlalchemy.engine.url import URL
from gobcore.datastore.sql import SqlDatastore
from gobcore.exceptions import GOBException

ORACLE_DRIVER = 'oracle+cx_oracle'


DSN_FAILOVER_TEMPLATE = """
(DESCRIPTION=
    (FAILOVER={failover})
    (LOAD_BALANCE=off)
    (CONNECT_TIMEOUT=3)
    (RETRY_COUNT=3)
    (ADDRESS_LIST={address_list})
    (CONNECT_DATA=(SERVICE_NAME={database})))
"""
DSN_FAILOVER_ADDRESS_TEMPLATE = '(ADDRESS=(PROTOCOL=tcp)(HOST={host})(PORT={port}))'

ORACLE_CONFIG = """
SQLNET.CRYPTO_CHECKSUM_CLIENT = required
SQLNET.CRYPTO_CHECKSUM_TYPES_CLIENT = (SHA512)
SQLNET.ENCRYPTION_CLIENT = required
SQLNET.ENCRYPTION_TYPES_CLIENT = (AES256)
"""


class OracleDatastore(SqlDatastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super(OracleDatastore, self).__init__(connection_config, read_config)

        self.connection_config['drivername'] = ORACLE_DRIVER
        self.connection_config['url'] = self.get_url()
        self.oracle_config_dir = tempfile.mkdtemp()
        # Reuire encryption on TCP level.
        # If th encryption is enabled, the follwoing SQL gives output
        #
        #  select NETWORK_SERVICE_BANNER from v$session_connect_info where
        #    SID = sys_context('USERENV','SID')
        #    AND NETWORK_SERVICE_BANNER LIKE '%ncryption service adapter%'
        #
        # Which will display something like:
        #   [{'network_service_banner':
        #     'AES256 Encryption service adapter for Linux: Version 19.0.0.0.0 - Production'}]
        #
        oracle_config = os.path.join(self.oracle_config_dir, 'sqlnet.ora')
        with open(oracle_config, 'w') as f:
            f.write(ORACLE_CONFIG)
        os.environ['TNS_ADMIN'] = self.oracle_config_dir

    def __del__(self):
        shutil.rmtree(self.oracle_config_dir)

    def get_url(self):
        # Allow mutiple hosts speciefied, comma separated
        items = {k: str(v).split(',')[0] if k in ('host', 'port') else v
                 for k, v in self.connection_config.items() if k != 'name'}
        url = URL(**items)

        # The Oracle driver can accept a service name instead of a SID
        service_name_pattern = re.compile(r"^\w+\.\w+\.\w+$")
        if service_name_pattern.match(self.connection_config["database"]):
            # Replace the SID by the service name
            url = str(url).replace(self.connection_config["database"],
                                   '?service_name=' + self.connection_config['database'])
        return url

    @staticmethod
    def _get_dsn(host: str, port: str, database: str):
        '''
          Return dsn in Oracle easyconnect string or a description string.
          The later if multiple hosts or ports are specified.
        '''
        def strech_list(a, b_len):
            '''
              Return list a streched to b_len filled with the last element of a
            '''
            return a + [a[-1]] * max(b_len - len(a), 0)

        def filter_whitespace(text: str):
            return ''.join(text.split())

        hosts, ports = host.split(','), port.split(',')
        ports = strech_list(ports, len(hosts))
        hosts = strech_list(hosts, len(ports))
        address_list = [DSN_FAILOVER_ADDRESS_TEMPLATE.format(host=h, port=p) for h, p in zip(hosts, ports)]

        return filter_whitespace(
            DSN_FAILOVER_TEMPLATE.format(
                failover='off' if len(address_list) == 1 else 'on',
                address_list=''.join(address_list),
                database=database
            )
        )

    def connect(self):
        """Connect to the datasource

        The cx_Oracle library is used to connect to the data source for databases

        :return: a connection to the given database
        """
        # Set the NLS_LANG variable to UTF-8 to get the correct encoding
        os.environ["NLS_LANG"] = ".UTF8"
        try:
            items = ('database', 'username', 'password', 'port', 'host')
            database, username, password, port, host = [str(self.connection_config[k]) for k in items]
            self.user = f"({username}@{database})"
            dsn = self._get_dsn(host, port, database)
            self.connection = cx_Oracle.Connection(user=username, password=password, dsn=dsn)
        except KeyError as e:
            raise GOBException(f'Missing configuration for source {self.connection_config["name"]}. Error: {e}')

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def _makedict(self, cursor):
        """Convert cx_oracle query result to be a dictionary
        """
        cols = [d[0].lower() for d in cursor.description]

        def createrow(*args):
            return dict(zip(cols, args))

        return createrow

    def _output_type_handler(self, cursor, name, defaultType, size, precision, scale):
        if defaultType == cx_Oracle.CLOB:
            return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)

    def query(self, query):
        """Reads from the database

        The cx_Oracle library is used to connect to the data source for databases

        :return: a list of data
        """
        FETCH_PER = 100  # Fetch contents in chunks, default chunk size = 100

        cursor = self.connection.cursor()
        cursor.arraysize = FETCH_PER
        self.connection.outputtypehandler = self._output_type_handler
        cursor.execute(query)
        cursor.rowfactory = self._makedict(cursor)

        for row in cursor:
            yield row

    def write_rows(self, table: str, rows: List[list]) -> None:
        raise NotImplementedError("Please implement write_rows for OracleDatastore")

    def execute(self, query: str) -> None:
        raise NotImplementedError("Please implement execute for OracleDatastore")

    def list_tables_for_schema(self, schema: str) -> List[str]:
        raise NotImplementedError("Please implement list_tables_for_schema for OracleDatastore")

    def rename_schema(self, schema: str, new_name: str) -> None:
        raise NotImplementedError("Please implement rename_schema for OracleDatastore")
