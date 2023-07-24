from pathlib import Path

from typing import List, Iterator, Callable, Optional, Any

import os
import oracledb
from tempfile import TemporaryDirectory

from gobcore.datastore.sql import SqlDatastore
from gobcore.exceptions import GOBException


# Require encryption on TCP level.
# Write this config to the sqlnet.ora file and pass it to oracledb.init_oracle_client
# If the encryption is enabled, the following SQL gives output
#
#  select NETWORK_SERVICE_BANNER
#  from v$session_connect_info
#  where SID = sys_context('USERENV','SID') AND NETWORK_SERVICE_BANNER LIKE '%ncryption service adapter%'
#
# Which will display something like:
#   [{'network_service_banner': 'AES256 Encryption service adapter for Linux: Version 19.0.0.0.0 - Production'}]
ORACLE_CONFIG = """
SQLNET.CRYPTO_CHECKSUM_CLIENT = required
SQLNET.CRYPTO_CHECKSUM_TYPES_CLIENT = (SHA512)
SQLNET.ENCRYPTION_CLIENT = required
SQLNET.ENCRYPTION_TYPES_CLIENT = (AES256)
"""
os.environ["NLS_LANG"] = ".UTF8"


class OracleDatastore(SqlDatastore):

    _client_initialised = False

    # Parameter values supplied by Neuron administrator (TNSnames)
    SINGLE_HOST_PARAM = (("retry_count", "3"), ("connection_timeout", "3"))
    MULTI_HOST_PARAM = SINGLE_HOST_PARAM + (("failover", "on"), ("load_balance", "off"))

    @classmethod
    def _init_client(cls):
        """
        Initializes the Oracle client by creating a temporary directory and writing the ORACLE_CONFIG
        to a sqlnet.ora file in that directory.
        It then calls oracledb.init_oracle_client() to initialize the client.
        """
        if not cls._client_initialised:
            with TemporaryDirectory() as tmpdir:
                Path(tmpdir, "sqlnet.ora").write_text(ORACLE_CONFIG)
                oracledb.init_oracle_client(config_dir=tmpdir)

            cls._client_initialised = True

    def __init__(self, connection_config: dict, read_config: dict = None):
        self._init_client()

        expected_keys = {"host", "port", "database", "username", "password"}
        if missing := expected_keys - connection_config.keys():
            raise GOBException(
                f"Missing configuration for source '{connection_config['name']}': {','.join(sorted(missing))}"
            )

        super(OracleDatastore, self).__init__(connection_config, read_config)
        self.user = f"({self.connection_config['username']}@{self.connection_config['database']})"

    @property
    def single_host(self) -> bool:
        return len(self.connection_config["host"].split(",")) == 1

    def connect(self) -> oracledb.Connection:
        """Connect to the Oracle datasource using oracledb."""
        params = self.SINGLE_HOST_PARAM if self.single_host else self.MULTI_HOST_PARAM
        dsn = self._build_connection_string(**self.connection_config, params=dict(params))

        try:
            connection = oracledb.connect(dsn)
        except oracledb.OperationalError as e:
            raise GOBException(
                f"Database connection for source {self.connection_config['name']} {self.user} failed: {e}"
            )

        connection.outputtypehandler = self._output_type_handler
        self.connection = connection
        return connection

    def query(self, query: str, **kwargs) -> Iterator[dict[str, Any]]:
        """Return query result iterator from the database formatted as dictionary."""
        with self.connection, self.connection.cursor() as cur:
            if "arraysize" in kwargs:
                cur.arraysize = kwargs["arraysize"]

            result = cur.execute(query.rstrip(";"))
            cur.rowfactory = self._dict_cursor(cur)  # execute query before setting rowfactory
            yield from result

    def write_rows(self, table: str, rows: List[list]) -> None:
        raise NotImplementedError("Please implement write_rows for OracleDatastore")

    def execute(self, query: str) -> None:
        """Executes a SQL statement on the database and commits the changes."""
        with self.connection, self.connection.cursor() as cur:
            cur.execute(query)
            self.connection.commit()

    def list_tables_for_schema(self, schema: str) -> List[str]:
        raise NotImplementedError("Please implement list_tables_for_schema for OracleDatastore")

    def rename_schema(self, schema: str, new_name: str) -> None:
        raise NotImplementedError("Please implement rename_schema for OracleDatastore")

    @staticmethod
    def _build_connection_string(
            host: str, port: str, database: str, username: str, password: str, params: dict = None, **kwargs
    ) -> str:
        """
        Returns the connection string for the Oracle database according to 'Easy Connect' syntax.
        see: https://docs.oracle.com/en/database/oracle/oracle-database/21/netag/configuring-naming-methods.html#GUID-8C85D289-6AF3-41BC-848B-BF39D32648BA
        """
        hosts = ",".join(f"{host}:{port}" for host in host.split(","))
        params = "&".join(f"{key}={value}".lower() for key, value in params.items())
        return f"{username}/{password}@tcp://{hosts}/{database}?{params}"

    @staticmethod
    def _dict_cursor(cursor: oracledb.Cursor) -> Callable[..., dict[str, Any]]:
        """Convert query tuple a dictionary with column names as keys."""
        cols = [d[0].lower() for d in cursor.description]
        return lambda *args: dict(zip(cols, args))

    @staticmethod
    def _output_type_handler(
            cursor: oracledb.Cursor, name: str, default_type, size, precision, scale
    ) -> Optional[oracledb.Var]:
        if default_type == oracledb.CLOB:
            return cursor.var(oracledb.LONG_STRING, arraysize=cursor.arraysize)
