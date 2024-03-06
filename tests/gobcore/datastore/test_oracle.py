from unittest import TestCase
from unittest.mock import patch

import oracledb

from gobcore.datastore.oracle import OracleDatastore, GOBException


class TestOracleDatastore(TestCase):

    def setUp(self) -> None:
        self.config = {
            "username": "username",
            "password": "password",
            "host": "host1,host2",  # failover
            "port": 1234,
            "database": "SID",
            "name": "config_name"
        }
        OracleDatastore._client_initialised = True
        self.store = OracleDatastore(self.config)

    @patch("gobcore.datastore.oracle.oracledb.init_oracle_client")
    @patch("gobcore.datastore.oracle.Path")
    @patch("gobcore.datastore.oracle.TemporaryDirectory")
    def test_init(self, mock_temp, mock_path, mock_init_client):
        mock_temp.return_value.__enter__.return_value = "config_dir"

        OracleDatastore._client_initialised = False
        OracleDatastore(self.config)

        mock_path.assert_called_with("config_dir", "sqlnet.ora")
        mock_init_client.assert_called_with(config_dir="config_dir")
        assert OracleDatastore._client_initialised is True
        assert "(username@SID)" == self.store.user

        mock_init_client.reset_mock()
        OracleDatastore(self.config)
        mock_init_client.assert_not_called()

        assert oracledb.defaults.fetch_lobs is False

    def test_init_missing_keys(self):
        with self.assertRaisesRegex(GOBException, "database,password,port,username"):
            OracleDatastore({"name": "my config", "host": "my_host"})

    @patch("gobcore.datastore.oracle.oracledb.connect")
    def test_connect(self, mock_connect):
        conn = self.store.connect()

        mock_connect.assert_called_with(
            "username/password@tcp://host1:1234,host2:1234/SID?"
            "retry_count=3&connection_timeout=3&failover=on&load_balance=off"
        )
        assert self.store.connection == conn == mock_connect.return_value

        # single host
        self.config["host"] = "host"
        OracleDatastore(self.config).connect()
        mock_connect.assert_called_with(
            "username/password@tcp://host:1234/SID?retry_count=3&connection_timeout=3"
        )

    @patch("gobcore.datastore.oracle.oracledb.connect", side_effect=oracledb.OperationalError("my error"))
    def test_connect_opserror(self, _):
        with self.assertRaisesRegex(GOBException, "my error"):
            self.store.connect()

    @patch("oracledb.Cursor", autospec=True, spec_set=True, description=(("a",), ("B",), ("c",)))
    def test_makedict(self, mock_cursor):
        assert {"a": 1, "b": 2, "c": 3} == self.store._dict_cursor(mock_cursor)(*(1, 2, 3, 4))

    @patch("oracledb.Cursor", autospec=True, spec_set=True, arraysize=5, description=(("id",),))
    @patch("oracledb.Connection", autospec=True, spec_set=True)
    def test_query(self, mock_conn, mock_cursor):
        mock_cursor.execute.return_value = (mock_cursor.rowfactory(*row) for row in [(1,), (2,)])
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        self.store.connection = mock_conn

        mock_cursor.execute.return_value = (mock_cursor.rowfactory(*row) for row in [(1,), (2,)])

        query = "SELECT this FROM that WHERE this=that;"
        result = list(self.store.query(query, arraysize=5))

        assert 5 == mock_cursor.arraysize
        assert [{"id": 1}, {"id": 2}] == result
        mock_cursor.execute.assert_called_with(query[:-1])

    @patch("oracledb.Cursor", autospec=True, spec_set=True)
    @patch("oracledb.Connection", autospec=True, spec_set=True)
    def test_execute(self, mock_conn, mock_cursor):
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        self.store.connection = mock_conn

        self.store.execute("SELECT 1")

        mock_cursor.execute.assert_called_with("SELECT 1")
        mock_conn.commit.assert_called_once()

    def test_not_implemented_methods(self):
        methods = [
            ('write_rows', ['the table', []]),
            ('list_tables_for_schema', ['the schema']),
            ('rename_schema', ['old', 'new']),
        ]

        for method, args in methods:
            with self.assertRaises(NotImplementedError):
                getattr(self.store, method)(*args)
