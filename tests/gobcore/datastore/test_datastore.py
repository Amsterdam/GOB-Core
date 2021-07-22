from typing import List
from unittest import TestCase
from unittest.mock import MagicMock

from gobcore.datastore.datastore import Datastore, ListEnabledDatastore, PutEnabledDatastore, DeleteEnabledDatastore


class DatastoreImpl(Datastore):

    def __init__(self, connection_config, read_config=None):
        super().__init__(connection_config, read_config)

    def connect(self):
        pass

    def disconnect(self):
        pass

    def query(self, query, **kwargs):
        pass


class DatastoreImplPut(Datastore, PutEnabledDatastore):
    def __init__(self, connection_config: dict, read_config: dict = None):
        super().__init__(connection_config, read_config)

    def connect(self):
        pass

    def disconnect(self):
        pass

    def query(self, query, **kwargs):
        pass

    def put_file(self, local_file_path: str, dst_path: str):
        pass


class DatastoreImplDelete(Datastore, DeleteEnabledDatastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super().__init__(connection_config, read_config)

    def connect(self):
        pass

    def disconnect(self):
        pass

    def query(self, query, **kwargs):
        pass

    def delete_file(self, filename: str):
        pass


class DatatoreImplList(Datastore, ListEnabledDatastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super().__init__(connection_config, read_config)

    def connect(self):
        pass

    def disconnect(self):
        pass

    def query(self, query, **kwargs):
        pass

    def list_files(self, path=None) -> List[str]:
        pass


class TestDatastore(TestCase):

    def test_init(self):
        connection_config = MagicMock()
        read_config = MagicMock()
        impl = DatastoreImpl(connection_config, read_config)

        self.assertEqual(connection_config, impl.connection_config)
        self.assertEqual(read_config, impl.read_config)

    def test_read(self):
        result = ['1', '2', '3']
        impl = DatastoreImpl({})
        impl.query = lambda x: iter(result)
        self.assertEqual(result, impl.read(None))

    def test_can_put_file(self):
        self.assertFalse(DatastoreImpl({}).can_put_file())
        self.assertTrue(DatastoreImplPut({}).can_put_file())

    def test_can_list_file(self):
        self.assertFalse(DatastoreImpl({}).can_list_file())
        self.assertTrue(DatatoreImplList({}).can_list_file())

    def test_can_delete_file(self):
        self.assertFalse(DatastoreImpl({}).can_delete_file())
        self.assertTrue(DatastoreImplDelete({}).can_delete_file())
