from gobcore.datastore.datastore import Datastore, ORACLE, POSTGRES, OBJECTSTORE, WFS, FILE, SFTP
from gobcore.datastore.oracle import OracleDatastore
from gobcore.datastore.postgres import PostgresDatastore
from gobcore.datastore.objectstore import ObjectDatastore
from gobcore.datastore.wfs import WfsDatastore
from gobcore.datastore.file import FileDatastore
from gobcore.datastore.sftp import SFTPDatastore


class DatastoreFactory:

    @staticmethod
    def get_datastore(config: dict, read_config: dict = None) -> Datastore:
        stores = {
            ORACLE: OracleDatastore,
            POSTGRES: PostgresDatastore,
            OBJECTSTORE: ObjectDatastore,
            WFS: WfsDatastore,
            FILE: FileDatastore,
            SFTP: SFTPDatastore,
        }

        store = stores.get(config.pop('type'))

        if store is None:
            raise NotImplementedError

        return store(config, read_config)
