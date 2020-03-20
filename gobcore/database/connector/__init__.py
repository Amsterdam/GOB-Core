from gobcore.database.connector.database import connect_to_database
from gobcore.database.connector.oracle import connect_to_oracle
from gobcore.database.connector.objectstore import connect_to_objectstore
from gobcore.database.connector.postgresql import connect_to_postgresql
from gobcore.database.connector.wfs import connect_to_wfs

__all__ = ['connect_to_database', 'connect_to_objectstore', 'connect_to_oracle',
           'connect_to_postgresql', 'connect_to_wfs']
