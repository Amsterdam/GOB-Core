from gobcore.database.connector.database import connect_to_database
from gobcore.database.connector.oracle import connect_to_oracle
from gobcore.database.connector.objectstore import connect_to_objectstore
from gobcore.database.connector.file import connect_to_file

__all__ = ['connect_to_database', 'connect_to_objectstore', 'connect_to_file', 'connect_to_oracle']
