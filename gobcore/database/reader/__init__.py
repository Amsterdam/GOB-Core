from gobcore.database.reader.database import read_from_database
from gobcore.database.reader.oracle import read_from_oracle
from gobcore.database.reader.objectstore import read_from_objectstore
from gobcore.database.reader.file import read_from_file

__all__ = ['read_from_database', 'read_from_objectstore', 'read_from_file', 'read_from_oracle']
