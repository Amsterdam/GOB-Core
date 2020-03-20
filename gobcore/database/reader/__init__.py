from gobcore.database.reader.database import query_database, read_from_database           # noqa: F401
from gobcore.database.reader.oracle import query_oracle, read_from_oracle                 # noqa: F401
from gobcore.database.reader.objectstore import query_objectstore, read_from_objectstore  # noqa: F401
from gobcore.database.reader.postgresql import query_postgresql                           # noqa: F401
from gobcore.database.reader.wfs import query_wfs, read_from_wfs                          # noqa: F401

__all__ = ['read_from_database', 'read_from_objectstore', 'read_from_oracle', 'read_from_wfs'] + \
          ['query_database', 'query_objectstore', 'query_oracle', 'query_postgresql', 'query_wfs']
