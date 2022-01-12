import requests

from gobcore.datastore.datastore import Datastore
from gobcore.logging.logger import logger


class WfsDatastore(Datastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super(WfsDatastore, self).__init__(connection_config, read_config)

        self.response = None

    def connect(self):
        """Connect to the datasource

        The requests library is used to connect to the data source

        :return:
        """
        self.user = ""  # No user identification for file reads
        self.response = requests.get(self.connection_config['url'])
        assert self.response.ok, f"API Response not OK for url {self.connection_config['url']}"

    def disconnect(self):
        pass  # pragma: no cover

    def query(self, query, **kwargs):
        """Reads from the GeoJSON response.

        The requests library is used to iterate through the items
        The `properties` (should be always present) values are placed at the top level in the feature.

        :return: a list of dicts
        """
        for feature in self.response.json()['features']:
            if 'properties' not in feature:
                logger.warning("WFS feature does not contain 'properties' key")
            feature |= feature.pop('properties', {})
            yield feature
