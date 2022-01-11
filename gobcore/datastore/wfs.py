import requests

from gobcore.datastore.datastore import Datastore


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
        """Reads from the response

        The requests library is used to iterate through the items
        The `properties` values are placed at the top level.

        :return: a list of dicts
        """
        for feature in self.response.json()['features']:
            feature |= feature.pop('properties')
            yield feature
