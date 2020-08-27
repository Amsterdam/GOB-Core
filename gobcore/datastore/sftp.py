import paramiko

from gobcore.datastore.datastore import Datastore


class SFTPDatastore(Datastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super(SFTPDatastore, self).__init__(connection_config, read_config)

        self.response = None

    def connect(self):
        """Connect to the datasource

        The FTP lib is used to connect to the data source

        :return:
        """
        transport = paramiko.Transport((self.connection_config["host"], int(self.connection_config["port"])))
        transport.connect(username=self.connection_config["username"], password=self.connection_config["password"])

        self.connection = paramiko.SFTPClient.from_transport(transport)

    def query(self):
        pass  # pragma: no cover

    def put_file(self, src, dest):
        self.connection.put(src, dest)
