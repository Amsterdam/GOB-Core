from ftplib import FTP

from gobcore.datastore.datastore import Datastore


class FTPDatastore(Datastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super(FTPDatastore, self).__init__(connection_config, read_config)

        self.response = None

    def connect(self):
        """Connect to the datasource

        The FTP lib is used to connect to the data source

        :return:
        """
        ftp = FTP()
        self.connection = ftp.connect(host=self.connection_config["host"],
                                      port=int(self.connection_config["port"]))
        self.connection.login(user=self.connection_config["username"],
                              passwd=self.connection_config["password"])

    def query(self):
        pass  # pragma: no cover

    def put_file(self, src, dest):
        with open(src, "rb") as f:
            self.connection.storbinary(f"STOR {dest}", f)
