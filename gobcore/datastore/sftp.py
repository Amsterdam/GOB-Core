import paramiko
import stat
import re

from gobcore.datastore.datastore import Datastore, PutEnabledDatastore, ListEnabledDatastore, DeleteEnabledDatastore


class SFTPDatastore(Datastore, PutEnabledDatastore, ListEnabledDatastore, DeleteEnabledDatastore):

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

    def _create_directories(self, path: str):
        split_path = path.split('/')
        dircnt = len(split_path)

        for i in range(dircnt):
            try:
                self.connection.mkdir('/'.join(split_path[:i+1]))
            except OSError:
                # Directory already exists
                pass

    def put_file(self, local_file_path: str, dst_path: str):
        dest_dir = '/'.join(dst_path.split('/')[:-1])
        self._create_directories(dest_dir)
        self.connection.put(local_file_path, dst_path)

    def list_files(self, path=None):

        def list_dir_recursively(rec_path: str = ''):

            result = []
            for p in self.connection.listdir(rec_path):
                full_path = f"{rec_path}/{p}"

                if stat.S_ISDIR(self.connection.stat(full_path).st_mode):
                    result += list_dir_recursively(full_path)
                else:
                    result.append(full_path)
            return result

        # Call recursive function above, and remove leading /
        all_files = [re.sub('^/', '', item) for item in list_dir_recursively()]
        return [file for file in all_files if file.startswith(path)] if path else all_files

    def delete_file(self, filename: str):
        self.connection.remove(filename)
