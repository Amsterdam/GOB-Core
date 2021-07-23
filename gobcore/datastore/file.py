import pandas

from gobcore.datastore.datastore import Datastore


class FileDatastore(Datastore):

    def __init__(self, connection_config: dict, read_config: dict = None):
        super(FileDatastore, self).__init__(connection_config, read_config)
        self.connection = None

    def connect(self):
        """Connect to the datasource

        The pandas library is used to connect to the data source for CSV data files

        :return:
        """
        self.user = ""  # No user identification for file reads
        if self.read_config['filetype'] == "CSV":
            self.connection = pandas.read_csv(
                filepath_or_buffer=self.connection_config['filepath'],
                sep=self.read_config['separator'],
                encoding=self.read_config['encoding'],
                dtype=str)
        else:
            raise NotImplementedError

    def disconnect(self):
        pass  # pragma: no cover

    def query(self, query, **kwargs):
        """Reads from the file connection

        The pandas library is used to iterate through the items

        :param query: Unused by this method

        :return: a list of dicts
        """

        def convert_row(row):
            def convert(v):
                return None if pandas.isnull(v) else v

            return {k: convert(v) for k, v in row.items()}

        for _, row in self.connection.iterrows():
            yield convert_row(row)
