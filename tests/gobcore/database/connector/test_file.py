from gobcore.database.connector.file import connect_to_file, LOCAL_DATADIR

from unittest import TestCase
from unittest.mock import patch


class TestConnectFile(TestCase):

    @patch("gobcore.database.connector.file.pandas.read_csv")
    def test_connect_to_file(self, mock_read_csv):
        config = {
            'filename': 'filename.csv',
            'filetype': 'CSV',
            'separator': 'sep',
            'encoding': 'enc',
        }

        mock_read_csv.return_value = "readcsvresult"

        result = connect_to_file(config)

        expected_path = LOCAL_DATADIR + "/filename.csv"
        mock_read_csv.assert_called_with(filepath_or_buffer=expected_path,
                                         sep=config['separator'],
                                         encoding=config['encoding'],
                                         dtype=str)

        self.assertEquals(("readcsvresult", ""), result)

    def test_connect_to_file_invalid_filetype(self):
        config = {
            'filename': 'somefilename.csv',
            'filetype': 'TXT',
        }

        with self.assertRaises(NotImplementedError):
            connect_to_file(config)