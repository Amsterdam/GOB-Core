import unittest

from gobcore.events.import_message import ImportMessage, MessageMetaData
from gobcore.exceptions import GOBException

from tests.gobcore import fixtures


class TestEvents(unittest.TestCase):

    def test_MessageMetaData(self):
        # Test creation of MessageMetaData
        metadata = fixtures.get_metadata_fixture()

        header = metadata.as_header
        keys = ['source', 'timestamp', 'id_column', 'entity', 'version', 'process_id', 'model']
        for key in keys:
            self.assertIn(key, header)

        # Test failure if not all keys are present
        header['source'] = None

        with self.assertRaises(GOBException):
            MessageMetaData(**header)

        header.pop('source')
        with self.assertRaises(KeyError):
            MessageMetaData(**header)


    def test_ImportMessage(self):
        metadata = fixtures.get_metadata_fixture()
        header = metadata.as_header
        summary = 'summary'
        contents = 'contents'
        msg = ImportMessage.create_import_message(header, summary, contents)
        import_message = ImportMessage(msg)

        self.assertIsInstance(import_message.metadata, MessageMetaData)
        self.assertEqual(import_message.summary, 'summary')
        self.assertEqual(import_message.contents, 'contents')
