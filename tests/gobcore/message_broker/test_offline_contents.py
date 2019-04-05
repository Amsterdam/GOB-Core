import mock
import unittest
from unittest.mock import mock_open
from pathlib import Path

import gobcore.message_broker.config as config
import gobcore.message_broker.offline_contents as oc


def mock_join(*args, **kwargs):
    a, b = args
    return f"{a}.{b}"


def converter(contents):
    return f"converted {contents}"


class TestOfflineContents(unittest.TestCase):

    def testUniqueName(self):
        self.assertIsInstance(oc._get_unique_name(), str)
        self.assertTrue(len(oc._get_unique_name()) > 10)
        self.assertFalse(oc._get_unique_name() == oc._get_unique_name())

    @mock.patch.object(Path, 'mkdir')
    @mock.patch('os.path.join', side_effect=mock_join)
    def testFilename(self, mocked_join, mocked_mkdir):
        # the filename returns the path to a valid filename, any missing dirs in the path will be created
        expected_dir = f"{config.GOB_SHARED_DIR}.{oc._MESSAGE_BROKER_FOLDER}"
        self.assertEqual(oc._get_filename("x"), f"{expected_dir}.x")
        mocked_mkdir.assert_called_with(exist_ok=True)

    @mock.patch('gobcore.message_broker.offline_contents._get_filename', return_value="filename")
    @mock.patch('os.remove')
    def testEndMessage(self, mocked_remove, mocked_filename):

        # End message without any contents_ref does nothing
        self.assertEqual(oc.end_message({}, {}), None)
        self.assertFalse(mocked_filename.called)
        self.assertFalse(mocked_remove.called)

        # End message with contents_ref gets the filename and removes it
        self.assertEqual(oc.end_message({}, "x"), None)
        mocked_filename.assert_called_with("x")
        mocked_remove.assert_called_with("filename")

    @mock.patch('gobcore.message_broker.offline_contents._get_unique_name', return_value="unique_name")
    @mock.patch('gobcore.message_broker.offline_contents._get_filename', return_value="filename")
    def testOffloadMessage(self, mocked_filename, mocked_unique_name):
        oc._MAX_CONTENTS_SIZE = 0

        self.assertEqual(oc.offload_message({}, converter), {})
        self.assertEqual(oc.offload_message({"any": "value"}, converter), {"any": "value"})

        mocked_writer = mock_open()
        with mock.patch('builtins.open', mocked_writer):
            self.assertEqual(oc.offload_message({"contents": "contents", "any": "value"}, converter),
                                                {"contents_ref": "unique_name", "any": "value"})
            mocked_writer.assert_called_once_with('filename', 'w')
            handle = mocked_writer()
            handle.write.assert_called_once_with('converted contents')

        '''
        Temporary disabled until a solution for sizeof is found
        # Only offload large messages
        oc._MAX_CONTENTS_SIZE = 1024

        mocked_writer = mock_open()
        with mock.patch('builtins.open', mocked_writer):
            self.assertEqual(oc.offload_message({"contents": "contents", "any": "value"}, converter),
                             {"contents": "contents", "any": "value"})
            self.assertFalse(mocked_writer.called)
        '''

    @mock.patch('gobcore.message_broker.offline_contents._get_filename', return_value="filename")
    def testLoadMessage(self, mocked_filename):
        self.assertEqual(oc.load_message({}, converter), ({}, None))

        mocked_reader = mock_open(read_data="some data")
        with mock.patch('builtins.open', mocked_reader):
            self.assertEqual(oc.load_message({"contents_ref": "unique_name", "any": "value"}, converter),
                             ({"any": "value", "contents": "converted some data"}, "unique_name"))
            mocked_reader.assert_called_once_with('filename', 'r')
            handle = mocked_reader()
            handle.read.assert_called()

    @mock.patch('os.remove')
    @mock.patch('builtins.open')
    def testContentsWriter(self, mock_open, mock_remove):
        cp_writer = None
        with oc.ContentsWriter() as writer:
            mock_open.assert_called_with(writer.filename, "w")

            writer.file.write.assert_called_with("[")
            writer.file.reset_mock()

            writer.write({})
            writer.file.write.assert_called_with("{}")
            writer.file.reset_mock()

            writer.write({})
            writer.file.write.assert_has_calls([mock.call(",\n"), mock.call("{}")])

            cp_writer = writer

        cp_writer.file.close.assert_called()
        mock_remove.assert_not_called()

        try:
            with oc.ContentsWriter() as writer:
                cp_writer = writer
                raise Exception("Any exception")
        except Exception:
            pass

        mock_remove.assert_called_with(cp_writer.filename)

    @mock.patch('gobcore.message_broker.offline_contents.ijson', mock.MagicMock())
    @mock.patch('os.remove')
    @mock.patch('builtins.open')
    def testContentsReader(self, mock_open, mock_remove):
        cp_reader = None
        with oc.ContentsReader("filename") as reader:
            mock_open.assert_called_with("filename", "r")

            cp_reader = reader

        self.assertIsNotNone(cp_reader.file)
        mock_remove.assert_called_with(cp_reader.filename)
