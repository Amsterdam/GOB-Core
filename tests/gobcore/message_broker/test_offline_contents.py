import mock
import unittest
from unittest.mock import mock_open, ANY
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
        self.assertIsInstance(oc.get_unique_name(), str)
        self.assertTrue(len(oc.get_unique_name()) > 10)
        self.assertFalse(oc.get_unique_name() == oc.get_unique_name())

    @mock.patch.object(Path, 'mkdir')
    def testFilename(self, mocked_mkdir):
        # the filename returns the path to a valid filename, any missing dirs in the path will be created
        expected_dir = f"{config.GOB_SHARED_DIR}/{oc._MESSAGE_BROKER_FOLDER}"
        self.assertEqual(oc.get_filename("x", oc._MESSAGE_BROKER_FOLDER), f"{expected_dir}/x")
        mocked_mkdir.assert_called_with(exist_ok=True, parents=True)

    @mock.patch('gobcore.message_broker.offline_contents.get_filename', return_value="filename")
    @mock.patch('os.remove')
    def testEndMessage(self, mocked_remove, mocked_filename):

        # End message without any contents_ref does nothing
        self.assertEqual(oc.end_message({}, {}), None)
        self.assertFalse(mocked_filename.called)
        self.assertFalse(mocked_remove.called)

        # End message with contents_ref gets the filename and removes it
        self.assertEqual(oc.end_message({}, "x"), None)
        mocked_filename.assert_called_with("x", "message_broker")
        mocked_remove.assert_called_with("filename")

    @mock.patch('gobcore.message_broker.offline_contents.get_filename', return_value="filename")
    @mock.patch('os.remove')
    def testEndMessageCloseReader(self, mocked_remove, mocked_filename):
        reader = mock.MagicMock()
        oc.end_message({'some': 'content', 'contents_reader': reader}, 'unique name')
        reader.close.assert_called_once()

    @mock.patch('gobcore.message_broker.offline_contents.get_filename', return_value="filename")
    @mock.patch('os.remove')
    @mock.patch('builtins.print')
    def testEndMessageRemoveFailed(self, mocked_print, mocked_remove, mocked_filename):
        mocked_remove.side_effect = Exception
        reader = mock.MagicMock()
        oc.end_message({'some': 'content', 'contents_reader': reader}, 'unique name')
        reader.close.assert_called_once()
        mocked_print.assert_called_once()
        self.assertTrue(mocked_print.call_args[0][0].startswith('Remove failed'))

    @mock.patch('gobcore.message_broker.offline_contents.get_unique_name', return_value="unique_name")
    @mock.patch('gobcore.message_broker.offline_contents.get_filename', return_value="filename")
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

    @mock.patch('gobcore.message_broker.offline_contents.get_unique_name', return_value="unique_name")
    @mock.patch('gobcore.message_broker.offline_contents.get_filename', return_value="filename")
    def testOffloadMessageException(self, mocked_filename, mocked_unique_name):
        msg = {'contents': 'the contents'}
        converter = mock.MagicMock(side_effect=IOError)

        with mock.patch('builtins.open', mock_open()):
            self.assertEqual(msg, oc.offload_message(msg, converter))

    @mock.patch('gobcore.message_broker.offline_contents.get_filename', return_value="filename")
    def testLoadMessage(self, mocked_filename):
        self.assertEqual(oc.load_message({}, converter, {}), ({}, None))

        mocked_reader = mock_open(read_data="some data")
        with mock.patch('builtins.open', mocked_reader):
            params = {"stream_contents": False}
            self.assertEqual(oc.load_message({"contents_ref": "unique_name", "any": "value"}, converter, params),
                             ({"any": "value", "contents": "converted some data"}, "unique_name"))
            mocked_reader.assert_called_once_with('filename', 'r')
            handle = mocked_reader()
            handle.read.assert_called()

    @mock.patch('gobcore.message_broker.offline_contents.get_filename', return_value="filename")
    def testLoadMessageReader(self, mocked_filename):
        mocked_reader = mock_open(read_data="some data")
        with mock.patch('builtins.open', mocked_reader):
            params = {"stream_contents": True}
            self.assertEqual(oc.load_message({"contents_ref": "unique_name", "any": "value"}, converter, params),
                             ({"any": "value", "contents": ANY, "contents_reader": ANY}, "unique_name"))

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
@mock.patch('builtins.open')
class TestContentsReader(unittest.TestCase):

    def test_init(self, mock_open):
        reader = oc.ContentsReader("filename")
        mock_open.assert_called_with("filename", "r")
        self.assertIsNotNone(reader.file)

    def test_items(self, mock_open):
        reader = oc.ContentsReader("filename")
        reader.close = mock.MagicMock()
        items = ['a', 'b', 'c', 'd']
        reader._items = items

        self.assertEqual(list(reader.items()), items)
        reader.close.assert_called_once()

    def test_close(self, mock_open):
        reader = oc.ContentsReader("filename")

        reader.close()
        mock_open.return_value.close.assert_called_once()
