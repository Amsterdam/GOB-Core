import datetime
import json

import unittest
from unittest import mock
from gobcore.utils import get_filename
from tempfile import TemporaryDirectory, NamedTemporaryFile

from gobcore.message_broker.utils import to_json
from unittest.mock import mock_open, ANY, patch, PropertyMock, MagicMock
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
    @mock.patch('builtins.print')
    def testEndMessageRemoveFailed(self, mocked_print, mocked_remove, mocked_filename):
        mocked_remove.side_effect = Exception
        reader = mock.MagicMock()
        oc.end_message({'some': 'content', 'contents_reader': reader}, 'unique name')
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

    def testForceOffloadMessage(self):
        with TemporaryDirectory() as tmpdir:
            with mock.patch("gobcore.utils.GOB_SHARED_DIR", str(tmpdir)):
                msg = {"contents": {"test": "data"}}
                message = oc.offload_message(msg, to_json, force_offload=True)
                assert "contents_ref" in message
                filename = get_filename(message["contents_ref"], "message_broker")
                content = json.loads(Path(filename).read_text())
                assert content == {"test": "data"}

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

    @patch("gobcore.message_broker.offline_contents.get_filename", MagicMock(return_value="filename"))
    def testLoadMessageReader(self):
        mocked_reader = mock_open(read_data="some data")
        expected = ({"any": "value", "contents": ANY}, "unique_name")

        with mock.patch("builtins.open", mocked_reader):
            params = {"stream_contents": True}
            actual = oc.load_message({"contents_ref": "unique_name", "any": "value"}, converter, params)

        assert actual == expected


class TestContentsWriter(unittest.TestCase):

    def test_init(self):
        writer = oc.ContentsWriter()
        assert oc._MESSAGE_BROKER_FOLDER in writer.filename

        writer = oc.ContentsWriter("destination")
        assert "destination" in writer.filename

    @patch("gobcore.message_broker.offline_contents.os.path")
    @patch("builtins.open")
    def test_enter(self, mock_open, mock_path):
        with self.assertRaises(FileExistsError):
            mock_path.exists = lambda x: True
            oc.ContentsWriter().__enter__()

        mock_path.exists = lambda x: False
        cw = oc.ContentsWriter()
        cw.__enter__()
        mock_open.assert_called_with(cw.filename, "ab")

    @patch("os.remove")
    @patch("builtins.open")
    def test_exit(self, mock_open, mock_remove):
        with oc.ContentsWriter():
            pass

        mock_open.return_value.close.assert_called()
        mock_remove.assert_not_called()

        mock_open.reset_mock()

        with self.assertRaises(Exception):
            with oc.ContentsWriter() as cw:
                raise Exception

        mock_open.return_value.close.assert_called()
        mock_remove.assert_called_with(cw.filename)

    def test_write(self):
        entity = {
            "entity": "my value",
            "datetime": datetime.datetime(2023, 1, 5),
            "date": datetime.date(2023, 1, 5),
            "int": 2,
            "list": [1, 2]
        }
        expected = b'{"entity":"my value","datetime":"2023-01-05T00:00:00.000000",' \
                   b'"date":"2023-01-05","int":2,"list":[1,2]}\n'

        with TemporaryDirectory() as tmpdir:
            with oc.ContentsWriter(tmpdir) as cw:
                cw.write(entity)

            with open(cw.filename, mode="rb") as fp:
                actual = fp.read()

            assert actual == expected


class TestContentsReader(unittest.TestCase):

    def test_init(self):
        reader = oc.ContentsReader("filename")
        assert reader.filename == "filename"

    @patch("gobcore.message_broker.offline_contents.ContentsReader._has_contents", PropertyMock(return_value=True))
    @patch("gobcore.message_broker.offline_contents.ijson", MagicMock())
    @patch("builtins.open")
    def test_open(self, mock_open):
        with patch(
                "gobcore.message_broker.offline_contents.ContentsReader._has_contents",
                PropertyMock(return_value=True)
        ):
            list(oc.ContentsReader("filename").items())
            mock_open.assert_called_with("filename", "rb")

    def test_items(self):
        items = [{"key": "value"}, {"key2": "value2"}]

        with NamedTemporaryFile(mode="w", delete=False) as tmpfile:
            for item in items:
                tmpfile.write(json.dumps(item) + "\n")

        try:
            assert list(oc.ContentsReader(tmpfile.name).items()) == items
        finally:
            Path(tmpfile.name).unlink()

    def test_empty_file(self):
        tmpfile = NamedTemporaryFile(mode="w", delete=False)
        tmpfile.write("")

        try:
            assert list(oc.ContentsReader(tmpfile.name).items()) == []
        finally:
            Path(tmpfile.name).unlink()

    def test_file_by_contentswriter(self):
        entities = [{"key1": "value1"}, {"key2": "value2"}, {"key3": 10}]

        with TemporaryDirectory() as tmpdir:

            with oc.ContentsWriter(tmpdir) as cw:
                for entity in entities:
                    cw.write(entity)

            actual = list(oc.ContentsReader(cw.filename).items())
            assert actual == entities
