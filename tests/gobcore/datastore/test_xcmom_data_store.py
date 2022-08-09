import json

import pytest

from gobcore.datastore.xcom_data_store import XComDataStore


class TestXComDataStore:

    @pytest.fixture
    def tmp_file(self, tmp_path):
        return tmp_path / "return.json"

    def test_write_data_to_file(self, tmp_file):
        XComDataStore(xcom_path=tmp_file).write({"contents_ref": "/path/to/data.json"})
        with tmp_file.open("r") as fp:
            data = json.load(fp)

        assert data["contents_ref"] == "/path/to/data.json"

    def test_read_from_arguments(self, tmp_file):
        xcom_data = XComDataStore(xcom_path=tmp_file).parse(
            json.dumps({"contents_ref": "/path/to/data.json"}))

        assert xcom_data["contents_ref"] == "/path/to/data.json"
