from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class XComDataStore:

    def __init__(self, xcom_path: Path = Path("/airflow/xcom/return.json")) -> None:
        """Before inserting data in the xcom json, validate it.

        :param xcom_path: path to write xcom data to.
        """
        self.xcom_path = xcom_path
        self.xcom_path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, data: dict[str, Any]) -> None:
        """Write xcom data to let follow-up tasks know where to find the results.

        :param data: content to be put in the xcom data file.
        """
        with self.xcom_path.open("w") as fp:
            json.dump(data, fp=fp)

    def parse(self, json_str: str) -> dict[str, Any]:
        """Parse xcom data from application arguments.

        :param json_str: json with data.
        :return: an XComData object.
        """
        return json.loads(json_str)
