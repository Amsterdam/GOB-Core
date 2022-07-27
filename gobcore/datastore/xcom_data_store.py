import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel


class XComData(BaseModel):
    """Holds xcom data, which is used in various tasks."""
    # Points to file in Azure storage container
    contents_ref: str


class XComDataStore:
    xcom_dataclass = XComData

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
        data = self.xcom_dataclass(**data)
        with self.xcom_path.open("w") as fp:
            json.dump(data.dict(), fp=fp)

    def parse(self, json_str: str) -> XComData:
        """Parse xcom data from application arguments.

        :param json_str: json with data for XComData model.
        :return: an XComData object.
        """
        return self.xcom_dataclass(**json.loads(json_str))
