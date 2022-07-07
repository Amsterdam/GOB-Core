"""
Pydantic models to fit gobmodel.json

To be extended
"""

from pydantic import BaseModel


class Schema(BaseModel):
    datasetId: str
    tableId: str
    version: str
