"""
Pydantic models to fit gobmodel.json

To be extended
"""
from typing import Optional

from pydantic import BaseModel


class Schema(BaseModel):
    datasetId: str
    tableId: str
    version: str

    """Required when 'identifier' in Amsterdam Schema is of type list. Ignored otherwise """
    entity_id: Optional[str]
