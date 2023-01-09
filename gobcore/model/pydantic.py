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

    """
    Optional base_uri specification for this collection, takes precedence over REPO_BASE.
    Should be a full url which can be appended with /datasets/..
    """
    base_uri: Optional[str]

    """Required when 'identifier' in Amsterdam Schema is of type list. Ignored otherwise """
    entity_id: Optional[str]
