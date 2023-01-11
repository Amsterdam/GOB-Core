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
    Base uri specification, if not set defaults to given value. (master branch)
    Should be a full url which can be appended with /datasets/..
    REPO_BASE environment variable takes precedence over base_uri.
    """
    base_uri: str = "https://raw.githubusercontent.com/Amsterdam/amsterdam-schema/master"

    """Required when 'identifier' in Amsterdam Schema is of type list. Ignored otherwise """
    entity_id: Optional[str]
