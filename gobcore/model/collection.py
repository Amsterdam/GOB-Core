"""GOB collection module."""


from collections import UserDict
from typing import Any, Optional

from pydantic import BaseModel, Field, constr, validator


class CollectionSchema(BaseModel):
    """GOB Collection Schema."""

    table_id: str
    dataset_id: str
    entity_id: str
    version: str

    class Config:
        """Pydantic config."""

        allow_mutation = False
        extra = "forbid"


class CollectionBase(BaseModel):
    """GOB Collection Base."""

    data: dict[str, Any] = Field(repr=False)
    name: str
    catalog_name: str
    entity_id: str
    has_states: bool
    attributes: dict[str, Any] = Field(repr=False)
    abbreviation: constr(to_upper=True)
    version: str
    description: str = Field(repr=False)
    schema_: Optional[CollectionSchema] = Field(alias="schema", repr=False)
    api: Optional[dict[str, Any]] = Field(repr=False)

    class Config:
        """Pydantic config."""

        allow_mutation = False
        extra = "forbid"

    @validator("schema_", pre=True)
    def collection_schema(cls, schema: Optional[dict[str, str]]) -> Optional[dict[str, str]]:
        """Initialise schema dictionary."""
        if schema:
            return {
                "table_id": schema["tableId"],
                "dataset_id": schema["datasetId"],
                "entity_id": schema.get("entity_id", "identificatie"),
                "version": schema["version"],
            }
        return None


class GOBCollection(CollectionBase, UserDict[str, Any]):
    """GOB Collection."""

    def __init__(self, collection_name, collection, catalog_name) -> None:
        """Initialise GOBCollection."""
        super().__init__(
            data=collection,
            name=collection_name,
            catalog_name=catalog_name,
            entity_id=collection["entity_id"],
            has_states=collection.get("has_states") is True,
            abbreviation=collection["abbreviation"],
            version=collection["version"],
            attributes=collection["attributes"],
            description=collection.get("description", ""),
            schema=collection.get("schema"),
            api=collection.get("api"),
        )

    @property
    def reference(self) -> str:
        """Return catalog_name:collection_name reference."""
        return f"{self.catalog_name}:{self.name}"

    @property
    def table_name(self) -> str:
        """Return collection table name."""
        return f"{self.catalog_name}_{self.name}".lower()

    @property
    def is_relation(self) -> bool:
        """Tell if I'm the relation collection."""
        return self.catalog_name == "rel"

    def matches_abbreviation(self, abbr: str) -> bool:
        """Return True if uppercased `abbr' matches collection abbreviation."""
        return abbr.upper() == self.abbreviation
