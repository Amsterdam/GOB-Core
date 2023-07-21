"""GOB collection module."""


from collections import UserDict
from typing import Any, Literal, NewType, Optional, Union

from pydantic import BaseModel, Field, StrictBool, constr, validator

from gobcore.model.metadata import FIXED_COLUMNS, METADATA_COLUMNS, STATE_COLUMNS
from gobcore.typesystem import get_gob_type
from gobcore.typesystem.gob_types import GOBType


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


FieldKind = Literal["attribute", "state", "fixed", "private", "public", "json"]


class GOBField(BaseModel):
    """GOB Field."""

    name: str
    type: str
    description: Optional[str] = Field(repr=False)
    config: dict[str, Any] = Field(repr=False)
    kind: FieldKind
    attributes: Optional[dict[str, "GOBField"]] = Field(repr=False)

    class Config:
        """Pydantic config."""

        allow_mutation = False
        extra = "forbid"

    @validator("attributes", pre=True)
    def json_attributes(
        cls, json_attrs: dict[str, dict[str, str]], values: dict[str, Any]
    ) -> Optional[dict[str, dict[str, Union[dict[str, str], str, None]]]]:
        """Initialise GOBField JSON attributes dictionary."""
        if json_attrs:
            if values["type"] not in ["GOB.JSON", "GOB.IncompleteDate"]:
                raise TypeError(f"Invalid GOB type {values['type']}; expected 'GOB.JSON' or 'GOB.IncompleteDate'")
            attributes = {}
            for key, value in json_attrs.items():
                attributes[key] = {
                    "name": key,
                    "type": value["type"],
                    "description": value.get("description"),
                    "config": {k: v for k, v in value.items() if not k in ["type", "description"]},
                    "kind": "attribute",
                }
            return attributes
        return None


    @property
    def gob_type(self) -> GOBType:
        """Return the GOBType of GOBField."""
        return get_gob_type(self.type)


class CollectionBase(BaseModel):
    """GOB Collection Base."""

    data: dict[str, Any] = Field(repr=False)
    name: str
    catalog_name: str
    entity_id: str
    has_states: StrictBool
    all_fields: dict[str, GOBField] = Field(repr=False)
    fields: dict[str, GOBField] = Field(repr=False)
    attributes: dict[str, GOBField] = Field(repr=False)
    references: dict[str, GOBField] = Field(repr=False)
    very_many_references: dict[str, GOBField] = Field(repr=False)
    abbreviation: constr(to_upper=True)  # type: ignore[valid-type]
    version: str
    description: str = Field(repr=False)
    schema_: Optional[CollectionSchema] = Field(alias="schema", repr=False)
    api: Optional[dict[str, Any]] = Field(repr=False)

    class Config:
        """Pydantic config."""

        allow_mutation = False
        extra = "forbid"

    @validator("all_fields", pre=True)
    def fill_all_fields(cls, dict_fields: dict[str, dict[str, Any]]) -> dict[str, dict[str, Optional[Any]]]:
        """Initialise CollectionBase all_fields dictionary."""
        all_fields = {}
        for key, value in dict_fields.items():
            json_attrs = value.get("attributes")
            if json_attrs:
                kind = "json"
            elif key in FIXED_COLUMNS:
                kind = "fixed"
            elif key in STATE_COLUMNS:
                kind = "state"
            elif key in METADATA_COLUMNS["private"]:
                kind = "private"
            elif key in METADATA_COLUMNS["public"]:
                kind = "public"
            else:
                kind = "attribute"
            all_fields[key] = {
                "name": key,
                "type": value["type"],
                "description": value.get("description"),
                "config": {k: v for k, v in value.items() if not k in ["type", "description", "attributes"]},
                "kind": kind,
                "attributes": json_attrs,
            }
        return all_fields

    @validator("fields", "attributes", "references", "very_many_references", pre=True)
    def fill_fields(cls, dict_fields: dict[str, dict[str, Any]], values) -> dict[str, dict[str, Optional[Any]]]:
        """Initialise CollectionBase fields and attributes dictionary."""
        fields = {}
        # Make sure all fields are valid
        if "all_fields" in values:
            for key in dict_fields:
                fields[key] = values["all_fields"][key]
        return fields

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
            all_fields=collection["all_fields"],
            fields=collection["fields"],
            attributes=collection["attributes"],
            references=collection["references"],
            very_many_references=collection["very_many_references"],
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
        """Tell if collection is a relation collection."""
        return self.catalog_name == "rel"

    def matches_abbreviation(self, abbr: str) -> bool:
        """Return True if uppercased `abbr' matches collection abbreviation."""
        return abbr.upper() == self.abbreviation
