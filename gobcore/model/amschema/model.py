from abc import ABC, abstractmethod
from enum import Enum
from typing import Literal, Optional, Union

from pydantic import BaseModel
from pydantic.fields import Field
from pydash import snake_case


class Property(ABC, BaseModel):
    description: Optional[str]
    provenance: Optional[str]

    @property
    @abstractmethod
    def gob_type(self):  # pragma: no cover
        pass

    def gob_representation(self, dataset: "Dataset"):
        return {
            "type": self.gob_type,
            **({"description": self.description} if self.description else {}),
        }


class StringFormatEnum(str, Enum):
    datetime = "date-time"
    date = "date"


class StringProperty(Property):
    type: Literal["string"]
    format: Optional[StringFormatEnum]

    @property
    def gob_type(self):
        return "GOB.String"


class NumberProperty(Property):
    type: Literal["number"]
    multipleOf: Optional[float]

    @property
    def gob_type(self):
        if self.multipleOf:
            return "GOB.Decimal"
        raise NotImplementedError()


class IntegerProperty(Property):
    type: Literal["integer"]

    @property
    def gob_type(self):
        return "GOB.Integer"


class RefProperty(Property):
    ref: str = Field(alias="$ref")

    refs_to_gob = {
        "https://geojson.org/schema/Point.json": "GOB.Geo.Point"
    }

    @property
    def gob_type(self):
        if self.ref in self.refs_to_gob:
            return self.refs_to_gob[self.ref]
        raise NotImplementedError(f"gob_type not implemented for {self.__class__} with ref {self.ref}")

    def gob_representation(self, dataset: "Dataset"):
        return {
            **super().gob_representation(dataset),
            "srid": dataset.srid
        }


class BooleanProperty(Property):
    type: Literal["boolean"]

    @property
    def gob_type(self):
        return "GOB.Boolean"


NonObjectProperties = Union[StringProperty, NumberProperty, IntegerProperty, RefProperty, BooleanProperty]


class ObjectProperty(Property):
    type: Literal["object"]
    properties: dict[str, NonObjectProperties]
    relation: Optional[str]

    def _is_relation(self):
        return not not self.relation

    @property
    def gob_type(self):
        return "GOB.Reference" if self._is_relation() else "GOB.JSON"

    def gob_representation(self, dataset: "Dataset"):

        if self._is_relation():
            type_attrs = {
                "ref": self.relation
            }

        else:
            attributes = {snake_case(k): v.gob_representation(dataset) for k, v in self.properties.items()}
            type_attrs = {
                "attributes": attributes
            }

        return {
            **super().gob_representation(dataset),
            **type_attrs,
        }


Properties = Union[NonObjectProperties, ObjectProperty]


class Schema(BaseModel):
    schema_: str = Field(alias="$schema")
    type: Literal["object"]
    additionalProperties: bool
    mainGeometry: str
    identifier: str
    required: list[str]
    display: str
    properties: dict[str, Properties]


class Table(BaseModel):
    id: str
    type: Literal["table"]
    version: str
    schema_: Schema = Field(alias="schema")


class TableListItem(BaseModel):
    id: str
    ref: str = Field(alias="$ref")
    activeVersions: dict[str, str]


class Dataset(BaseModel):
    type: Literal["dataset"]
    id: str
    title: str
    status: str
    version: str
    crs: str
    owner: str
    creator: str
    publisher: str
    auth: str
    authorizationGrantor: str
    tables: list[TableListItem]

    @property
    def srid(self) -> int:
        if not self.crs.startswith("EPSG:"):
            raise Exception(f"CRS {self.crs} does not start with EPSG. Don't know what to do with this. Help me?")
        return int(self.crs.replace("EPSG:", ""))
