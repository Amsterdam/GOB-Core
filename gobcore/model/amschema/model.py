from abc import ABC, abstractmethod
from decimal import Decimal
from enum import Enum
from typing import Literal, Optional, Union

from pydantic import BaseModel, conlist
from pydantic.fields import Field
from pydash import snake_case


class Property(ABC, BaseModel):
    description: Optional[str]
    provenance: Optional[str]
    shortname: Optional[str]

    @property
    @abstractmethod
    def gob_type(self) -> str:
        pass

    def gob_representation(self, dataset: "Dataset"):
        return {
            "type": self.gob_type,
            "description": self.description or "",
            **({"shortname": snake_case(self.shortname)} if self.shortname else {}),
        }


class StringFormatEnum(str, Enum):
    datetime = "date-time"
    date = "date"


class StringProperty(Property):
    type: Literal["string"]
    format: Optional[StringFormatEnum]
    minLength: Optional[int]
    maxLength: Optional[int]

    @property
    def gob_type(self):
        if self.format == StringFormatEnum.date:
            return "GOB.Date"
        elif self.format == StringFormatEnum.datetime:
            return "GOB.DateTime"
        elif self.maxLength == 1:
            return "GOB.Character"

        return "GOB.String"


class NumberProperty(Property):
    type: Literal["number"]
    multipleOf: Optional[float]

    @property
    def gob_type(self):
        return "GOB.Decimal"

    def gob_representation(self, dataset: "Dataset"):
        """Return the GOB representation of NumberProperty."""
        if self.multipleOf:
            _, _, exponent = Decimal(str(self.multipleOf)).as_tuple()
            return {
                **super().gob_representation(dataset),
                "precision": -exponent
            }
        return super().gob_representation(dataset)


class IntegerProperty(Property):
    type: Literal["integer"]

    @property
    def gob_type(self):
        return "GOB.Integer"


class RefProperty(Property):
    ref: str = Field(alias="$ref")

    refs_to_gob = {
        "https://geojson.org/schema/Point.json": "GOB.Geo.Point",
        "https://geojson.org/schema/Polygon.json": "GOB.Geo.Polygon",
        "https://geojson.org/schema/LineString.json": "GOB.Geo.Geometry",
        "https://geojson.org/schema/Geometry.json": "GOB.Geo.Geometry",
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

    def mapped_properties(self, dataset: "Dataset"):
        return {snake_case(k): v.gob_representation(dataset) for k, v in self.properties.items()}

    def gob_representation(self, dataset: "Dataset"):

        if self._is_relation():
            type_attrs = {
                "ref": self.relation
            }

        else:
            type_attrs = {
                "attributes": self.mapped_properties(dataset)
            }

        return {
            **super().gob_representation(dataset),
            **type_attrs,
        }


class ArrayProperty(Property):
    type: Literal["array"]
    items: Union[NonObjectProperties, ObjectProperty]
    relation: Optional[str]

    def _is_relation(self):
        return not not self.relation

    @property
    def gob_type(self):
        return "GOB.ManyReference" if self._is_relation() else "GOB.JSON"

    def gob_representation(self, dataset: "Dataset"):
        if self._is_relation():
            type_attrs = {
                "ref": self.relation
            }
        elif isinstance(self.items, ObjectProperty):
            type_attrs = {
                "has_multiple_values": True,
                "attributes": self.items.mapped_properties(dataset)
            }
        else:
            raise NotImplementedError()

        return {
            **super().gob_representation(dataset),
            **type_attrs,
        }


Properties = Union[NonObjectProperties, ObjectProperty, ArrayProperty]


class Schema(BaseModel):
    schema_: str = Field(alias="$schema")
    type: Literal["object"]
    additionalProperties: bool
    mainGeometry: Optional[str]
    identifier: Union[str, list[str]]
    required: list[str]
    display: str
    properties: dict[str, Properties]


class TemporalDimensions(BaseModel):
    geldigOp: Optional[conlist(str, min_items=2, max_items=2)]


class Temporal(BaseModel):
    identifier: str
    dimensions: TemporalDimensions


class Table(BaseModel):
    id: str
    type: Literal["table"]
    version: str
    schema_: Schema = Field(alias="schema")
    temporal: Optional[Temporal]


class TableListItem(BaseModel):
    id: str
    ref: str = Field(alias="$ref")
    activeVersions: dict[str, str]


class Publisher(BaseModel):
    ref: str = Field(alias="$ref", regex="^publishers/[A-Z]+$")


class Dataset(BaseModel):
    type: Literal["dataset"]
    id: str
    title: str
    status: str
    version: str
    # Coördinaten in het stelsel van de Rijksdriehoeksmeting (RD)
    crs: str = "EPSG:28992"
    owner: str
    creator: str
    publisher: Union[str, Publisher]
    auth: str
    authorizationGrantor: str
    tables: list[TableListItem]

    @property
    def srid(self) -> int:
        if not self.crs.startswith("EPSG:"):
            raise Exception(f"CRS {self.crs} does not start with EPSG. Don't know what to do with this. Help me?")
        return int(self.crs.replace("EPSG:", ""))
