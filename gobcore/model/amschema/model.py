from abc import ABC, abstractmethod
from enum import Enum
from typing import Literal, Optional, Union

from pydantic import BaseModel, conlist
from pydantic.fields import Field
from pydash import snake_case


class Property(ABC, BaseModel):
    description: Optional[str]
    provenance: Optional[str]
    auth: Optional[str]

    @property
    @abstractmethod
    def gob_type(self):  # pragma: no cover
        pass

    @property
    def is_secure(self):
        """Determine if type of Property instance is secure."""
        return self.auth == "BRK/RSN"

    def gob_representation(self, dataset: "Dataset"):
        if self.is_secure:
            return {
                "type": self.gob_type,
                "level": 4,
                "description": self.description or ""
            }
        return {
            "type": self.gob_type,
            "description": self.description or ""
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
            return "GOB.SecureDate" if self.is_secure else "GOB.Date"
        if self.format == StringFormatEnum.datetime:
            return "GOB.SecureDateTime" if self.is_secure else "GOB.DateTime"
        if self.maxLength == 1:
            return "GOB.Character"
        return "GOB.SecureString" if self.is_secure else "GOB.String"


class NumberProperty(Property):
    type: Literal["number"]
    multipleOf: Optional[float]

    @property
    def gob_type(self):
        return "GOB.SecureDecimal" if self.is_secure else "GOB.Decimal"


class IntegerProperty(Property):
    type: Literal["integer"]

    @property
    def gob_type(self):
        return "GOB.SecureInteger" if self.is_secure else "GOB.Integer"


class RefProperty(Property):
    """Amsterdam schema property with $ref attribute."""
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
            "srid": dataset.srid,
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
        """Map Amsterdam schema object properties to GOB properties."""
        gob_properties = {}
        for ams_prop, ams_value in self.properties.items():
            # Property with no 'auth' value inherites from object.
            if not ams_value.auth and self.auth:
                ams_value.auth = self.auth
            gob_properties[snake_case(ams_prop)] = ams_value.gob_representation(dataset)
        return gob_properties

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
    """Amsterdam schema: table schema object."""
    schema_: str = Field(alias="$schema")
    type: Literal["object"]
    additionalProperties: bool
    mainGeometry: Optional[str]
    identifier: Union[str, list[str]]
    required: list[str]
    display: str
    properties: dict[str, Properties]


class TemporalDimensions(BaseModel):
    """Amsterdam schema: table temporal dimensions."""
    geldigOp: Optional[conlist(str, min_items=2, max_items=2)]


class Temporal(BaseModel):
    """Amsterdam schema: table temporal."""
    identifier: str
    dimensions: TemporalDimensions


class Table(BaseModel):
    """Amsterdam schema: table; corresponds to GOB collection."""
    id: str
    type: Literal["table"]
    version: str
    auth: Optional[str]
    authorizationGrantor: Optional[str]
    temporal: Optional[Temporal]
    schema_: Schema = Field(alias="schema")


class TableListItem(BaseModel):
    """Table list item in Amsterdam schema dataset."""
    id: str
    ref: str = Field(alias="$ref")
    activeVersions: dict[str, str]


class Dataset(BaseModel):
    """Amsterdam schema: dataset; corresponds to GOB catalog."""
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
            raise Exception(
                f"CRS {self.crs} does not start with EPSG. Don't know what to do with this. Help me?"
            )
        return int(self.crs.replace("EPSG:", ""))
