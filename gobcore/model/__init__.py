import copy
import os
from collections import UserDict
from typing import Any, Optional

from pydantic import BaseModel, Field, constr

from gobcore.exceptions import GOBException
from gobcore.model.collection import GOBCollection
from gobcore.model.metadata import FIELD, FIXED_FIELDS, PRIVATE_META_FIELDS, PUBLIC_META_FIELDS, STATE_FIELDS
from gobcore.model.pydantic import Schema
from gobcore.model.quality import QUALITY_CATALOG, get_quality_assurances
from gobcore.model.relations import get_inverse_relations, get_relations
from gobcore.model.schema import load_schema
from gobcore.parse import json_to_cached_dict


class NotInModelException(Exception):
    pass


class NoSuchCatalogException(NotInModelException):
    pass


class NoSuchCollectionException(NotInModelException):
    pass


class CatalogBase(BaseModel):
    """GOB Catalog Base."""

    data: dict[str, Any] = Field(repr=False)
    name: str
    abbreviation: constr(to_upper=True)
    version: str
    description: str = Field(repr=False)
    collections: dict[str, GOBCollection] = Field(repr=False)

    class Config:
        """Pydantic config."""
        allow_mutation = False
        extra = "forbid"


class GOBCatalog(CatalogBase, UserDict[str, Any]):
    """GOB Catalog class."""

    def __init__(self, catalog_name, catalog) -> None:
        """Initialise GOBCatalog."""
        super().__init__(
            data=catalog,
            name=catalog_name,
            abbreviation=catalog["abbreviation"],
            version=catalog["version"],
            description=catalog["description"],
            collections=catalog["collections"],
        )

    def matches_abbreviation(self, abbr: str) -> bool:
        """Return True if uppercased `abbr' matches catalog abbreviation."""
        return abbr.upper() == self.abbreviation

    def get_collection_from_abbr(self, collection_abbr) -> Optional[GOBCollection]:
        """Return collection by collection abbreviation."""
        for collection in self.collections.values():
            if collection.matches_abbreviation(collection_abbr):
                return collection
        return None

    def get_reference_by_abbr(self, collection_abbr) -> Optional[str]:
        """Return catalog_name:collection_name reference by collection abbreviation."""
        if collection := self.get_collection_from_abbr(collection_abbr):
            return collection.reference
        return None


def upgrade_to_gob_classes(model: 'GOBModel') -> None:
    """Upgrade catalog and collections dictionaries to GOB classes."""
    for catalog_name, catalog in model.items():
        catalog_collections = {}
        for collection_name, collection in catalog["collections"].items():
            catalog_collections[collection_name] = GOBCollection(collection_name, collection, catalog_name)
        catalog["collections"] = catalog_collections
        model[catalog_name] = GOBCatalog(catalog_name, catalog)


class GOBModel(UserDict):
    _initialised = False

    global_attributes = {
        **PRIVATE_META_FIELDS,
        **PUBLIC_META_FIELDS,
        **FIXED_FIELDS
    }

    def __new__(cls, legacy: bool = False):
        """GOBModel (instance) singleton."""
        if cls._initialised:
            if cls.legacy_mode is not legacy:
                raise Exception("Tried to initialise model with different legacy setting")
            # Model is already initialised
        else:
            # GOBModel singleton initialisation.
            singleton = super().__new__(cls)
            cls.legacy_mode: bool = legacy
            cls.inverse_relations = None

            # Set and used to cache SQLAlchemy models by the SA layer.
            # Use model.sa.gob.get_sqlalchemy_models() to retrieve/init.
            cls.sqlalchemy_models = None

            # UserDict (GOBModel classmethod).
            super().__init__(cls)

            cached_data = json_to_cached_dict(os.path.join(os.path.dirname(__file__), 'gobmodel.json'))
            # Initialise GOBModel.data (leave cached_data untouched).
            cls.data = copy.deepcopy(cached_data)

            if os.getenv('DISABLE_TEST_CATALOGUE'):
                # Default is to include the test catalogue.
                # By setting the DISABLE_TEST_CATALOGUE environment variable
                # the test catalogue can be removed.
                del cls.data["test_catalogue"]

            # Proces GOBModel.data.
            cls._load_schemas(cls.data)
            cls._init_data(cls.data)

            # Upgrade dictionaries to GOB classes.
            upgrade_to_gob_classes(singleton)

            cls._initialised = True
            cls.__instance = singleton
        return cls.__instance

    # Match __new__ parameters.
    @classmethod
    def __init__(cls, legacy=False):
        pass

    @classmethod
    def _init_data(cls, data):
        """Extract references for easy access.

        Add catalog and collection names to catalog and collection objects.
        """
        for catalog_name, catalog in data.items():
            catalog['name'] = catalog_name
            cls._init_catalog(catalog)

        # This needs to happen after initialisation of the object catalogs
        data[QUALITY_CATALOG] = get_quality_assurances(data)
        data[QUALITY_CATALOG]['name'] = QUALITY_CATALOG
        data["rel"] = get_relations(cls)
        data["rel"]["name"] = "rel"

        cls._init_catalog(data[QUALITY_CATALOG])
        cls._init_catalog(data["rel"])

    @classmethod
    def _init_catalog(cls, catalog):
        """Initialise GOBModel.data object with all fields and helper dicts."""
        for entity_name, collection in catalog['collections'].items():
            collection['name'] = entity_name

            # GOB API.
            if cls.legacy_mode:
                if 'schema' in collection and 'legacy_attributes' in collection:
                    collection['attributes'] = collection.get(
                        'legacy_attributes', collection['attributes'])

            state_attributes = STATE_FIELDS if (collection.get("has_states") is True) else {}
            all_attributes = {
                **state_attributes,
                **collection['attributes']
            }

            collection['references'] = cls._extract_references(collection['attributes'])
            collection['very_many_references'] = cls._extract_very_many_references(collection['attributes'])

            # Add fields to the GOBModel to be used in database creation and lookups
            collection['fields'] = all_attributes

            # Include complete definition, including all global fields
            collection['all_fields'] = {
                **all_attributes,
                **cls.global_attributes
            }

    @staticmethod
    def _load_schemas(data):
        """Load any external schemas and updates catalog model accordingly.

        :return: None
        """
        for catalog in data.values():
            for model in catalog['collections'].values():
                if model.get('schema') is not None:
                    schema = Schema.parse_obj(model.get("schema"))
                    model.update(load_schema(schema))

    @staticmethod
    def _extract_references(attributes):
        return {field_name: spec for field_name, spec in attributes.items()
                if spec['type'] in ['GOB.Reference', 'GOB.ManyReference', 'GOB.VeryManyReference']}

    @staticmethod
    def _extract_very_many_references(attributes):
        return {field_name: spec for field_name, spec in attributes.items()
                if spec['type'] in ['GOB.VeryManyReference']}

    def get_inverse_relations(self):
        if not self.inverse_relations:
            self.inverse_relations = get_inverse_relations(self)
        return self.inverse_relations

    @classmethod
    def has_states(cls, catalog_name, collection_name):
        """Tells if a collection has states.

        :param catalog_name: name of the catalog
        :param collection_name: name of the collection
        :return: True if the collection has states
        """
        try:
            collection = cls.data[catalog_name]['collections'][collection_name]
            return collection.get("has_states") is True
        except KeyError:
            return False

    def get_source_id(self, entity, input_spec):
        """Gets the id that uniquely identifies the entity within the source.

        :param entity: the entity
        :param input_spec: the input format specification
        :return: the source id
        """
        source_id_field = input_spec['source']['entity_id']
        source_id = str(entity[source_id_field])
        if self.has_states(input_spec['catalogue'], input_spec['entity']):
            # Volgnummer could be a different field in the source entity than FIELD.SEQNR
            try:
                seqnr_field = input_spec['gob_mapping'][FIELD.SEQNR]['source_mapping']
            except KeyError:
                seqnr_field = FIELD.SEQNR

            # Source id + volgnummer is source id
            source_id = f"{source_id}.{entity[seqnr_field]}"
        return source_id

    def get_reference_by_abbreviations(self, catalog_abbreviation, collection_abbreviation):
        """Return catalog_name:collection_name reference by abbreviations."""
        for catalog in self.values():
            if catalog.matches_abbreviation(catalog_abbreviation):
                return catalog.get_reference_by_abbr(collection_abbreviation)

    def get_table_names(self):
        """Helper function to generate all table names."""
        table_names = []
        for catalog in self.values():
            for collection in catalog.collections.values():
                table_names.append(collection.table_name)
        return table_names

    @staticmethod
    def get_table_name(catalog_name, collection_name) -> str:
        """See collection.table_name."""
        return f'{catalog_name}_{collection_name}'.lower()

    def get_table_name_from_ref(self, ref):
        """Returns the table name from a reference.

        :param ref:
        :return:
        """
        catalog, collection = self.split_ref(ref)
        return self.get_table_name(catalog, collection)

    @staticmethod
    def split_ref(ref: str) -> tuple[str, str]:
        """Splits reference into tuple of (catalog_name, collection_name).

        :param ref:
        :return:
        """
        split_res = ref.split(':')

        if len(split_res) != 2 or not all(len(item) > 0 for item in split_res):
            raise GOBException(f"Invalid reference {ref}")
        return split_res

    def get_catalog_collection_names_from_ref(self, ref):
        """Use self.split_ref() -- used only by GOB-API."""
        return self.split_ref(ref)

    def get_collection_from_ref(self, ref: str) -> Optional[GOBCollection]:
        """Return collection ref is referring to.

        :param ref:
        :return:
        """
        catalog_name, collection_name = self.split_ref(ref)
        try:
            return self[catalog_name].collections[collection_name]
        except KeyError:
            return None

    @staticmethod
    def _split_table_name(table_name: str):
        split = [part for part in table_name.split('_') if part]

        if len(split) < 2:
            raise GOBException("Invalid table name")

        return split

    def get_catalog_from_table_name(self, table_name: str) -> str:
        """Returns catalog name from table name -- used only by GOB-API.

        :param table_name:
        :return:
        """
        return self._split_table_name(table_name)[0]

    def get_collection_from_table_name(self, table_name: str) -> str:
        """Return collection name from table name -- used only by GOB-API.

        :param table_name:
        :return:
        """
        return "_".join(self._split_table_name(table_name)[1:])

    def get_catalog_from_abbr(self, catalog_abbr: str) -> GOBCatalog:
        """Returns catalog from abbreviation.

        :param catalog_abbr:
        """
        try:
            return [catalog for catalog in self.values() if catalog["abbreviation"].lower() == catalog_abbr][0]
        except IndexError as exc:
            raise NoSuchCatalogException(catalog_abbr) from exc

    def get_catalog_collection_from_abbr(
        self, catalog_abbr: str, collection_abbr: str
    ) -> tuple[GOBCatalog, GOBCollection]:
        """Returns catalog and collection.

        :param catalog_abbr:
        :param collection_abbr:
        :return:
        """
        catalog = self.get_catalog_from_abbr(catalog_abbr)
        collection = catalog.get_collection_from_abbr(collection_abbr)
        if collection is None:
            raise NoSuchCollectionException(f"{collection_abbr} ({catalog_abbr})")
        return catalog, collection
