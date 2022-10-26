import os
import copy
from collections import UserDict

from gobcore.exceptions import GOBException
from gobcore.parse import json_to_cached_dict
from gobcore.model.metadata import FIELD
from gobcore.model.metadata import STATE_FIELDS
from gobcore.model.metadata import PRIVATE_META_FIELDS, PUBLIC_META_FIELDS, FIXED_FIELDS
from gobcore.model.pydantic import Schema
from gobcore.model.relations import get_relations, get_inverse_relations
from gobcore.model.quality import QUALITY_CATALOG, get_quality_assurances
from gobcore.model.schema import load_schema


class NotInModelException(Exception):
    pass


class NoSuchCatalogException(NotInModelException):
    pass


class NoSuchCollectionException(NotInModelException):
    pass


class GOBModel(UserDict):
    _initialised = False

    global_attributes = {
        **PRIVATE_META_FIELDS,
        **PUBLIC_META_FIELDS,
        **FIXED_FIELDS
    }

    def __new__(cls, legacy=False):
        """GOBModel (instance) singleton."""
        if cls._initialised:
            if cls.legacy_mode is not legacy:
                raise Exception("Tried to initialise model with different legacy setting")
            # Model is already initialised
        else:
            # GOBModel singleton initialisation.
            singleton = super().__new__(cls)
            cls.legacy_mode = legacy
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

            cls._initialised = True
            cls.__instance = singleton
        return cls.__instance

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
        """Initialises GOBModel.data object with all fields and helper dicts."""
        catalog_name = catalog["name"]

        for entity_name, collection in catalog['collections'].items():
            collection['name'] = entity_name

            # GOB API.
            if cls.legacy_mode:
                if 'schema' in collection and 'legacy_attributes' not in collection:
                    raise GOBException(
                        f"Expected 'legacy_attributes' to be defined for {catalog_name} {entity_name}")
                collection['attributes'] = collection.get('legacy_attributes', collection['attributes'])

            state_attributes = STATE_FIELDS if cls.has_states(catalog_name, entity_name) else {}
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
        for catalog_name, catalog in self.items():
            if catalog['abbreviation'] == catalog_abbreviation.upper():
                for collection_name, collection in catalog['collections'].items():
                    if collection['abbreviation'] == collection_abbreviation.upper():
                        return ':'.join([catalog_name, collection_name])

    def get_table_names(self):
        """Helper function to generate all table names."""
        table_names = []
        for catalog_name, catalog in self.items():
            for collection_name in catalog['collections']:
                table_names.append(self.get_table_name(catalog_name, collection_name))
        return table_names

    def get_table_name(self, catalog_name, collection_name):
        return f'{catalog_name}_{collection_name}'.lower()

    def get_table_name_from_ref(self, ref):
        """Returns the table name from a reference.

        :param ref:
        :return:
        """
        catalog, collection = self.split_ref(ref)
        return self.get_table_name(catalog, collection)

    def split_ref(self, ref) -> tuple:
        """Splits reference into tuple of (catalog_name, collection_name).

        :param ref:
        :return:
        """
        split_res = ref.split(':')

        if len(split_res) != 2 or not all(len(item) > 0 for item in split_res):
            raise GOBException(f"Invalid reference {ref}")
        return split_res

    def get_catalog_collection_names_from_ref(self, ref):
        return self.split_ref(ref)

    def get_collection_from_ref(self, ref) -> dict:
        """Returns collection ref is referring to.

        :param ref:
        :return:
        """
        catalog_name, collection_name = self.split_ref(ref)
        try:
            return self[catalog_name]['collections'][collection_name]
        except KeyError:
            return None

    def _split_table_name(self, table_name: str):
        split = [part for part in table_name.split('_') if part]

        if len(split) < 2:
            raise GOBException("Invalid table name")

        return split

    def get_catalog_from_table_name(self, table_name: str):
        """Returns catalog name from table name.

        :param table_name:
        :return:
        """
        return self._split_table_name(table_name)[0]

    def get_collection_from_table_name(self, table_name: str):
        """Returns collection name from table name.

        :param table_name:
        :return:
        """
        return "_".join(self._split_table_name(table_name)[1:])

    def get_catalog_from_abbr(self, catalog_abbr: str):
        """Returns catalog from abbreviation.

        :param catalog_abbr:
        """
        try:
            return [catalog for catalog in self.values()
                    if catalog['abbreviation'].lower() == catalog_abbr][0]
        except IndexError as exc:
            raise NoSuchCatalogException(catalog_abbr) from exc

    def get_catalog_collection_from_abbr(self, catalog_abbr: str, collection_abbr: str):
        """Returns catalog and collection.

        :param catalog_abbr:
        :param collection_abbr:
        :return:
        """
        catalog = self.get_catalog_from_abbr(catalog_abbr)

        try:
            collection = [collection for collection in catalog['collections'].values()
                          if collection['abbreviation'].lower() == collection_abbr][0]
        except IndexError as exc:
            raise NoSuchCollectionException(collection_abbr) from exc

        return catalog, collection
