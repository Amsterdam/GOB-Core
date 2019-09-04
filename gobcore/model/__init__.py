import os
import json

from gobcore.exceptions import GOBException
from gobcore.model.metadata import FIELD
from gobcore.model.metadata import STATE_FIELDS
from gobcore.model.metadata import PRIVATE_META_FIELDS, PUBLIC_META_FIELDS, FIXED_FIELDS
from gobcore.model.relations import get_relations, get_inverse_relations
from gobcore.model.schema import load_schema, SchemaException


EVENTS_DESCRIPTION = {
    "eventid": "Unique identification of the event, numbered sequentially",
    "timestamp": "Datetime when the event as created",
    "catalogue": "The catalogue in which the entity resides",
    "entity": "The entity to which the event need to be applied",
    "version": "The version of the entity model",
    "action": "Add, change, delete or confirm",
    "source": "The functional source of the entity, e.g. AMSBI",
    "application": "The technical source of the entity, e.g. DIVA",
    "source_id": "The id of the entity in the source",
    "contents": "A json object that holds the contents for the action, the full entity for an Add"
}

EVENTS = {
    "eventid": "GOB.PKInteger",
    "timestamp": "GOB.DateTime",
    "catalogue": "GOB.String",
    "entity": "GOB.String",
    "version": "GOB.String",
    "action": "GOB.String",
    "source": "GOB.String",
    "application": "GOB.String",
    "source_id": "GOB.String",
    "contents": "GOB.JSON"
}


class GOBModel():
    inverse_relations = None
    _data = None

    def __init__(self):
        if GOBModel._data is not None:
            # Model is already initialised
            return

        path = os.path.join(os.path.dirname(__file__), 'gobmodel.json')
        with open(path) as file:
            data = json.load(file)

        if os.getenv('DISABLE_TEST_CATALOGUE', False):
            # Default is to include the test catalogue
            # By setting the DISABLE_TEST_CATALOGUE environment variable
            # the test catalogue can be removed
            del data["test_catalogue"]

        GOBModel._data = data
        self._load_schemas()
        data["rel"] = get_relations(self)

        global_attributes = {
            **PRIVATE_META_FIELDS,
            **PUBLIC_META_FIELDS,
            **FIXED_FIELDS
        }

        # Extract references for easy access in API
        for catalog_name, catalog in self._data.items():
            for entity_name, model in catalog['collections'].items():
                model['references'] = self._extract_references(model['attributes'])
                model['very_many_references'] = self._extract_very_many_references(model['attributes'])

                self._set_api(catalog_name, entity_name, model)

                model_attributes = model['attributes']
                state_attributes = STATE_FIELDS if self.has_states(catalog_name, entity_name) else {}
                all_attributes = {
                    **state_attributes,
                    **model_attributes
                }

                # Add fields to the GOBModel to be used in database creation and lookups
                model['fields'] = all_attributes

                # Include complete definition, including all global fields
                model['all_fields'] = {
                    **all_attributes,
                    **global_attributes
                }

    def _load_schemas(self):
        """
        Load any external schemas and updates model accordingly

        :return: None
        """
        for catalog_name, catalog in self._data.items():
            for entity_name, model in catalog['collections'].items():
                if model.get('schema') is not None:
                    try:
                        model['attributes'] = load_schema(model['schema'], catalog_name, entity_name)
                    except SchemaException as e:
                        # Use a fallback scenario as long as the schemas are still in development
                        print(f"ERROR: failed to load schema {model['schema']} for {catalog_name}:{entity_name}")
                        model['attributes'] = model["_attributes"]

    def _set_api(self, catalog_name, entity_name, model):
        """
        Sets the api attribute of an entity model

        Entities with state are automatically populated with a end_validity filter

        :param catalog_name: name of the catalog
        :param entity_name:  name of the entity
        :param model: the entity model
        :return: None
        """
        api = model.get('api', {})
        if not api.get('filters'):
            api['filters'] = []
        if self.has_states(catalog_name, entity_name):
            # If no filter on end validity is set, add the default filter
            if not [item for item in api['filters'] if item["field"] == FIELD.END_VALIDITY]:
                api['filters'].append({
                    "field": FIELD.END_VALIDITY,
                    "op": "is_null"
                })
        model['api'] = api

    def _extract_references(self, attributes):
        return {field_name: spec for field_name, spec in attributes.items()
                if spec['type'] in ['GOB.Reference', 'GOB.ManyReference', 'GOB.VeryManyReference']}

    def _extract_very_many_references(self, attributes):
        return {field_name: spec for field_name, spec in attributes.items()
                if spec['type'] in ['GOB.VeryManyReference']}

    def get_inverse_relations(self):
        if not self.inverse_relations:
            self.inverse_relations = get_inverse_relations(self)
        return self.inverse_relations

    def get_catalog_names(self):
        return self._data.keys()

    def get_catalogs(self):
        return self._data

    def get_catalog(self, catalog_name):
        return self._data[catalog_name] if catalog_name in self._data else None

    def get_collection_names(self, catalog_name):
        catalog = self.get_catalog(catalog_name)
        return catalog['collections'].keys() if 'collections' in catalog else None

    def get_collections(self, catalog_name):
        catalog = self.get_catalog(catalog_name)
        return catalog['collections'] if catalog and 'collections' in catalog else None

    def get_collection(self, catalog_name, collection_name):
        collections = self.get_collections(catalog_name)
        return collections[collection_name] if collections and collection_name in collections else None

    def get_collection_by_name(self, collection_name):
        """Finds collection only by name. Raises GOBException when multiple collections with name are found. Returns
        (catalog, collection) tuple when success.

        :param collection_name:
        :return:
        """
        collections = []
        catalog = None

        for catalog_name in self._data.keys():
            collection = self.get_collection(catalog_name, collection_name)

            if collection:
                collections.append(collection)
                catalog = catalog_name

        if len(collections) > 1:
            raise GOBException(f"Multiple collections found with name {collection_name}")

        return (catalog, collections[0]) if collections else None

    def get_functional_key_fields(self, catalog_name, collection_name):
        """
        Return the list of fieldnames that functionally identifies an entity

        :param catalog_name: name of the catalog
        :param collection_name: name of the collection
        :return: array of fieldnames
        """
        collection = self.get_collection(catalog_name, collection_name)
        result = [FIELD.SOURCE, collection["entity_id"]]
        if self.has_states(catalog_name, collection_name):
            result.append(FIELD.SEQNR)
        return result

    def get_technical_key_fields(self, catalog_name, collection_name):
        """
        Return the list of fieldnames that technically identifies an entity

        :param catalog_name: name of the catalog
        :param collection_name: name of the collection
        :return: array of fieldnames
        """
        return [FIELD.SOURCE, FIELD.SOURCE_ID]

    def has_states(self, catalog_name, collection_name):
        """
        Tells if a collection has states

        :param catalog_name: name of the catalog
        :param collection_name: name of the collection
        :return: True if the collection has states
        """
        collection = self.get_collection(catalog_name, collection_name)
        return collection.get("has_states") is True

    def get_source_id(self, entity, input_spec):
        """
        Gets the id that uniquely identifies the entity within the source

        :param entity: the entity
        :param input_spec: the input format specification
        :return: the source id
        """
        source_id_field = input_spec['source']['entity_id']
        source_id = str(entity[source_id_field])
        if self.has_states(input_spec['catalogue'], input_spec['entity']):
            # Source id + volgnummer is source id
            source_id = f"{source_id}.{entity[FIELD.SEQNR]}"
        return source_id

    def get_reference_by_abbreviations(self, catalog_abbreviation, collection_abbreviation):
        for catalog_name, catalog in self._data.items():
            if catalog['abbreviation'] == catalog_abbreviation.upper():
                for collection_name, collection in catalog['collections'].items():
                    if collection['abbreviation'] == collection_abbreviation.upper():
                        return ':'.join([catalog_name, collection_name])

    def get_table_names(self):
        '''
        Helper function to generate all table names
        '''
        table_names = []
        for catalog_name in self.get_catalog_names():
            for collection_name in self.get_collection_names(catalog_name):
                table_names.append(self.get_table_name(catalog_name, collection_name))
        return table_names

    def get_table_name(self, catalog_name, collection_name):
        return f'{catalog_name}_{collection_name}'.lower()

    def get_table_name_from_ref(self, ref):
        """Returns the table name from a reference

        :param ref:
        :return:
        """
        catalog, collection = self.split_ref(ref)
        return self.get_table_name(catalog, collection)

    def split_ref(self, ref) -> tuple:
        """Splits reference into tuple of (catalog_name, collection_name)

        :param ref:
        :return:
        """
        split_res = ref.split(':')

        if len(split_res) != 2 or not all([len(item) > 0 for item in split_res]):
            raise GOBException(f"Invalid reference {ref}")
        return split_res

    def get_catalog_collection_names_from_ref(self, ref):
        return self.split_ref(ref)

    def get_collection_from_ref(self, ref) -> dict:
        """Returns collection ref is referring to

        :param ref:
        :return:
        """
        catalog_name, collection_name = self.split_ref(ref)
        return self.get_collection(catalog_name, collection_name)

    def _split_table_name(self, table_name: str):
        split = [part for part in table_name.split('_') if part]

        if len(split) < 2:
            raise GOBException("Invalid table name")

        return split

    def get_catalog_from_table_name(self, table_name: str):
        """Returns catalog name from table name

        :param table_name:
        :return:
        """
        return self._split_table_name(table_name)[0]

    def get_collection_from_table_name(self, table_name: str):
        """Returns collection name from table name

        :param table_name:
        :return:
        """
        return "_".join(self._split_table_name(table_name)[1:])
