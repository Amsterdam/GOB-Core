"""GOB Events

Each possible event in GOB is defined in this module.
The definition includes:
    name - the name of the event, e.g. ADD
    timestamp - the name of the timestamp attribute in the GOB entity that tells when this event has last been applied
    get_modification - method to

todo: The delete and confirm actions contain too much data. Contents can be left empty
    A deletion or confirmation simple specifies source and sourceid in a collection
    See also the examples in the action classes

"""
from abc import ABCMeta, abstractmethod

from gobcore.exceptions import GOBException
from gobcore.model import GOBModel
from gobcore.typesystem import get_gob_type

hash_key = '_hash'
modifications_key = 'modifications'


class ImportEvent(metaclass=ABCMeta):
    name = "event"
    is_add_new = False
    timestamp_field = None  # Each event is timestamped
    gob_model = GOBModel()
    skip = ["_entity_source_id", "_last_event"]

    @classmethod
    @abstractmethod
    def create_event(cls, _source_id, _entity_source_id, data):
        """Creates the event dict for the given parameters

        :param _source_id: the source id of the data this event is based on
        :param _id_column: the key or attribute which holds the id
        :param data: the data for the event

        :return: a dict representing the event
        """
        data["_source_id"] = _source_id
        data["_entity_source_id"] = _entity_source_id

        return {"event": cls.name, "data": data}

    @classmethod
    def last_event(cls, data):
        return {"_last_event": data.get("_last_event")}

    def __init__(self, data, metadata):
        self._data = data
        self._metadata = metadata
        self.last_event = self._data.pop("_last_event", None)

        self._model = self.gob_model.get_collection(self._metadata.catalogue, self._metadata.entity)

    def apply_to(self, entity):
        """Sets the attributes in data on the entity (expands `data['mutations'] first)

        :param entity: the instance to be modified
        :param metadata: the metadata of the import message
        :return:
        """
        # Register the time that the event has been applied to the entity
        setattr(entity, self.timestamp_field, self._metadata.timestamp)
        # Register the application that delivered the event
        entity._application = self._metadata.application

        for key, value in self._data.items():
            if key not in self.skip:
                gob_type = get_gob_type(self._model['all_fields'][key]['type'])
                setattr(entity, key, gob_type.from_value(value).to_db)

    def get_attribute_dict(self):
        """Gets an dict with attributes to insert entities in bulk

        :return:
        """
        entity = {
            self.timestamp_field: self._metadata.timestamp,
            '_application': self._metadata.application
        }

        for key, value in self._data.items():
            if key not in self.skip:
                gob_type = get_gob_type(self._model['all_fields'][key]['type'])
                entity[key] = gob_type.from_value(value).to_db
        return entity


class ADD(ImportEvent):
    """
    Example:
    {
        ADD
        entity: meetbout
        source: meetboutengis
        source_id: 12881429
        data: {
            meetboutid: "12881429",
            indicatie_beveiligd: True,
            ....
        }
    }
    """
    name = "ADD"
    is_add_new = True
    timestamp_field = "_date_created"

    def apply_to(self, entity):
        # Clear the _date_deleted field to re-enable deleted records
        setattr(entity, '_date_deleted', None)

        # The data for the add event is in the entity attribute
        self._data = self._data["entity"]

        super().apply_to(entity)

    @classmethod
    def create_event(cls, _source_id, _entity_source_id, data):
        #   ADD has no modifications, only data
        if modifications_key in data:
            data.pop(modifications_key)

        event_data = {
            "entity": data,
            **(cls.last_event(data))
        }

        return super().create_event(_source_id, _entity_source_id, event_data)

    def get_attribute_dict(self):
        # The data for the add event is in the entity attribute
        self._data = self._data["entity"]

        return super().get_attribute_dict()


class MODIFY(ImportEvent):
    """
    Example:
    {
        MODIFY
        entity: meetbouten
        source: meetboutengis
        source_id: 12881429
        data: {
            modifications: [{
                key: "indicatie_beveiligd",
                new_value: False
                old_value: True,
            }]
        }
    }
    """
    name = "MODIFY"
    timestamp_field = "_date_modified"

    def apply_to(self, entity):
        # Set the hash
        entity._hash = self._data[hash_key]

        # extract modifications from the data, before applying the event to the entity
        modifications = self._data.pop(modifications_key)
        attribute_set = self._extract_modifications(entity, modifications)
        self._data = {**self._data, **attribute_set}

        super().apply_to(entity)

    def _extract_modifications(self, entity, modifications):
        """extracts attributes to modify, and checks if old values are indeed present on entity

        :param entity: the instance to be modified
        :param modifications: a collection of mutations of attributes to be interpretated

        :return: a dict with extracted and verified mutations
        """
        modified_attributes = {}

        for mutation in modifications:
            # It might be tempting to compare the current value with the expected value (old_value in the event)
            # But: the current value is a database type and the expected value is a json type
            modified_attributes[mutation['key']] = mutation['new_value']
        return modified_attributes

    @classmethod
    def create_event(cls, _source_id, _entity_source_id, data):
        #   MODIFY has no data attributes only modifications
        if modifications_key not in data:
            raise GOBException("MODIFY event requires modifications")
        mods = {
            modifications_key: data[modifications_key],
            hash_key: data[hash_key],
            **(cls.last_event(data))
        }

        return super().create_event(_source_id, _entity_source_id, mods)


class DELETE(ImportEvent):
    """
    Example:
    {
        DELETE
        entity: meetbouten
        source: meetboutengis
        source_id: 12881429
        data: {}
    }
    """
    name = "DELETE"
    timestamp_field = "_date_deleted"

    @classmethod
    def create_event(cls, _source_id, _entity_source_id, data):
        #  DELETE has no data, except reference to entity age
        return super().create_event(_source_id, _entity_source_id, cls.last_event(data))


class CONFIRM(ImportEvent):
    """
    Example:
    {
        CONFIRM
        entity: meetbouten
        source: meetboutengis
        source_id: 12881429
        data: {}
    }
    """
    name = "CONFIRM"
    timestamp_field = "_date_confirmed"

    @classmethod
    def create_event(cls, _source_id, _entity_source_id, data):
        #  CONFIRM has no data, except reference to entity age
        return super().create_event(_source_id, _entity_source_id, cls.last_event(data))


class BULKCONFIRM(ImportEvent):
    """
    Example:
    {
        BULKCONFIRM
        entity: meetbouten
        source: meetboutengis
        source_id: None
        data: {
            confirms: [
                {'source_id': 12881429, 'last_event': 1234},
                {'source_id': 12881430, 'last_event': 1235},
                ...
            ],
            _source_id: None
        }
    }
    """
    name = "BULKCONFIRM"
    timestamp_field = "_date_confirmed"

    @classmethod
    def create_event(cls, confirms):
        #  BULKCONFIRM has a list of dicts with source_id and last_event
        data = {
            'confirms': confirms,
            '_source_id': None,
            '_entity_source_id': None
        }
        return {"event": cls.name, "data": data}
