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

modifications_key = 'modifications'


class ImportEvent(metaclass=ABCMeta):
    name = "event"
    is_add_new = False
    timestamp_field = None  # Each event is timestamped

    @classmethod
    @abstractmethod
    def create_event(cls, _source_id, _id_column, _entity_id, data):
        """Creates the event dict for the given parameters

        :param entity: the entity to create the event against
        :param metadata: the metadata for the import event
        :param data: the data for the event

        :return: a dict representing the event
        """
        data["_source_id"] = _source_id
        data[_id_column] = _entity_id

        return {"event": cls.name, "data": data}

    def __init__(self, data, metadata):
        self._data = data
        self._metadata = metadata

    def pop_ids(self):
        """Removes and returns relevent ids

        :param metadata: metadata, required to locate id columns in data

        :return: entity_id, source_id: ids of this event
        """
        entity_id = self._data.pop(self._metadata.id_column)
        source_id = self._data.pop(self._metadata.source_id_column)

        return entity_id, source_id

    def apply_to(self, entity):
        """Sets the attributes in data on the entity (expands `data['mutations'] first)

        :param entity: the instance to be modified
        :param metadata: the metadata of the import message
        :return:
        """
        entity[self.timestamp_field] = self._metadata.timestamp

        model = self._metadata.model
        for key, value in self._data.items():
            # todo make this more generic (this should not be read from sqlalchemy model, but from GOB-model
            # todo: other data types (like date) require similar conversions
            if isinstance(getattr(model, key).prop.columns[0].type, bool):
                # Except for None values, all new values are string values
                # A boolean string value has to be converted to a boolean
                # Note: Do not use bool(value); bool('False') evaluates to True !
                setattr(entity, key, value == str(True))
            else:
                setattr(entity, key, value)


class ADD(ImportEvent):
    """
    Example:
        ADD
        entity: meetbout
        source: meetboutengis
        source_id: 12881429
        data: {
            meetboutid: "12881429",
            indicatie_beveiligd: "True",
            ....
        }
    """
    name = "ADD"
    is_add_new = True
    timestamp_field = "_date_created"

    @classmethod
    def create_event(cls, _source_id, _id_column, _entity_id, data):
        #   ADD has no modifications, only data
        if modifications_key in data:
            data.pop(modifications_key)

        return super().create_event(_source_id, _id_column, _entity_id, data)


class MODIFY(ImportEvent):
    """
    Example:
        MODIFY
        entity: meetbouten
        source: meetboutengis
        source_id: 12881429
        data: {
            modifications: [{
                key: "indicatie_beveiligd",
                new_value: "False"
                old_value: True,
            }]
        }
    """
    name = "MODIFY"
    timestamp_field = "_date_modified"

    def apply_to(self, entity, metadata):
        # extract modifications from the data, before applying the event to the entity

        modifications = self.__data.pop(self.modifications_key)
        attribute_set = self._extract_modifications(entity, modifications)
        self._data = {**self._data, **attribute_set}

        super().apply_to(entity, metadata)

    def _extract_modifications(self, entity, modifications):
        """extracts attributes to modify, and checks if old values are indeed present on entity

        :param entity: the instance to be modified
        :param modifications: a collection of mutations of attributes to be interpretated
        :return: a dict with extracted and verified mutations
        """
        modified_attributes = {}

        for mutation in modifications:
            current_val = getattr(entity, mutation['key'])
            expected_val = mutation['old_value']
            if current_val != expected_val:
                msg = f"Trying to modify data that is not in sync: entity id {entity._id}, " \
                      f"attribute {mutation['key']} had value '{current_val}', but expected was '{expected_val}'"
                raise RuntimeError(msg)
            else:
                modified_attributes[mutation['key']] = mutation['new_value']
        return modified_attributes

    @classmethod
    def create_event(cls, _source_id, _id_column, _entity_id, data):
        #   MODIFY has no data attributes only modifications
        if modifications_key not in data:
            raise GOBException("MODIFY event requires modifications")
        mods = {modifications_key: data[modifications_key]}

        return super().create_event(_source_id, _id_column, _entity_id, mods)


class DELETE(ImportEvent):
    """
    Example:
        DELETE
        entity: meetbouten
        source: meetboutengis
        source_id: 12881429
        data: {}
    """
    name = "DELETE"
    timestamp_field = "_date_deleted"

    @classmethod
    def create_event(cls, _source_id, _id_column, _entity_id, data):
        #  DELETE has no data
        return super().create_event(_source_id, _id_column, _entity_id, {})


class CONFIRM(ImportEvent):
    """
    Example:
        CONFIRM
        entity: meetbouten
        source: meetboutengis
        source_id: 12881429
        data: {}
    """
    name = "CONFIRM"
    timestamp_field = "_date_confirmed"

    @classmethod
    def create_event(cls, _source_id, _id_column, _entity_id, data):
        #  CONFIRM has no data
        return super().create_event(_source_id, _id_column, _entity_id, {})
