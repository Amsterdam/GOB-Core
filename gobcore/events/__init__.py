"""GOB Events

GOB events are the add, modification, deletion and confirmation of data.
The possible events are defined in this module.
The definition and characteristics of each event is in the gob_events module.
"""

import json

from gobcore.exceptions import GOBException
from gobcore.events import import_events
from gobcore.events.import_message import MessageMetaData
from gobcore.events.import_events import ImportEvent
from gobcore.model import GOBModel
from gobcore.model.migrations import GOBMigrations

# The possible events are imported from the gob_events module
GOB = import_events

# The actual events that are used within GOB
GOB_EVENTS = [
    GOB.ADD,
    GOB.DELETE,
    GOB.MODIFY,
    GOB.CONFIRM,
    GOB.BULKCONFIRM
]

# Convert GOB_EVENTS to a dictionary indexed by the name of the event
_gob_events_dict = {event.name: event for event in GOB_EVENTS}


def _get_event(name):
    """
    Get the event definition for a given event name

    :param name:
    :return: the event definition (class) for the given event name
    """
    try:
        return _gob_events_dict[name]
    except KeyError as exc:
        raise GOBException(f"{name} is an invalid GOB event") from exc


def get_event_for(old_data, new_data, modifications, version):
    """
    Get the event for the given entity, source-data, metadata and calculated modifications

    :param old_data: the entity data as stored in the database
    :param new_data: the entity data as received in the message
    :param modifications: the list of modifications for the entity (old_data - new_data)

    :return: the event that corresponds with the given parameters
    """
    has_old_state = old_data is not None
    has_new_state = new_data is not None
    assert has_old_state or has_new_state, "Event without old or new state"

    has_modifications = len(modifications) > 0

    # Get the event class (ADD, MODIFY, CONFIRM, DELETE)
    gob_event = _get_event_class_for(has_old_state, has_new_state, has_modifications)

    if has_old_state:
        # MODIFY, CONFIRM, DELETE
        # Attach event to the previous event for the entity
        _last_event = old_data._last_event
    else:
        # ADD
        # No previous event, entity source id is equal to the new source id
        _last_event = None

    if has_new_state:
        # ADD, MODIFY, CONFIRM
        # Register the source of the event
        _hash = new_data[import_events.hash_key]
    else:
        # DELETE
        # No new source id, keep existing source id
        _hash = None

    # The event data are the modifications
    data = dict(modifications=modifications)
    data["_last_event"] = _last_event
    data[import_events.hash_key] = _hash

    if has_new_state:
        # Merge the new state data (possible ADD event)
        # The ultimate event class will filter either the modifications (MODIFY) or the new data (ADD)
        data.update(new_data)

    tid = old_data._tid if has_old_state else new_data['_tid']
    return gob_event.create_event(tid, data, version)


def _get_event_class_for(has_old_state, has_new_state, has_modifications):
    """
    Get the event class for the given situation.

    :param has_old_state:
    :param has_new_state:
    :param has_modifications:

    :return: the event class
    """
    if not (has_old_state or has_new_state):
        raise GOBException("Undetermined class, there should always be at least an old "
                           "or a new state to a change")

    if has_old_state and not has_new_state:
        return GOB.DELETE
    if has_new_state and not has_old_state:
        return GOB.ADD

    if has_modifications:
        return GOB.MODIFY
    return GOB.CONFIRM


def GobEvent(tid, event_message, metadata):
    """Get the event instance for a given event, instantiated with the data of the event

    Example:
        get_event({'event': "ADD", 'data': data) => GOBAction:ADD

    :param name:
    :return: the instantiated event
    """

    event_name = event_message["event"]
    data = event_message["data"]

    return _get_event(event_name)(tid, data, metadata)


def database_to_gobevent(event) -> ImportEvent:
    """Reconstruct the original event out of the stored event

    :param event: the database event
    :return: a ADD, MODIFY, CONFIRM or DELETE event
    """
    # Parse the json data of the event
    if isinstance(event.contents, dict):
        data = event.contents
    else:
        data = json.loads(event.contents)

    # Get the model version to check if the event should be migrated to the correct version.
    # No legacy_mode! -- database_to_gobevent is only used by GOB-Upload.
    model_version = GOBModel().get_collection(event.catalogue, event.entity)['version']

    if model_version != event.version:
        # Event should be migrated to the correct GOBModel version
        data = GOBMigrations().migrate_event_data(event, data, event.catalogue, event.entity, model_version)

    event_msg = {
        "event": event.action,
        "data": data,
    }

    msg_header = {
        "process_id": None,
        "source": event.source,
        "application": event.application,
        "id_column": data.get("id_column"),
        "catalogue": event.catalogue,
        "entity": event.entity,
        "version": event.version,
        "timestamp": event.timestamp,
    }

    # Construct the event out of the reconstructed event data
    gob_event = GobEvent(event.tid, event_msg, MessageMetaData(msg_header))

    # Store the id of the event in the gob_event
    gob_event.id = event.eventid
    return gob_event
