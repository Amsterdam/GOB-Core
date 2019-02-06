"""GOB Events

The event definitions that are defined in the gob_events module are

GOB events are the add, modification, deletion and confirmation of data
The possible events are defined in this module.
The definition and characteristics of each event is in the gob_events module

"""
from gobcore.exceptions import GOBException
from gobcore.events import import_events

# The possible events are imported from the gob_events module
GOB = import_events

# The actual events that are used within GOB
GOB_EVENTS = [
    GOB.ADD,
    GOB.DELETE,
    GOB.MODIFY,
    GOB.CONFIRM
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
    except KeyError:
        raise GOBException(f"{name} is an invalid GOB event")


def get_event_for(old_data, new_data, modifications):
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
        # The source_id might change, so save the old entity source_id
        _entity_source_id = old_data._source_id
    else:
        # ADD
        # No previous event, entity source id is equal to the new source id
        _last_event = None
        _entity_source_id = new_data["_source_id"]

    if has_new_state:
        # ADD, MODIFY, CONFIRM
        # Register the source of the event
        _source_id = new_data["_source_id"]
        _hash = new_data[import_events.hash_key]
    else:
        # DELETE
        # No new source id, keep existing source id
        _source_id = old_data._source_id
        _hash = None

    # The event data are the modifications
    data = dict(modifications=modifications)
    data["_last_event"] = _last_event
    data[import_events.hash_key] = _hash

    if has_new_state:
        # Merge the new state data (possible ADD event)
        # The ultimate event class will filter either the modifications (MODIFY) or the new data (ADD)
        data.update(new_data)

    return gob_event.create_event(_source_id, _entity_source_id, data)


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
    elif has_new_state and not has_old_state:
        return GOB.ADD

    if has_modifications:
        return GOB.MODIFY
    else:
        return GOB.CONFIRM


def GobEvent(event_message, metadata):
    """Get the event instance for a given event, instantiated with the data of the event

    Example:
        get_event({'event': "ADD", 'data': data) => GOBAction:ADD

    :param name:
    :return: the instantiated event
    """

    event_name = event_message["event"]
    data = event_message["data"]

    return _get_event(event_name)(data, metadata)
