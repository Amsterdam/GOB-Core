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


def get_event_for(entity, data, metadata, modifications):
    """
    Get the event dict for the given entity, source-data, metadata and calculated modifications

    :param entity:
    :param data:
    :param metadata:
    :param modifications:

    :return: the event dict
    """
    has_old_state = entity is not None
    has_new_state = data is not None
    has_modifications = len(modifications) > 0

    gob_event = _get_event_class_for(has_old_state, has_new_state, has_modifications)

    # get relevenant id's from either entity or data
    if has_old_state:
        source_id = entity._source_id
        entity_id = getattr(entity, metadata.id_column)
    else:
        source_id = data[metadata.id_column]
        entity_id = source_id

    modifications_dict = dict(modifications=modifications)
    data = modifications_dict if data is None else {**modifications_dict, **data}

    return gob_event.create_event(
        source_id, metadata.id_column, entity_id, data)


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
