"""Queue configuration for events exchange.

GOB Events are always stored in the database in the events table. However, the 'store events' process is expected to
send each event to the EVENTS_EXCHANGE separately as well, to be picked up by other processes for further processing.
"""
NEW_EVENT_PREFIX = "event."


def get_routing_key(catalog: str, collection: str):
    """Returns routing key for event on catalog/collection.

    :param catalog:
    :param collection:
    :return:
    """
    return f"{NEW_EVENT_PREFIX}{catalog}.{collection}"
