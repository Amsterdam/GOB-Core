from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import CONNECTION_PARAMS, get_queue


def publish(queue_name, key, msg):
    with AsyncConnection(CONNECTION_PARAMS) as connection:
        connection.publish(get_queue(queue_name), key, msg)
