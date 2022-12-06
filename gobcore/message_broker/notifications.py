import pika

from gobcore.message_broker.config import CONNECTION_PARAMS
from gobcore.message_broker.utils import to_json
from gobcore.message_broker.initialise_queues import _create_exchange, _create_queue, _bind_queue

# Use a dedicated exchange for notifications
NOTIFY_EXCHANGE = "gob.notify"
# Each queue in the notification exchange starts with 'gob.notify'
NOTIFY_BASE_QUEUE = 'gob.notify'
# Notifications can be included in a message with the 'notification' key
NOTIFICATION_KEY = 'notification'
# Notification type (used as routing key) and contents
NOTIFICATION_TYPE = 'type'
NOTIFICATION_CONTENTS = 'contents'
# Notification messages copy a selection of the original header fields
NOTIFICATION_HEADER_FIELDS = ['source', 'catalogue', 'collection', 'application', 'entity', 'version', 'process_id']


def listen_to_notifications(id, notification_type=None):
    """
    Listen to notification messages
    The id is used to create a queue on which notifications will be received for the id
    A type can be specified to filter out unwanted messages

    :param id:
    :return:
    """
    queue = f"{NOTIFY_BASE_QUEUE}.{id}"
    return _listen_to_notifications(NOTIFY_EXCHANGE, queue, notification_type or '')


def contains_notification(result_msg):
    """
    Tells wether the message includes any notifications

    :param result_msg:
    :return:
    """
    return result_msg and result_msg.get(NOTIFICATION_KEY)


def send_notification(result_msg):
    """
    Send any notifications that are contained in the result msg

    :param result_msg:
    :return:
    """
    # If a notification has been specified then notification the notification
    if contains_notification(result_msg):
        header_fields = NOTIFICATION_HEADER_FIELDS
        notification = result_msg[NOTIFICATION_KEY]
        notification = {
            'header': {key: result_msg['header'].get(key) for key in header_fields},
            **notification
        }
        # Send notification
        _send_notification(NOTIFY_EXCHANGE, notification_type=notification.get(NOTIFICATION_TYPE), msg=notification)
        # Delete when handled
        del result_msg[NOTIFICATION_KEY]


def add_notification(msg, notification):
    """
    Include a notification in the message

    :param msg:
    :param notification:
    :return:
    """
    msg[NOTIFICATION_KEY] = {
        NOTIFICATION_TYPE: notification.type,
        NOTIFICATION_CONTENTS: notification.contents
    }


def get_notification(msg):
    """
    Returns an Event object for the given message

    :param msg:
    :return:
    """
    if msg.get(NOTIFICATION_TYPE) == EventNotification.type:
        return EventNotification.from_msg(msg)


class EventNotification():

    type = "events"

    def __init__(self, applied, last_event, header=None):
        """
        Initialize an Event Notification

        :param applied: What kind of events have been applied and how many
        :param last_event: The last eventid before and after the events have been applied
        :param header:
        """
        self.contents = {
            'applied': applied,
            'last_event': last_event
        }
        self.header = header

    @classmethod
    def from_msg(cls, msg):
        """
        Construct an Event Notification object from a message

        :param msg:
        :return:
        """
        return cls(**msg[NOTIFICATION_CONTENTS], header=msg['header'])

def _send_notification(exchange, notification_type, msg):
    """
    Send a notification message of a specified type (None for unspecied) on the specified exchange

    :param exchange:
    :param msg:
    :return:
    """
    with pika.BlockingConnection(CONNECTION_PARAMS) as connection:
        channel = connection.channel()

        # Create exchange if it does not yet exist
        _create_exchange(channel=channel, exchange=exchange, durable=True)

        # Convert the message to json
        json_msg = to_json(msg)

        # Send the message as a non-persistent message on the queue
        channel.basic_publish(
            exchange=exchange,
            routing_key=notification_type,
            properties=pika.BasicProperties(
                delivery_mode=2  # Make messages persistent
            ),
            body=json_msg
        )


def _listen_to_notifications(exchange, queue, notification_type=None):
    """
    Listen to notification messages on the specified exchange and queue of a specified type (None for all)

    :param exchange:
    :param queue:
    :return:
    """
    with pika.BlockingConnection(CONNECTION_PARAMS) as connection:
        channel = connection.channel()

        # Create exchange and queue if they do not yet exist
        _create_exchange(channel=channel, exchange=exchange, durable=True)
        _create_queue(channel=channel, queue=queue, durable=True)
        # Bind to the queue and listen to messages of the specifief type
        _bind_queue(channel=channel, exchange=exchange, queue=queue, key=notification_type or '')
    return queue
