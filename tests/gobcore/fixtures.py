import random
import string

from gobcore.events import GOB_EVENTS
from gobcore.events import import_events
from gobcore.events.import_message import MessageMetaData


def random_string(length=12, source=None):
    if source is None:
        source = string.ascii_letters

    return ''.join(random.choice(source) for x in range(length))


def get_service_fixture(handler):
    return {
        random_string(): {
            'queue': random_string(),
            'handler': handler,
            'report_back': random_string(),
            'report_queue': random_string()
        }
    }


def random_bool():
    return random.choice([True, False])


def random_gob_event():
    return random.choice(GOB_EVENTS)


def get_event_data_fixture(gob_event):
    if gob_event.name == 'MODIFY':
        return {import_events.modifications_key: {}}
    return {}


def get_event_fixture():
    gob_event = random_gob_event()
    data = get_event_data_fixture(gob_event)
    return gob_event.create_event(random_string(), random_string(), random_string(), data)


def get_metadata_fixture():
    header = {key: random_string() for key in ["source", "timestamp", "entity_id", "entity", "version", "gob_model"]}
    return MessageMetaData(header)
