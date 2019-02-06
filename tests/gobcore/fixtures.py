import random
import string

from gobcore.events import GOB_EVENTS
from gobcore.events import import_events
from gobcore.events.import_message import MessageMetaData


class Entity(object):
    def __init__(self, initial_data):
        for key in initial_data:
            setattr(self, key, initial_data[key])


def random_string(length=12, source=None):
    if source is None:
        source = string.ascii_letters

    return ''.join(random.choice(source) for x in range(length))


def get_service_fixture(handler):
    return {
        random_string(): {
            'exchange': random_string(),
            'queue': random_string(),
            'key': random_string(),
            'handler': handler,
            'report': {
                'exchange': random_string(),
                'queue': random_string(),
                'key': random_string()
            }
        }
    }

def random_bool():
    return random.choice([True, False])


def random_gob_event():
    return random.choice(GOB_EVENTS)


def get_event_data_fixture(gob_event):
    if gob_event.name == 'MODIFY':
        return {import_events.modifications_key: {"_last_event": None}, import_events.hash_key: None}
    return {"_last_event": None, import_events.hash_key: None}


def get_event_fixture():
    gob_event = random_gob_event()
    data = get_event_data_fixture(gob_event)
    return gob_event.create_event(random_string(), random_string(), data)


def get_data_fixture():
    data = {
        "_source_id": random_string(),
        "_hash": random_string(),
        "_last_event": random.randint(0, 100)
    }
    return data


def get_entity_fixture(data):
    entity = Entity(data)
    return entity


def get_metadata_fixture():
    header = {key: random_string() for key in ["source", "timestamp", "version", "model", "application"]}
    header['catalogue'] = 'meetbouten'
    header['entity'] = 'meetbouten'
    header['id_column'] = 'meetboutid'
    header['process_id'] = f"{header['timestamp']}.{header['source']}.{header['entity']}"
    return MessageMetaData(header)
