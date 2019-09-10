import decimal
import json

from gobcore.message_broker.offline_contents import load_message
from gobcore.typesystem.json import GobTypeJSONEncoder


def to_json(obj):
    return json.dumps(obj, cls=GobTypeJSONEncoder, allow_nan=False)


def from_json(obj):
    return json.loads(obj, parse_float=decimal.Decimal)


def get_message_from_body(body, params):
    offload_id = None
    try:
        # Try to parse body as json message, else pass it as it is received
        msg = from_json(body)

        # Allow for offline contents
        if params["load_message"]:
            msg, offload_id = load_message(msg, from_json, params)
    except (TypeError, json.decoder.JSONDecodeError):
        # message was not json, pass message as it is received
        msg = body

    return msg, offload_id
