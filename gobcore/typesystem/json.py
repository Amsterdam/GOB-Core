import datetime
import decimal
import json
import enum

from orjson import orjson

from gobcore.typesystem.gob_types import GOBType


class GobTypeJSONEncoder(json.JSONEncoder):
    """Extension of the json.JSONEncoder to help with encoding GOB Types into native JSON

    Use as follows:

        import json

        gob_type = Point.from_values(x=1, y=2)
        json.dumps(gob_type, cls=GobTypeJSONEncoder)

    GOB.Geo-types will return geojson
    """

    def default(self, obj):  # noqa: C901 (Function is too complex)
        if isinstance(obj, GOBType):
            return json.loads(obj.json)

        if type(obj) is datetime.datetime:
            value = obj.isoformat()
            if len(value) == len('YYYY-MM-DDTHH:MM:SS'):
                # Add missing microseconds
                value += '.000000'
            return value

        if type(obj) is datetime.date:
            return obj.isoformat()

        if isinstance(obj, decimal.Decimal):
            return json.loads(str(obj))

        if isinstance(obj, enum.Enum):
            return str(obj.value)

        return super().default(obj)


class GobTypeORJSONEncoder:

    def __call__(self, obj):  # noqa: C901 (Function is too complex)
        if isinstance(obj, GOBType):
            return orjson.loads(obj.json)

        elif type(obj) is datetime.date:
            return obj.isoformat()

        elif type(obj) is datetime.datetime:
            # force YYYY-MM-DDTHH:MM:SS.ffffff
            return obj.isoformat(timespec="microseconds")

        elif isinstance(obj, decimal.Decimal):
            return str(obj)

        else:
            raise TypeError(type(obj))
