import datetime
import decimal
import json

from gobcore.typesystem.gob_types import GOBType, Date, DateTime


class GobTypeJSONEncoder(json.JSONEncoder):
    """Extension of the json.JSONEncoder to help with encoding GOB Types into native JSON

    Use as follows:

        import json

        gob_type = Point.from_values(x=1, y=2)
        json.dumps(gob_type, cls=GobTypeJSONEncoder)

    GOB.Geo-types will return geojson
    """

    def default(self, obj):
        if isinstance(obj, GOBType):
            return json.loads(obj.json)

        if isinstance(obj, decimal.Decimal):
            return json.loads(str(obj))

        if type(obj) is datetime.date:
            # First convert datetime to string and use GOBType to create JSON output
            string_value = datetime.datetime.strftime(obj, Date.internal_format)
            obj = Date.from_value(string_value, **{'format': Date.internal_format})
            return json.loads(obj.json)

        if type(obj) is datetime.datetime:
            # First convert datetime to string and use GOBType to create JSON output
            string_value = datetime.datetime.strftime(obj, DateTime.internal_format)
            obj = DateTime.from_value(string_value, **{'format': DateTime.internal_format})
            return json.loads(obj.json)

        return super().default(obj)
