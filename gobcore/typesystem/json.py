import json

from gobcore.typesystem.gob_types import GOBType


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
        return super().default(self, obj)
