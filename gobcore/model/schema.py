import collections
import requests
import re

from jsonschema import RefResolver


class SchemaException(Exception):
    pass


def load_schema(uri, catalog, collection):
    """
    Load json schema for the given catalog and connection

    :param uri:
    :param catalog:
    :param collection:
    :return: resolved json schema converted to GOB model format
    :raises: SchemaException when schema load fails
    """
    try:
        # Load schema
        response = requests.get(uri, timeout=3)
        schema = response.json()

        # Resolve schema
        resolved = _resolve_all(uri, schema)

        # Convert to GOBModel format
        return _to_gob(resolved)
    except Exception as e:
        raise SchemaException(f"Failed to load schema {uri}: {str(e)}")


def _do_resolve(node, resolver):
    """
    Recursively resolve node using the given resolver to resolve reference to other schemas

    :param node:
    :param resolver:
    :return: resolved node
    """
    if isinstance(node, collections.Mapping) and '$ref' in node:
        resolved_node = {}
        resolved_node.update(node)
        with resolver.resolving(node['$ref']) as resolved:
            resolved_node.update(resolved)
            return resolved_node
    elif isinstance(node, collections.Mapping):
        for k, v in node.items():
            node[k] = _do_resolve(v, resolver)
    elif isinstance(node, (list, tuple)):
        for i in range(len(node)):
            node[i] = _do_resolve(node[i], resolver)
    return node


def _resolve_all(uri, spec):
    """
    Resolve the given schema (spec)

    :param uri:
    :param spec:
    :return:
    """
    return _do_resolve(spec, RefResolver(uri, spec))


def _get_gob_info(value):
    """
    Given a schema value, transform it into a GOB model structure

    :param value:
    :return:
    """
    ams_class = value.get("ams.class")
    if ams_class is not None:
        # Example value: https://ams-schema.glitch.me/gebieden/bouwblokken@v0.1
        # Take:                                       ---1---- ----2------
        pattern = re.compile(r"\/(\w*)\/(\w*)\@.*$")
        match = pattern.search(ams_class)
        try:
            catalog = match.group(1)
            collection = match.group(2)
            return {
                "type": "GOB.Reference",
                "ref": f"{catalog}:{collection}"
            }
        except AttributeError:
            raise SchemaException(f"Failed to derive catalog and collection from {ams_class}")

    titles = {
        "GeoJSON Point": "GOB.Geo.Point"
    }
    if value.get("title") in titles:
        return {
            "type": titles[value["title"]],
            "srid": "RD"
        }

    types = {
        "string": "GOB.String",
        "number": "GOB.Decimal",
        "integer": "GOB.Integer",
        "object": "GOB.JSON",
        "boolean": "GOB.Boolean"
    }
    if value.get("type") in types:
        return {
            "type": types[value["type"]]
        }

    raise SchemaException(f"Failed to convert {value} to GOB info")


def _to_gob(spec):
    """
    Transforms the given schema (spec) to a GOB model

    :param spec:
    :return:
    """

    model = {}

    for key, value in spec["properties"].items():
        model[key] = value
        model[key].update(_get_gob_info(value))

    return model
