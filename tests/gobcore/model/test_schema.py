import unittest
import mock

from gobcore.model.schema import _get_gob_info, _to_gob, _do_resolve, SchemaException, load_schema


class TestAMSSchema(unittest.TestCase):

    def setUp(self):
        pass

    @mock.patch("gobcore.model.schema.requests.get")
    @mock.patch("gobcore.model.schema._resolve_all")
    @mock.patch("gobcore.model.schema._to_gob")
    def test_load_schema(self, mock_to_gob, mock_resolve, mock_get):
        self.assertEqual(mock_to_gob.return_value, load_schema('uri', 'catalog', 'collection'))
        mock_get.assert_called_with('uri', timeout=3)
        mock_to_gob.assert_called_with(mock_resolve.return_value)

    def test_schema_gob_info(self):
        expect = {
            "string": "GOB.String",
            "number": "GOB.Decimal",
            "integer": "GOB.Integer",
            "object": "GOB.JSON",
            "boolean": "GOB.Boolean"
        }

        for t in expect.keys():
            result = _get_gob_info({
                "type": t
            })
            self.assertEqual(result, {
                "type": expect[t]
            })

        result = _get_gob_info({
            "title": "GeoJSON Point"
        })
        self.assertEqual(result, {
            "type": "GOB.Geo.Point",
            "srid": "RD"
        })

        result = _get_gob_info({
            "ams.class": "http(s)://some-address/some-more/catalog/collection@vn.m"
        })
        self.assertEqual(result, {
            "ref": "catalog:collection",
            "type": "GOB.Reference"
        })

        with self.assertRaises(SchemaException):
            result = _get_gob_info({
                "ams.class": "some-rubbish"
            })

        with self.assertRaises(SchemaException):
            result = _get_gob_info({})
            self.assertEqual(result, None)

        with self.assertRaises(SchemaException):
            result = _get_gob_info({
                "title": "some-rubbish"
            })

        with self.assertRaises(SchemaException):
            result = _get_gob_info({
                "type": "some-rubbish"
            })

    def test_to_gob(self):
        model = _to_gob({
            "properties": {
                "x": {
                    "type": "string",
                    "anything": "else"
                },
                "y": {
                    "type": "string",
                    "anything": "else"
                }
            }
        })
        self.assertEqual(model, {
            'x': {'anything': 'else', 'type': 'GOB.String'},
            'y': {'anything': 'else', 'type': 'GOB.String'}
        })

    def test_do_resolve(self):
        resolver = mock.MagicMock()

        node = {
            "a": "b"
        }
        result = _do_resolve(node, resolver)
        self.assertEqual(result, node)

        node = {
            "a": "b",
            "c": {
                "d": "e"
            }
        }
        result = _do_resolve(node, resolver)
        self.assertEqual(result, node)

        node = {
            "a": "b",
            "$ref": "da ref"
        }
        resolver.resolving.return_value.__enter__.return_value = {'e': 'f'}
        result = _do_resolve(node, resolver)
        resolver.resolving.assert_called_with('da ref')
        self.assertEqual({'$ref': 'da ref', 'a': 'b', 'e': 'f'}, result)

        # Same as previous, now as part of list
        node = [node]
        self.assertEqual([result], _do_resolve(node, resolver))

