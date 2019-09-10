import pytest
from decimal import Decimal
from math import inf, nan
from json.decoder import JSONDecodeError
from unittest.mock import patch

from gobcore.message_broker.utils import to_json, from_json, get_message_from_body

@pytest.mark.parametrize(
    "obj, expected, success",
    [
        ({"abc": {"xyz": "bar"}}, '{"abc": {"xyz": "bar"}}', True),
        ({"abc": {"xyz": None}}, '{"abc": {"xyz": null}}', True),
        ({"abc": nan}, None, False),
        ({"abc": inf}, None, False),
    ],
)
def test_to_json(obj, expected, success):
    if success:
        assert to_json(obj) == expected
    else:
        with pytest.raises(ValueError):
            to_json(obj)


@pytest.mark.parametrize(
    "obj, expected",
    [
        (b'{"abc": {"xyz": "bar"}}', {"abc": {"xyz": "bar"}}),
        (b'{"foo": ["baz", null, 1.0, 2]}', {"foo": ["baz", None, Decimal('1.0'), 2]}),
        (b'{"abc": {"xyz": "bar"}, "num": 3.14}', {"abc": {"xyz": "bar"}, "num": Decimal('3.14')}),
        (b'{"abc": {"xyz": "bar"}, "num": 12.56789}', {"abc": {"xyz": "bar"}, "num": Decimal('12.56789')}),
        (b'{}', {}),
        (b'[]', []),
        ('{"abc": {"xyz": "bar"}}', {"abc": {"xyz": "bar"}}),
        ('{"foo": ["baz", null, 1.0, 2]}', {"foo": ["baz", None, Decimal('1.0'), 2]}),
        ('{"abc": {"xyz": "bar"}, "num": 3.14}', {"abc": {"xyz": "bar"}, "num": Decimal('3.14')}),
        ('{"abc": {"xyz": "bar"}, "num": 12.56789}', {"abc": {"xyz": "bar"}, "num": Decimal('12.56789')}),
        ('{}', {}),
        ('[]', []),
        ('3', Decimal('3')),
        ('3.14', Decimal('3.14')),
    ],
)
def test_from_json(obj, expected):
    assert from_json(obj) == expected


@pytest.mark.parametrize(
    "obj, msg, offload_id, params, load_message_called",
    [
        (b'{"abc": {"xyz": "bar"}}', {"abc": {"xyz": "bar"}}, None, {"load_message": False}, False),
        (b'{"foo": ["baz", null, 1.0, 2]}', {"foo": ["baz", None, Decimal('1.0'), 2]}, None, {"load_message": False}, False),
        (b'{"num": 3.14}', {"num": Decimal('3.14')}, None, {"load_message": False}, False),
        (b'{"num": 12.56789}', {"num": Decimal('12.56789')}, None, {"load_message": False}, False),
        (b'{}', {}, None, {"load_message": False}, False),
        (b'[]', [], None, {"load_message": False}, False),
        ('{"abc": {"xyz": "bar"}}', {"abc": {"xyz": "bar"}}, None, {"load_message": False}, False),
        ('{"foo": ["baz", null, 1.0, 2]}', {"foo": ["baz", None, Decimal('1.0'), 2]}, None, {"load_message": False}, False),
        ('{"num": 3.14}', {"num": Decimal('3.14')}, None, {"load_message": False}, False),
        ('{"num": 12.56789}', {"num": Decimal('12.56789')}, None, {"load_message": False}, False),
        ('{}', {}, None, {"load_message": False}, False),
        ('[]', [], None, {"load_message": False}, False),
        ('3', Decimal('3'), None, {"load_message": False}, False),
        ('3.14', Decimal('3.14'), None, {"load_message": False}, False),
        ('abc', 'abc', None, {"load_message": False}, False),
        (b'{"abc": {"xyz": "bar"}}', {"abc": {"xyz": "bar"}}, None, {"load_message": True}, True),
    ],
)
@patch("gobcore.message_broker.utils.load_message")
def test_get_message_from_body(mock_load_message, obj, msg, offload_id, params, load_message_called):
    mock_load_message.return_value = (msg, offload_id)
    assert get_message_from_body(obj, params) == (msg, offload_id)
    if load_message_called:
        mock_load_message.assert_called_with(msg, from_json, params)
