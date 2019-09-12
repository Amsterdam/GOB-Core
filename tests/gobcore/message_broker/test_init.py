from unittest import TestCase
from unittest.mock import patch

from gobcore.message_broker import CONNECTION_PARAMS, publish


class TestInitFile(TestCase):

    @patch("gobcore.message_broker.AsyncConnection")
    def test_publish(self, mock_async_connection):
        exchange = 'exchange'
        key = 'key'
        msg = 'msg'
        publish(exchange, key, msg)
        mock_async_connection.return_value.__enter__.return_value.publish.assert_called_with(exchange, key, msg)
        mock_async_connection.assert_called_with(CONNECTION_PARAMS)

