from unittest import TestCase
from unittest.mock import patch, MagicMock, ANY

from gobcore.workflow.start_workflow import start_workflow, retry_workflow

@patch("gobcore.workflow.start_workflow.WORKFLOW_EXCHANGE", 'workflow exchange')
@patch("gobcore.workflow.start_workflow.WORKFLOW_REQUEST_KEY", 'workflow request key')
class TestStartWorkflow(TestCase):

    @patch("gobcore.workflow.start_workflow.pika.BasicProperties")
    @patch("gobcore.workflow.start_workflow.pika.BlockingConnection")
    def test_start_workflow(self, mock_blocking_connection, mock_basic_properties):
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_blocking_connection.return_value.__enter__.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        start_workflow('any workflow', {'arguments': 'any arguments'})
        mock_channel.basic_publish.assert_called_with(
            body='{"header": {"arguments": "any arguments"}, "contents": {}, "workflow": "any workflow"}',
            exchange='workflow exchange',
            properties=mock_basic_properties(
                delivery_mode=2  # Make messages persistent
            ),
            routing_key='workflow request key')

        start_workflow('any workflow', {'arguments': 'any arguments', 'contents_ref': 'contents ref'})
        mock_channel.basic_publish.assert_called_with(
            body='{"header": {"arguments": "any arguments", "contents_ref": "contents ref"}, "contents": {}, "workflow": "any workflow", "contents_ref": "contents ref"}',
            exchange='workflow exchange',
            properties=mock_basic_properties(
                delivery_mode=2  # Make messages persistent
            ),
            routing_key='workflow request key')

    @patch("gobcore.workflow.start_workflow.pika.BasicProperties")
    @patch("gobcore.workflow.start_workflow.pika.BlockingConnection")
    def test_retry_workflow(self, mock_blocking_connection, mock_basic_properties):
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_blocking_connection.return_value.__enter__.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel

        msg = {
            'workflow': {
            }
        }

        # No retry when no retry is specified
        result = retry_workflow(msg)
        self.assertFalse(result)
        mock_channel.basic_publish.assert_not_called()

        msg = {
            'workflow': {
                'retry_time': 0
            }
        }
        # No retry when out of retry time
        result = retry_workflow(msg)
        self.assertFalse(result)
        mock_channel.basic_publish.assert_not_called()

        msg = {
            'workflow': {
                'retry_time': 100
            }
        }
        # Retry when retry is specified
        result = retry_workflow(msg)
        mock_channel.basic_publish.assert_called_with(
            body='{"workflow": {"retry_time": 40}}',
            exchange='',
            properties=ANY,
            routing_key='gob.workflow.workflow.queue_delay'
        )
        self.assertTrue(result)
