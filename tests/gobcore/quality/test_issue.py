from unittest import TestCase
from unittest.mock import MagicMock, patch, ANY

import datetime

from gobcore.model import FIELD
from gobcore.quality.issue import QA_LEVEL, Issue, IssueException, log_issue, process_issues

class Mock_QA_CHECK():
    any_check = {
        'id': 'any check'
    }

@patch("gobcore.quality.issue.QA_CHECK", Mock_QA_CHECK)
class TestIssue(TestCase):

    def test_issue(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }

        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        self.assertEqual(issue.check, {'id': 'any_check'})
        self.assertEqual(issue.entity_id_attribute, 'id')
        self.assertEqual(issue.entity_id, entity['id'])
        self.assertIsNone(getattr(issue, FIELD.SEQNR))
        self.assertEqual(issue.attribute, 'attr')
        self.assertEqual(issue.value, entity['attr'])
        self.assertIsNone(issue.compared_to)
        self.assertIsNone(issue.compared_to_value)

        issue = Issue({'id': 'any_check'}, entity, None, 'attr')
        self.assertEqual(issue.entity_id_attribute, Issue._DEFAULT_ENTITY_ID)
        self.assertIsNone(issue.entity_id)

        entity[FIELD.SEQNR] = 'any seqnr'
        issue = Issue({'id': 'any_check'}, entity, None, 'attr')
        self.assertEqual(getattr(issue, FIELD.SEQNR), 'any seqnr')

        issue = Issue({'id': 'any_check'}, entity, None, 'attr', 'other attr')
        self.assertEqual(issue.compared_to, 'other attr')
        self.assertIsNone(issue.compared_to_value)

        entity['other attr'] = 'any other value'
        issue = Issue({'id': 'any_check'}, entity, None, 'attr', 'other attr')
        self.assertEqual(issue.compared_to, 'other attr')
        self.assertEqual(issue.compared_to_value, 'any other value')

    def test_issue_fails(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        with self.assertRaises(IssueException):
            issue = Issue({}, entity, 'id', 'attr')

    def test_get_value(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')

        entity['x'] = None
        result = issue._get_value(entity, 'x')
        self.assertIsNone(result)

        for value in [1, True, "s", 2.0]:
            entity['x'] = value
            result = issue._get_value(entity, 'x')
            self.assertEqual(result, value)

        entity['x'] = datetime.datetime.now()
        result = issue._get_value(entity, 'x')
        self.assertTrue(isinstance(result, str))

    def test_format_value(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')

        result = issue._format_value(None)
        self.assertEqual(result, Issue._NO_VALUE)

        result = issue._format_value(1)
        self.assertEqual(result, "1")

    def test_msg(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        check = {
            'id': 'any_check',
            'msg': 'any msg'
        }
        issue = Issue(check, entity, 'id', 'attr')

        result = issue.msg()
        self.assertEqual(result, "attr: any msg")

        issue = Issue(check, entity, 'id', 'attr', 'any compared to')
        result = issue.msg()
        self.assertEqual(result, "attr: any msg any compared to")

    def test_log_args(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        check = {
            'id': 'any_check',
            'msg': 'any msg'
        }
        issue = Issue(check, entity, 'id', 'attr')

        result = issue.log_args()
        self.assertEqual(result, {
            'id': 'attr: any msg',
            'data': {
                'id': 'any id',
                FIELD.SEQNR: None,
                'attr': 'any attr'
            }})

        issue = Issue(check, entity, 'id', 'attr', 'any compared to', 'any compare to value')

        result = issue.log_args()
        self.assertEqual(result, {
            'id': 'attr: any msg any compared to',
            'data': {
                'id': 'any id',
                FIELD.SEQNR: None,
                'attr': 'any attr',
                'any compared to': 'any compare to value'
            }})

        result = issue.log_args(any_key="any value")
        self.assertEqual(result, {
            'id': 'attr: any msg any compared to',
            'data': {
                'id': 'any id',
                FIELD.SEQNR: None,
                'attr': 'any attr',
                'any compared to': 'any compare to value',
                'any_key': 'any value'
            }})

    def test_get_explanation(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        self.assertIsNone(issue.get_explanation())

        issue.compared_to = 'to'
        issue.compared_to_value = 'value'
        self.assertEqual(issue.get_explanation(), 'to = value')

        issue.explanation = 'explanation'
        self.assertEqual(issue.get_explanation(), 'explanation')

    def test_log_issue(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check', 'msg': 'any msg'}, entity, 'id', 'attr')
        mock_logger = MagicMock()
        mock_logger.get_name.return_value = "any name"
        log_issue(mock_logger, QA_LEVEL.INFO, issue)
        mock_logger.add_issue.assert_called()

    @patch("gobcore.quality.issue.start_workflow")
    def test_process_issues(self, mock_start_workflow):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check', 'msg': 'any msg'}, entity, 'id', 'attr')
        mock_logger = MagicMock()
        mock_logger.get_name.return_value = "any name"
        mock_issue = MagicMock()
        mock_logger.get_issues.return_value = [mock_issue]

        msg = {
            'header': {
                'source': 'any source',
                'application': 'any application',
                'catalogue': 'any catalogue',
                'collection': 'any collection',
                'attribute': 'any attribute',
                'other': 'any other',
                'mode': 'any mode'
            }
        }
        process_issues(msg, mock_logger)
        mock_start_workflow.assert_called_with({
            'workflow_name': "import",
            'step_name': "update_model"
        }, {
            'header': {
                'catalogue': 'qa',
                'entity': 'any catalogue_any collection',
                'collection': 'any catalogue_any collection',
                'source': 'any name_any source_any application_any attribute',
                'application': 'any application',
                'timestamp': ANY,
                'version': '0.1',
                'mode': 'any mode'
            },
            'contents': ANY,
            'summary': {
                'num_records': 1
            }
        })
