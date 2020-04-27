from unittest import TestCase
from unittest.mock import MagicMock, patch, ANY

import datetime

from gobcore.model import FIELD
from gobcore.quality.issue import QA_LEVEL, Issue, IssueException, log_issue, process_issues, is_functional_process

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

    def test_get_id(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        self.assertEqual(issue.get_unique_id(), 'any_check_attr_any id')

        entity = {
            'id': 'any id',
            'attr': 'any attr',
            FIELD.SEQNR: 1
        }
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        self.assertEqual(issue.get_unique_id(), 'any_check_attr_any id_1')

    def test_join_issue(self):
        entity = {
            'id': 'any id',
            'attr': 5
        }
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        other_issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        issue.join_issue(other_issue)
        self.assertEqual(issue.value, '5, 5')

        entity['attr'] = None
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        other_issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        issue.join_issue(other_issue)
        self.assertEqual(issue.value, f"{Issue._NO_VALUE}, {Issue._NO_VALUE}")

        other_issue.entity_id = 'other id'
        with self.assertRaises(IssueException):
            issue.join_issue(other_issue)

    def test_sorted_value(self):
        entities = [{'id': 'any_id', 'attr': value} for value in [1, 8, 7, 5, 9]]
        issues = [Issue({'id': 'any_check'}, entity, 'id', 'attr') for entity in entities]
        issue = issues[0]
        for other_issue in issues[1:]:
            issue.join_issue(other_issue)
        self.assertEqual(issue.value, '1, 5, 7, 8, 9')

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

    def test_log_issue_no_entity(self):
        # Skip issues that are not linked to an entity
        entity = {
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check', 'msg': 'any msg'}, entity, 'id', 'attr')
        mock_logger = MagicMock()
        mock_logger.get_name.return_value = "any name"
        log_issue(mock_logger, QA_LEVEL.INFO, issue)
        mock_logger.add_issue.assert_not_called()

    @patch("gobcore.quality.issue.logger")
    @patch("gobcore.quality.issue.start_workflow")
    @patch("gobcore.quality.issue.is_functional_process")
    def test_process_issues(self, mock_is_functional, mock_start_workflow, mock_logger):
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

        mock_is_functional.return_value = False
        process_issues(msg)
        mock_start_workflow.assert_not_called()

        mock_is_functional.return_value = True
        msg['header']['catalogue'] = 'qa'
        process_issues(msg)
        mock_start_workflow.assert_not_called()

        mock_is_functional.return_value = True
        msg['header']['catalogue'] = 'any catalogue'
        process_issues(msg)
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

    def test_is_functional_process(self):
        for process in ['aap', 'noot', '']:
            self.assertFalse(is_functional_process(process))

        for process in ['Import', 'import', 'ImporT', 'IMPORT', 'compare', 'relate']:
            self.assertTrue(is_functional_process(process))
