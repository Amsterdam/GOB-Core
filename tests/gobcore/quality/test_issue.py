from unittest import TestCase
from unittest.mock import MagicMock, patch, ANY

import datetime

from gobcore.model import FIELD
from gobcore.quality.issue import QA_LEVEL, Issue, IssueException, log_issue, process_issues, is_functional_process, \
                                  _start_issue_workflow

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

    def test_json(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr',
            'compared attr': 'any compared value'
        }
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr', 'compared attr')

        expected_json = '{"check": {"id": "any_check"}, "entity": {"id": "any id", "volgnummer": null, "begin_geldigheid": null, "eind_geldigheid": null, "attr": "any attr"}, "id_attribute": "id", "attribute": "attr", "compared_to": "compared attr", "compared_to_value": "any compared value"}'

        self.assertEqual(expected_json, issue.json)

        from_json = Issue.from_json(expected_json)
        self.assertEqual(from_json.value, issue.value)

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
        other_entity = {
            'id': 'any id',
            'attr': 6
        }
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        other_issue = Issue({'id': 'any_check'}, other_entity, 'id', 'attr')
        issue.join_issue(other_issue)
        self.assertEqual(issue.value, '5, 6')

        # The same value will not be stored twice
        issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        other_issue = Issue({'id': 'any_check'}, entity, 'id', 'attr')
        issue.join_issue(other_issue)
        self.assertEqual(issue.value, 5)

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
        # Issue without id. Should add issue, but not log it
        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check', 'msg': 'any msg'}, entity, 'id', 'attr')
        mock_logger = MagicMock()
        mock_logger.get_name.return_value = "any name"
        log_issue(mock_logger, QA_LEVEL.INFO, issue)
        mock_logger.add_issue.assert_called()
        mock_logger.data_info.assert_not_called()

        # Issue with id. Should not add issue, but should log it
        mock_logger.reset_mock()
        issue = Issue({'id': 'any_check', 'msg': 'any msg'}, {}, 'id', 'attr')
        log_issue(mock_logger, QA_LEVEL.INFO, issue)
        mock_logger.add_issue.assert_not_called()
        mock_logger.data_info.assert_called()

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

    @patch("gobcore.quality.issue.QualityUpdate")
    @patch("gobcore.quality.issue.logger")
    @patch("gobcore.quality.issue.is_functional_process")
    @patch("gobcore.quality.issue._start_issue_workflow")
    def test_process_issues(self, mock_start_issue_workflow, mock_is_functional, mock_logger, mock_quality_update):
        mock_issue = MagicMock()

        mock_quality_update.CATALOG = 'qa'

        msg = {
            'header': {
                'source': 'any source',
                'application': 'any application',
                'process_id': 'the original process id',
                'catalogue': 'any catalogue',
                'collection': 'any collection',
                'attribute': 'any attribute',
                'other': 'any other',
                'mode': 'any mode'
            }
        }

        # Skip issues for unnamed process steps
        mock_logger.get_name.return_value = None
        process_issues(msg)
        mock_start_issue_workflow.assert_not_called()

        mock_logger.get_name.return_value = "any name"

        # Skip empty issues for non-functional process steps
        mock_logger.get_issues.return_value = []
        mock_is_functional.return_value = False
        process_issues(msg)
        mock_start_issue_workflow.assert_not_called()

        # Always skip any issues of the QA catalog itself
        mock_logger.get_issues.return_value = [mock_issue]
        mock_is_functional.return_value = True
        msg['header']['catalogue'] = 'qa'
        process_issues(msg)
        mock_start_issue_workflow.assert_not_called()

        # Skip GOBPrepare
        mock_logger.get_issues.return_value = [mock_issue]
        mock_is_functional.return_value = True
        msg['header']['catalogue'] = 'any catalogue'
        msg['header']['application'] = 'GOBPrepare'
        process_issues(msg)
        mock_start_issue_workflow.assert_not_called()

        # Skip when no collection is present
        mock_logger.get_issues.return_value = [mock_issue]
        mock_is_functional.return_value = True
        msg['header']['application'] = 'any application'
        del msg['header']['collection']
        process_issues(msg)
        mock_start_issue_workflow.assert_not_called()

        # Non-functional processes might report issues
        # In that case they will not be skipped and handled as regular issues
        mock_logger.get_issues.return_value = [mock_issue]
        mock_is_functional.return_value = False
        msg['header']['collection'] = 'any collection'
        process_issues(msg)
        mock_start_issue_workflow.assert_called()
        mock_start_issue_workflow.reset_mock()

        # Do not skip empty issues for functional process steps
        mock_logger.get_issues.return_value = []
        mock_is_functional.return_value = True
        process_issues(msg)
        mock_start_issue_workflow.assert_called()
        mock_start_issue_workflow.reset_mock()

        # The regular case is a functional process step that has any issues to report
        mock_logger.get_issues.return_value = [mock_issue]
        mock_is_functional.return_value = True
        process_issues(msg)
        mock_start_issue_workflow.assert_called_with(msg.get('header'), [mock_issue], mock_quality_update.return_value)

    def test_get_validity(self):
        entity = {
            'id': 'any id',
            'validity': '2020-05-22'
        }
        issue = Issue({'id': 'any_check'}, entity, 'id', 'validity')
        self.assertEqual(issue._get_validity(entity, 'validity'), '2020-05-22T00:00:00')

        entity['validity'] = datetime.date(year=1020, month=5, day=22)
        self.assertEqual(issue._get_validity(entity, 'validity'), '1020-05-22T00:00:00')

        # Conversion fails, set to None
        entity['validity'] = 'non date'
        self.assertEqual(issue._get_validity(entity, 'validity'), None)

    @patch('gobcore.quality.issue.Issue')
    @patch('gobcore.quality.issue.start_workflow')
    @patch('gobcore.quality.issue.ContentsWriter', MagicMock())
    @patch("gobcore.quality.issue.ProgressTicker", MagicMock())
    def test_start_issue_workflow(self, mock_start_workflow, mock_issue):
        header = {
            'catalogue': 'qa',
            'collection': 'any collection',
        }
        issues = [{'id': 'issue 1'}, {'id': 'issue 2'}]
        quality_update = MagicMock()

        _start_issue_workflow(header, issues, quality_update)
        
        mock_start_workflow.assert_called()

    @patch('builtins.print')
    @patch('gobcore.quality.issue.os.remove')
    def test_remove_empty_offloading_files(self, mock_remove, mock_print):
        header = {
            'catalogue': 'qa',
            'collection': 'any collection',
        }
        issues = []
        quality_update = MagicMock()

        _start_issue_workflow(header, issues, quality_update)
        self.assertTrue(mock_remove.called)

        mock_remove.side_effect = Exception("any exception")
        _start_issue_workflow(header, issues, quality_update)

        self.assertEqual(mock_print.call_args[0][0], "Remove failed (any exception)")

    def test_state_attributes(self):
        entity = {
            'id': 'any id',
            'attr': 'any attr',
            FIELD.SEQNR: 'any seqnr',
            FIELD.START_VALIDITY: '2006-01-20',
            FIELD.END_VALIDITY: '2006-01-20 12:35'
        }
        issue = Issue({'id': 'any_check', 'msg': 'any msg'}, entity, 'id', 'attr')

        self.assertEqual(getattr(issue, FIELD.SEQNR), entity[FIELD.SEQNR])
        self.assertEqual(getattr(issue, FIELD.START_VALIDITY), '2006-01-20T00:00:00')
        self.assertEqual(getattr(issue, FIELD.END_VALIDITY), '2006-01-20T12:35:00')

        for v in [datetime.date(2020, 1, 20), datetime.datetime(2020, 1, 20), '20200120', '2020-01-20']:
            entity[FIELD.START_VALIDITY] = v
            issue = Issue({'id': 'any_check', 'msg': 'any msg'}, entity, 'id', 'attr')
            self.assertEqual(getattr(issue, FIELD.START_VALIDITY), '2020-01-20T00:00:00')

        entity = {
            'id': 'any id',
            'attr': 'any attr'
        }
        issue = Issue({'id': 'any_check', 'msg': 'any msg'}, entity, 'id', 'attr')
        for attr in [FIELD.SEQNR, FIELD.START_VALIDITY, FIELD.END_VALIDITY]:
            self.assertEqual(getattr(issue, attr), None)

    def test_is_functional_process(self):
        for process in ['aap', 'noot', '']:
            self.assertFalse(is_functional_process(process))

        for process in ['Import', 'import', 'ImporT', 'IMPORT', 'relate_check']:
            self.assertTrue(is_functional_process(process))
