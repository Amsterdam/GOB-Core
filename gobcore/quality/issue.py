import datetime
from dateutil import parser
import json
import os

from gobcore.message_broker.config import IMPORT, RELATE, RELATE_CHECK
from gobcore.message_broker.offline_contents import ContentsWriter
from gobcore.message_broker.utils import to_json
from gobcore.model import FIELD
from gobcore.logging.logger import Logger, logger
from gobcore.quality.config import QA_LEVEL, QA_CHECK
from gobcore.quality.quality_update import QualityUpdate
from gobcore.utils import ProgressTicker
from gobcore.workflow.start_workflow import start_workflow


class IssueException(Exception):
    pass


class Issue():
    """
    Data issue class

    """
    _DEFAULT_ENTITY_ID = 'identificatie'
    _NO_VALUE = '<<NO VALUE>>'

    def __init__(self, check: dict, entity: dict, id_attribute: str, attribute: str,
                 compared_to: str = None, compared_to_value=None):
        """
        Initialises an Issue

        1. Bind issue to a Quality Assurance Check
        An issue has to be related to a check.
        Checks are defined in the QA_CHECK class.
        The first step is to find the corresponding check for the issue
        The issue is so bound to QA check

        2. Bind issue to a data entity
        An issue is about an entity
        The entity is identified by an id.
        The attribute of the id is the id_attribute parameter: entity[id_attribute]
        The default attribute for for the id is 'identificatie': entity[identificatie]
        The issue is so bound to an entity and its id, sequence number and validity dates are registered in the issue.

        3. Bind issue to a data entity attribute
        An issue is about a specific attribute of the data entity
        The name of the attribute is specified by the attribute parameter
        The value to be reported by the issue will be entity[attribute]

        4. Optional: Relate the issue to another value
        An issue can relate to another value, for example when a data is expected to lie after another date
        In this case the name of the attribute to which the data is compared and optionally its value
        can be specified in the compared_to and compared_to_value attribute.
        If no compared_to_value is specified the value is retrieved from the entity: entity[compared_to]

        Result

        The Issue describes a Quality Issue where:
        - it is related to a predefined QA Check
        - it is related to an entity by id, sequence nr, start- and end-validity
        - it contains the name and value of the failing attribute
        - optionally it contains the name and value of the attribute to which it has been compared

        :param dict check: The dictionary that specifies the QA check that has failed
        :param dict entity: The dictionary that contains the entity that failed to pass a QA check
        :param str id_attribute: The name of the entity ID attribute
        :param str attribute: The name of the entity attribute that caused the failure
        :param str compared_to: Optional, name of the attribute to which the issues relates
        :param compared_to_value: Optional, name of the attribute to which the issues relates
        """
        if not hasattr(QA_CHECK, check.get('id') or ""):
            raise IssueException(f"Issue reported for undefined check: {check}")
        self.check = check
        self.check_id = check['id']

        self.entity = entity

        # Entity id and sequence number
        self.entity_id_attribute = id_attribute or self._DEFAULT_ENTITY_ID
        self.entity_id = self._get_value(entity, self.entity_id_attribute)
        setattr(self, FIELD.SEQNR, self._get_value(entity, FIELD.SEQNR))

        # Entity start- and end-validity
        for attr in [FIELD.START_VALIDITY, FIELD.END_VALIDITY]:
            setattr(self, attr, self._get_validity(entity, attr))

        # Concerned attribute and value
        self.attribute = attribute
        self._values = [self._get_value(entity, attribute)]

        # Any concerned other attribute
        self.compared_to = compared_to
        self.compared_to_value = compared_to_value
        if compared_to and compared_to_value is None:
            # Default other attribute value is get its value from the entity itself
            self.compared_to_value = self._get_value(entity, self.compared_to)

        self.explanation = None

    def get_unique_id(self):
        # An issue if uniquely identified by the id of the failing check, the concerned attribute
        # and the entity identification
        return "_".join([str(value) for value in [
            self.check_id,
            self.attribute,
            self.entity_id,
            getattr(self, FIELD.SEQNR)
        ] if value])

    def join_issue(self, other_issue):
        if self.get_unique_id() != other_issue.get_unique_id():
            raise IssueException(f"Join issue requires same ID {self.get_unique_id} <> {other_issue.get_unique_id()}")
        if other_issue.value not in self._values:
            self._values.append(other_issue.value)

    @property
    def value(self):
        if len(self._values) > 1:
            return ", ".join(sorted([self._format_value(value) for value in self._values]))
        else:
            return self._values[0]

    def _get_validity(self, entity: dict, attribute: str):
        """
        Get a validity datetime

        If the value is a string then parse it as a date-time
        Use _get_value to convert the result in a regular attribute value

        :param entity:
        :param attribute:
        :return:
        """
        value = entity.get(attribute)
        try:
            if isinstance(value, str):
                value = parser.parse(value)
            elif isinstance(value, datetime.date):
                value = datetime.datetime.combine(value, datetime.datetime.min.time())
        except ValueError:
            # Validity is erroneous; set value to None
            value = None
        return self._get_value({attribute: value}, attribute)

    def _get_value(self, entity: dict, attribute: str):
        """
        Gets the value of an entity attribute

        :param entity:
        :param attribute:
        :return:
        """
        # Allow None values
        value = entity.get(attribute)
        if isinstance(value, datetime.date):
            # Dates are iso formatted
            value = value.isoformat()
        return value

    def _format_value(self, value) -> str:
        """
        Returns the formatted value.
        Explicitly format None values

        :param value:
        :return:
        """
        return self._NO_VALUE if value is None else str(value)

    def get_explanation(self) -> str:
        if self.explanation:
            return self.explanation
        elif self.compared_to:
            return f"{self.compared_to} = {self.compared_to_value}"

    def msg(self) -> str:
        """
        Return a message that describes the issue at a general level.
        No entity values are included in the message, only attribute names and static strings

        :return:
        """
        msg = f"{self.attribute}: {self.check['msg']}"
        if self.compared_to:
            msg += f" {self.compared_to}"
        return msg

    @property
    def json(self) -> str:
        """
        Return a json representation of the issue to offload to file

        :return:
        """
        # Only store the mimial required fields for the entity
        json_entity = {
            self.entity_id_attribute: self.entity_id,
            FIELD.SEQNR: getattr(self, FIELD.SEQNR),
            FIELD.START_VALIDITY: getattr(self, FIELD.START_VALIDITY),
            FIELD.END_VALIDITY: getattr(self, FIELD.END_VALIDITY),
            self.attribute: self.value
        }

        json_obj = {
            "check": self.check,
            "entity": json_entity,
            "id_attribute": self.entity_id_attribute,
            "attribute": self.attribute,
            "compared_to": self.compared_to,
            "compared_to_value": self.compared_to_value
        }
        return to_json(json_obj)

    @classmethod
    def from_json(cls, json_entity):
        entity = json.loads(json_entity)
        return cls(**entity)

    def log_args(self, **kwargs) -> dict:
        """
        Convert the issue into arguments that are suitable to add in a log message

        :param kwargs:
        :return:
        """
        args = {
            'id': self.msg(),
            'data': {
                self.entity_id_attribute: self.entity_id,
                FIELD.SEQNR: getattr(self, FIELD.SEQNR),
                self.attribute: self._format_value(self.value),
                **({self.compared_to: self._format_value(self.compared_to_value)} if self.compared_to else {}),
                **kwargs
            }
        }
        if self.compared_to:
            args['data'][self.compared_to] = self._format_value(self.compared_to_value)
        return args


def log_issue(logger: Logger, level: QA_LEVEL, issue: Issue) -> None:
    """Logs Issue

    Only issues without an entity_id are actually written as log messages. All other issues are added
    to the logger instance for further handling (they will be picked up by the process_issues function in
    this file)

    :param logger:
    :param level:
    :param issue:
    :return:
    """

    if issue.entity_id is not None:
        # Only add issues that are linked to entities
        logger.add_issue(issue, level)
    else:
        # Log the message
        {
            QA_LEVEL.FATAL: logger.data_error,
            QA_LEVEL.ERROR: logger.data_error,
            QA_LEVEL.WARNING: logger.data_warning,
            QA_LEVEL.INFO: logger.data_info
        }[level](issue.msg(), issue.log_args())


def is_functional_process(process):
    """
    A functional process is to import or check relations
    Other process steps like compare, apply and store events are considered technical process steps

    :param process:
    :return:
    """
    functional_processes = [process.lower() for process in [IMPORT, RELATE, RELATE_CHECK]]
    return process.lower() in functional_processes


def process_issues(msg):
    issues = logger.get_issues()

    header = msg.get('header', {})

    # Issues are processed as updates to the quality catalog
    quality_update = QualityUpdate()

    # Enrich bevinding with logger info
    quality_update.proces = logger.get_name()

    # Don't process issues of unnamed process
    if quality_update.proces is None:
        return

    # Enrich bevinding with msg info
    for attribute in ['source', 'application', 'catalogue', 'collection', 'attribute']:
        setattr(quality_update, attribute, header.get(f"original_{attribute}", header.get(attribute)))

    # Only log quality for functional steps
    # Otherwise only log issues if there are any issues
    if not (is_functional_process(quality_update.proces) or issues):
        # Functional process issues are always processed, even if they are empty
        # Otherwise a check is made if there are any issues, if not then skip the empty set
        # So even an non-functional process may report Issues
        return

    # Skip GOBPrepare jobs
    # Don't Quality check yourself
    # Don't check jobs where no collection is present
    if quality_update.application == 'GOBPrepare' \
            or quality_update.catalogue == QualityUpdate.CATALOG \
            or quality_update.collection is None:
        return

    _start_issue_workflow(header, issues, quality_update)

    logger.clear_issues()


def _start_issue_workflow(header, issues, quality_update):
    catalogue = header.get('catalogue')
    collection = header.get('collection')

    with ContentsWriter() as writer, \
            ProgressTicker(f"Process issues {catalogue} {collection}", 10000) as progress:

        num_records = 0
        filename = writer.filename

        for json_issue in issues:
            progress.tick()
            issue = Issue(**json_issue)
            writer.write(quality_update.get_contents(issue))
            num_records += 1

        if num_records:
            # Start workflow, allow retries when an identical workflow is already running for max_retry_time seconds
            workflow = {
                'workflow_name': "import",
                'step_name': "update_model",
                'retry_time': 10 * 60  # retry for max 10 minutes
            }

            wf_msg = {
                'header': quality_update.get_header(header),
                'contents_ref': filename,
                'summary': {
                    'num_records': num_records
                }
            }

            start_workflow(workflow, wf_msg)

    if not num_records:
        # no offloading, delete file
        os.remove(filename)
