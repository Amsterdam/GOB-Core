import datetime

from gobcore.model import FIELD
from gobcore.quality.config import QA_LEVEL, QA_CHECK
from gobcore.quality.quality_update import QualityUpdate
from gobcore.logging.logger import Logger, logger
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
                 compared_to: str=None, compared_to_value=None):
        if not hasattr(QA_CHECK, check.get('id') or ""):
            raise IssueException(f"Issue reported for undefined check: {check}")
        self.check = check
        self.check_id = check['id']

        # Entity id and sequence number
        self.entity_id_attribute = id_attribute or self._DEFAULT_ENTITY_ID
        self.entity_id = self._get_value(entity, self.entity_id_attribute)
        setattr(self, FIELD.SEQNR, self._get_value(entity, FIELD.SEQNR))

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
        self._values.append(other_issue.value)

    @property
    def value(self):
        if len(self._values) > 1:
            return ", ".join(sorted([self._format_value(value) for value in self._values]))
        else:
            return self._values[0]

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
    # Log the message
    {
        QA_LEVEL.FATAL: logger.error,
        QA_LEVEL.ERROR: logger.error,
        QA_LEVEL.WARNING: logger.warning,
        QA_LEVEL.INFO: logger.info
    }[level](issue.msg(), issue.log_args())

    if issue.entity_id is not None:
        # Only add issues that are linked to entities
        logger.add_issue(issue)


def process_issues(msg):
    issues = logger.get_issues()
    if not issues:
        return

    # Issues are processed as updates to the quality catalog
    quality_update = QualityUpdate(issues)

    # Enrich bevinding with logger info
    quality_update.proces = logger.get_name()

    # Enrich bevinding with msg info
    header = msg.get('header', {})
    for attribute in ['source', 'application', 'catalogue', 'collection', 'attribute']:
        setattr(quality_update, attribute, header.get(f"original_{attribute}", header.get(attribute)))

    workflow = {
        'workflow_name': "import",
        'step_name': "update_model"
    }
    wf_msg = quality_update.get_msg(msg)
    start_workflow(workflow, wf_msg)

    logger.clear_issues()
