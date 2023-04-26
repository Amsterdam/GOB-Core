import datetime

from gobcore.model import FIELD
from gobcore.model.quality import QUALITY_CATALOG, get_entity_name

from typing import TYPE_CHECKING, Iterable, Any

if TYPE_CHECKING:
    from gobcore.quality.issue import Issue
    from gobcore.message_broker.typing import Message, Header


class QualityUpdate:

    CATALOG = QUALITY_CATALOG

    def __init__(self):
        self.source = None
        self.application = None
        self.catalogue = None
        self.collection = None
        self.attribute = None
        self.process = None

    @classmethod
    def from_msg(cls, msg: "Message"):
        header = msg.get("header", {})
        qa = QualityUpdate()
        for attribute in ['source', 'application', 'catalogue', 'collection', 'attribute']:
            setattr(qa, attribute, header.get(f"original_{attribute}", header.get(attribute)))
        return qa

    @staticmethod
    def _combined_key(values: Iterable, join='_') -> str:
        return join.join([str(value).strip() for value in values if value is not None])

    def get_unique_id(self, issue: "Issue") -> str:
        """
        Each qa issue is uniquely identified by the error, attribute and entity identification
        :param issue:
        :return:
        """
        return self._combined_key([
            self.process,
            issue.check_id,
            issue.attribute,
            issue.entity_id,
            getattr(issue, FIELD.SEQNR)
        ], '.')

    def get_source(self) -> str:
        return self._combined_key([
            self.process,
            self.source,
            self.application,
            self.attribute])

    def get_header(self, msg_header: "Header") -> "Header":
        header = {
            'catalogue': QUALITY_CATALOG,
            'entity': get_entity_name(self.catalogue, self.collection),
            'collection': get_entity_name(self.catalogue, self.collection),
            'source': self.get_source(),
            'application': self.application,
            'process_id': msg_header.get('process_id'),
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'version': '0.1'
        }
        if msg_header.get('mode'):
            header['mode'] = msg_header['mode']
        return header

    def get_contents(self, issue: "Issue") -> dict[str, Any]:
        unique_id = self.get_unique_id(issue)
        return {
            '_source_id': unique_id,
            'meldingnummer': unique_id,
            'code': issue.check_id,
            'proces': self.process,
            'attribuut': issue.attribute,
            'identificatie': issue.entity_id,
            'volgnummer': getattr(issue, FIELD.SEQNR),
            'begin_geldigheid': getattr(issue, FIELD.START_VALIDITY),
            'eind_geldigheid': getattr(issue, FIELD.END_VALIDITY),
            'betwijfelde_waarde': str(issue.value),
            'onderbouwing': issue.get_explanation(),
            'voorgestelde_waarde': None
        }
