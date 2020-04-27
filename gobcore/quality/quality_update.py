import datetime

from gobcore.model import FIELD
from gobcore.model.quality import QUALITY_CATALOG, get_entity_name


class QualityUpdate():

    CATALOG = QUALITY_CATALOG

    def __init__(self, issues):
        self.issues = issues if isinstance(issues, list) else [issues]
        self.source = None
        self.application = None
        self.catalogue = None
        self.collection = None
        self.attribute = None
        self.proces = None

    def _combined_key(self, values):
        return "_".join([str(value) for value in values if value is not None])

    def get_unique_id(self, issue):
        """
        Each qa issue is uniquely identified by the error, attribute and entity identification
        :param issue:
        :return:
        """
        return self._combined_key([
            issue.check_id,
            issue.attribute,
            issue.entity_id,
            getattr(issue, FIELD.SEQNR)])

    def get_source(self):
        return self._combined_key([
            self.proces,
            self.source,
            self.application,
            self.attribute])

    def get_header(self, msg_header):
        header = {
            'catalogue': QUALITY_CATALOG,
            'entity': get_entity_name(self.catalogue, self.collection),
            'collection': get_entity_name(self.catalogue, self.collection),
            'source': self.get_source(),
            'application': self.application,
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'version': '0.1'
        }
        if msg_header.get('mode'):
            header['mode'] = msg_header['mode']
        return header

    def get_contents(self, issue):
        unique_id = self.get_unique_id(issue)
        return {
            '_source_id': unique_id,
            'meldingnummer': unique_id,
            'code': issue.check_id,
            'proces': self.proces,
            'attribuut': issue.attribute,
            'identificatie': issue.entity_id,
            'volgnummer': getattr(issue, FIELD.SEQNR),
            'betwijfelde_waarde': str(issue.value),
            'onderbouwing': issue.get_explanation(),
            'voorgestelde_waarde': None
        }

    def get_msg(self, msg):
        msg_header = msg.get('header', {})

        return {
            'header': self.get_header(msg_header),
            'contents': [self.get_contents(issue) for issue in self.issues],
            'summary': {
                'num_records': len(self.issues)
            }
        }
