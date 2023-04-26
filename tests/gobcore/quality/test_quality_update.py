from unittest import TestCase
from unittest.mock import MagicMock

from gobcore.quality.issue import Issue
from gobcore.quality.quality_update import QualityUpdate
from gobcore.typesystem.gob_types import Decimal


class TestQualityUpdate(TestCase):

    def test_get_contents(self):
        qa = QualityUpdate()

        mock_issue = MagicMock()
        for value in 1, True, 2.5, "any value", Decimal(1.5):
            mock_issue.value = value
            contents = qa.get_contents(mock_issue)
            self.assertEqual(contents['betwijfelde_waarde'], str(value))

        header = {
            'source': 'any source',
            'application': 'any application',
            'catalogue': 'any catalogue',
            'collection': 'any collection',
            'attribute': 'any attribute',
            'mode': 'any mode',
        }

        # Enrich bevinding with msg info
        for attribute in ['source', 'application', 'catalogue', 'collection', 'attribute']:
            setattr(qa, attribute, header.get(f"original_{attribute}", header.get(attribute)))

        self.assertEqual("any source_any application_any attribute", qa.get_source())

        qa_header = qa.get_header(header)
        self.assertEqual(qa_header['catalogue'], 'qa')
        self.assertEqual(qa_header['collection'], 'any catalogue_any collection')

    def test_from_header(self):
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

        qa = QualityUpdate.from_msg(msg)

        assert qa.source == "any source"
        assert qa.application == "any application"
        assert qa.catalogue == "any catalogue"
        assert qa.collection == "any collection"
        assert qa.attribute == "any attribute"
        assert qa.process is None

    def test_from_msg_extract_header(self):
        msg = {'header': {'original_source': 'source', 'original_application': 'app', 'catalogue': 'cat',
                          'collection': 'col', 'original_attribute': 'attr'}}
        qa = QualityUpdate.from_msg(msg)
        assert qa.source == 'source'
        assert qa.application == 'app'
        assert qa.catalogue == 'cat'
        assert qa.collection == 'col'
        assert qa.attribute == 'attr'

    def test_from_msg_empty_header(self):
        msg = {}
        qa = QualityUpdate.from_msg(msg)
        assert qa.source is None
        assert qa.application is None
        assert qa.catalogue is None
        assert qa.collection is None
        assert qa.attribute is None

    def test_from_msg_missing_fields(self):
        msg = {'header': {'original_source': 'source'}}
        qa = QualityUpdate.from_msg(msg)
        assert qa.source == 'source'
        assert qa.application is None
        assert qa.catalogue is None
        assert qa.collection is None
        assert qa.attribute is None

    def test_get_unique_id(self):
        # issue = {'check_id': 'check', 'attribute': 'attr', 'entity_id': 'entity', 'seqnr': 1}
        issue = Issue(
            check={'id': 'Attribute_exists'},
            entity={'entity_id': "my_id", "volgnummer": 1},
            id_attribute='entity_id',
            attribute='attr'
        )
        qa = QualityUpdate()
        qa.process = 'process'
        unique_id = qa.get_unique_id(issue)
        assert unique_id == 'process.Attribute_exists.attr.my_id.1'

    def test_get_source(self):
        qa = QualityUpdate()
        qa.process = 'process'
        qa.source = 'source'
        qa.application = 'app'
        qa.attribute = 'attr'
        source = qa.get_source()
        assert source == 'process_source_app_attr'
