import unittest

from gobcore.model import metadata


class TestModel(unittest.TestCase):

    def test_metadata_columns(self):
        self.assertIn('public', metadata.METADATA_COLUMNS)
        self.assertIn('private', metadata.METADATA_COLUMNS)

    def test_fixed_columns(self):
        self.assertIn('_gobid', metadata.FIXED_COLUMNS)
        self.assertIn('_id', metadata.FIXED_COLUMNS)

    def test_private_meta_fields(self):
        self.assertIn('_source', metadata.PRIVATE_META_FIELDS)
        self.assertIn('_source_id', metadata.PRIVATE_META_FIELDS)
        self.assertIn('_last_event', metadata.PRIVATE_META_FIELDS)

    def test_public_meta_fields(self):
        self.assertIn('_version', metadata.PUBLIC_META_FIELDS)
        self.assertIn('_date_created', metadata.PUBLIC_META_FIELDS)
        self.assertIn('_date_confirmed', metadata.PUBLIC_META_FIELDS)
        self.assertIn('_date_modified', metadata.PUBLIC_META_FIELDS)
        self.assertIn('_date_deleted', metadata.PUBLIC_META_FIELDS)
