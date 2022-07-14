import unittest
from unittest.mock import patch

from gobcore.model import GOBModel
from gobcore.model.sa.gob import get_sqlalchemy_models, columns_to_model


class TestGob(unittest.TestCase):

    def setUp(self):
        pass

    def test_model(self):
        models = get_sqlalchemy_models(GOBModel())
        for (name, cls) in models.items():
            m = cls()
            self.assertEqual(str(m), name)

    class MockModel:
        collections = {
            'col1': {
                'name': 'collection 1',
                'very_many_references': {'very_many_reference': {}}
            },
            'col2': {
                'name': 'collection 2',
                'very_many_references': {}
            }
        }
        catalogs = {
            'cat1': {
                'name': 'catalog 1',
            },
            'cat2': {
                'name': 'catalog 2',
            }
        }

        def get_catalog_collection_from_abbr(self, cat_abbr, col_abbr):
            return self.catalogs[cat_abbr], self.collections[col_abbr]

        def get_table_name(self, catalog_name, collection_name):
            return f"{catalog_name}_{collection_name}"

    class MockForeignKeyConstraint:

        def __init__(self, src_cols, dst_cols, name):
            self.src_cols = src_cols
            self.dst_cols = dst_cols
            self.name = name

        def __str__(self):
            return self.name

        def __eq__(self, other):
            return self.name == other.name

    @patch("gobcore.model.sa.gob.ForeignKeyConstraint", MockForeignKeyConstraint)
    @patch("gobcore.model.sa.gob.NameCompressor.compress_name", lambda x: x)
    @patch("gobcore.model.sa.gob.get_column")
    @patch("gobcore.model.sa.gob.split_relation_table_name")
    @patch("gobcore.model.sa.gob._create_model_type")
    @patch("gobcore.model.sa.gob.UniqueConstraint")
    def test_columns_to_model_rel(self, mock_unique, mock_model_type, mock_split, mock_get_column):
        mock_get_column.side_effect = lambda name, spec: spec
        mock_split.return_value = {
            'src_cat_abbr': 'cat1',
            'src_col_abbr': 'col1',
            'dst_cat_abbr': 'cat2',
            'dst_col_abbr': 'col2',
            'reference_name': 'reference'
        }

        self.assertEqual(mock_model_type.return_value, columns_to_model(self.MockModel(), 'rel', 'table_name', {
            'column1': 'column1spec',
            'column2': 'column2spec',
        }))

        mock_model_type.assert_called_with('table_name', {
            'column1': 'column1spec',
            'column2': 'column2spec',
        }, False, (
            self.MockForeignKeyConstraint('', '', 'table_name_sfk'),
            self.MockForeignKeyConstraint('', '', 'table_name_dfk'),
            mock_unique.return_value,
            mock_unique.return_value,
        ))

    @patch("gobcore.model.sa.gob.ForeignKeyConstraint", MockForeignKeyConstraint)
    @patch("gobcore.model.sa.gob.NameCompressor.compress_name", lambda x: x)
    @patch("gobcore.model.sa.gob.get_column")
    @patch("gobcore.model.sa.gob.split_relation_table_name")
    @patch("gobcore.model.sa.gob._create_model_type")
    @patch("gobcore.model.sa.gob.UniqueConstraint")
    def test_columns_to_model_rel_vmr(self, mock_unique, mock_model_type, mock_split, mock_get_column):
        mock_get_column.side_effect = lambda name, spec: spec
        mock_split.return_value = {
            'src_cat_abbr': 'cat1',
            'src_col_abbr': 'col1',
            'dst_cat_abbr': 'cat2',
            'dst_col_abbr': 'col2',
            'reference_name': 'very_many_reference'
        }

        self.assertEqual(mock_model_type.return_value, columns_to_model(self.MockModel(), 'rel', 'table_name', {
            'column1': 'column1spec',
            'column2': 'column2spec',
        }))

        mock_model_type.assert_called_with('table_name', {
            'column1': 'column1spec',
            'column2': 'column2spec',
        }, False, (mock_unique.return_value, mock_unique.return_value,))
