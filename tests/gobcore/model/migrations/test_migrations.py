import unittest
from unittest.mock import patch, MagicMock

from gobcore.exceptions import GOBException
from gobcore.model.migrations import GOBMigrations


class MockEvent():

    def __init__(self, action, version):
        self.action = action
        self.version = version


class TestMigrations(unittest.TestCase):

    def setUp(self):
        self.migrations = GOBMigrations()
        self.add_event = MockEvent('ADD', '0.1')
        self.mock_migration = {
            'target_version': '0.2',
            'conversions': [
                {
                    'action': 'rename',
                    'old_column': 'old',
                    'new_column': 'new'
                },
                {
                    'action': 'delete',
                    'column': 'deleted_column'
                },
                {
                    'action': 'add',
                    'column': 'added_column',
                    'default': 'default value'
                }
            ]
        }

    @patch('gobcore.model.migrations.GOBMigrations._extract_migrations')
    def test_initialize_model_once(self, mock_extract_migrations):
        # Model has already been initialised, _extract_migrations should not be called again
        migrations = GOBMigrations()
        mock_extract_migrations.assert_not_called()

    def test_get_migration(self):
        # Assert we get a list of relations for a collection
        self.assertIsInstance(self.migrations._get_migration('brk', 'kadastralesecties', '0.1'), dict)

    def test_get_migration_keyerror(self):
        # Assert an empty list is returned if no migration is found
        self.assertEqual(self.migrations._get_migration('catalog', 'collection', '0.1'), None)

    def test_apply_migration_add(self):
        # Test if a column has been renamed in event data
        data = {
            'entity': {
                'old': 'value',
                'deleted_column': 'some value',
            }
        }

        expected_data = {
            'entity': {
                'new': 'value',
                'added_column': 'default value',
            }
        }

        result = self.migrations._apply_migration(self.add_event, data, self.mock_migration)

        self.assertEqual(result, expected_data)
        self.assertEqual(self.add_event.version, '0.2')

    def test_apply_migration_modify(self):
        # Test if a column has been renamed in event data
        modify_event = MockEvent('MODIFY', '0.1')

        data = {
            'modifications': [
                {
                    'key': 'old',
                    'old_value': 'test',
                    'new_value': 'modify',
                },
                {
                    'key': 'other_key',
                    'old_value': 'test',
                    'new_value': 'modify'
                },
                {
                    'key': 'deleted_column',
                    'old_value': 'test',
                    'new_value': 'modify',
                }
            ]
        }

        expected_data = {
            'modifications': [
                {
                    'key': 'new',
                    'old_value': 'test',
                    'new_value': 'modify'
                },
                {
                    'key': 'other_key',
                    'old_value': 'test',
                    'new_value': 'modify'
                },
            ]
        }

        result = self.migrations._apply_migration(modify_event, data, self.mock_migration)

        self.assertEqual(result, expected_data)
        self.assertEqual(modify_event.version, '0.2')

    def test_apply_migration_not_implemented(self):
        # Assert migration fails for notimplemented action
        not_implemented_mock_migration = {
            'target_version': '0.2',
            'conversions': [
                {
                    'action': 'non-existent',
                }
            ]
        }

        data = {}

        with self.assertRaises(NotImplementedError):
            result = self.migrations._apply_migration(self.add_event, data, not_implemented_mock_migration)

    @patch('gobcore.model.migrations.logger', MagicMock())
    @patch('gobcore.model.migrations.GOBMigrations._get_migration')
    def test_migrate_event_data(self, mock_get_migration):
        mock_get_migration.return_value = self.mock_migration

        data = {
            'entity': {
                'old': 'value',
            }
        }

        expected_data = {
            'entity': {
                'new': 'value',
                'added_column': 'default value',
            }
        }

        result = self.migrations.migrate_event_data(self.add_event, data, 'catalog', 'collection', '0.2')

        self.assertEqual(result, expected_data)


    @patch('gobcore.model.migrations.logger', MagicMock())
    @patch('gobcore.model.migrations.GOBMigrations._get_migration')
    def test_migrate_event_data_multiple(self, mock_get_migration):
        mock_migration2 = {
            'target_version': '0.3',
            'conversions': [
                {
                    'action': 'rename',
                    'old_column': 'new',
                    'new_column': 'extra_new'
                }
            ]
        }

        mock_get_migration.side_effect = [self.mock_migration, mock_migration2]

        data = {
            'entity': {
                'old': 'value',
            }
        }

        expected_data = {
            'entity': {
                'extra_new': 'value',
                'added_column': 'default value',
            }
        }

        result = self.migrations.migrate_event_data(self.add_event, data, 'catalog', 'collection', '0.3')

        self.assertEqual(result, expected_data)

    @patch('gobcore.model.migrations.GOBMigrations._get_migration')
    def test_split_json_add(self, mock_get_migration):
        add_event = MockEvent('ADD', '0.1')
        data = {
            'entity': {
                'some_col': 'ABC',
                'new_code_column': '456willbeoverwritten',
                'json_col': {
                    'code': '123',
                    'omsc': 'La die la'
                }
            }
        }

        expected = {
            'entity': {
                'some_col': 'ABC',
                'json_col': {
                    'code': '123',
                    'omsc': 'La die la'
                },
                'new_code_column': '123',
                'new_omsc_column': 'La die la'
            }
        }

        mock_get_migration.side_effect = [{
            'target_version': '2.0',
            'conversions': [{
                'column': 'json_col',
                'action': 'split_json',
                'mapping': {
                    'new_code_column': 'code',
                    'new_omsc_column': 'omsc'
                }
            }]
        }]

        self.migrations.migrate_event_data(add_event, data, 'catalog', 'collection', '2.0')

        self.assertEqual(data, expected)

    @patch('gobcore.model.migrations.GOBMigrations._get_migration')
    def test_split_json_modify(self, mock_get_migration):
        modify_event = MockEvent('MODIFY', '0.1')

        data = {
            'modifications': [
                {
                    'key': 'json_col',
                    'old_value': {
                        'code': 123,
                        'omsc': 'La die la'
                    },
                    'new_value': {
                        'code': 234,
                        'omsc': 'La die la la la'
                    },
                },
            ]
        }

        expected = {
            'modifications': [
                {
                    'key': 'json_col',
                    'old_value': {
                        'code': 123,
                        'omsc': 'La die la'
                    },
                    'new_value': {
                        'code': 234,
                        'omsc': 'La die la la la'
                    },
                },
                {
                    'key': 'new_code_column',
                    'old_value': 123,
                    'new_value': 234
                },
                {
                    'key': 'new_omsc_column',
                    'old_value': 'La die la',
                    'new_value': 'La die la la la'
                }
            ]
        }

        mock_get_migration.side_effect = [{
            'target_version': '2.0',
            'conversions': [{
                'column': 'json_col',
                'action': 'split_json',
                'mapping': {
                    'new_code_column': 'code',
                    'new_omsc_column': 'omsc'
                }
            }]
        }]

        self.migrations.migrate_event_data(modify_event, data, 'catalog', 'collection', '2.0')

        self.assertEqual(data, expected)

    @patch('gobcore.model.migrations.logger')
    @patch('gobcore.model.migrations.GOBMigrations._get_migration')
    def test_migrate_event_data_missing_migration(self, mock_get_migration, mock_logger):
        mock_get_migration.side_effect = [self.mock_migration, None]

        data = {
            'entity': {
                'old': 'value',
            }
        }

        with self.assertRaises(GOBException):
            self.migrations.migrate_event_data(self.add_event, data, 'catalog', 'collection', '0.3')
            mock_logger.assert_called()
