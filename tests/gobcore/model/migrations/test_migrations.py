import unittest
from unittest.mock import patch, MagicMock

from gobcore.exceptions import GOBException
from gobcore.model.migrations import GOBMigrations


class TestMigrations(unittest.TestCase):

    def setUp(self):
        self.migrations = GOBMigrations()
        self.mock_migration = {
            'target_version': '0.2',
            'conversions': [
                {
                    'action': 'rename',
                    'old_column': 'old',
                    'new_column': 'new'
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

    def test_apply_migration(self):
        # Assert an empty list is returned if no migration is found
        data = {
            'entity': {
                'old': 'value',
                '_version': '0.1'
            }
        }

        expected_data = {
            'entity': {
                'new': 'value',
                '_version': '0.2'
            }
        }

        result = self.migrations._apply_migration(data, self.mock_migration)

        self.assertEqual(result, expected_data)

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
            result = self.migrations._apply_migration(data, not_implemented_mock_migration)

    @patch('gobcore.model.migrations.logger', MagicMock())
    @patch('gobcore.model.migrations.GOBMigrations._get_migration')
    def test_migrate_event_data(self, mock_get_migration):
        mock_get_migration.return_value = self.mock_migration

        data = {
            'entity': {
                'old': 'value',
                '_version': '0.1'
            }
        }

        expected_data = {
            'entity': {
                'new': 'value',
                '_version': '0.2'
            }
        }

        self.migrations.migrate_event_data(data, 'catalog', 'collection', '0.2')

        self.assertEqual(data, expected_data)


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
                '_version': '0.1'
            }
        }

        expected_data = {
            'entity': {
                'extra_new': 'value',
                '_version': '0.3'
            }
        }

        self.migrations.migrate_event_data(data, 'catalog', 'collection', '0.3')

        self.assertEqual(data, expected_data)

    @patch('gobcore.model.migrations.logger')
    @patch('gobcore.model.migrations.GOBMigrations._get_migration')
    def test_migrate_event_data_missing_migration(self, mock_get_migration, mock_logger):
        mock_get_migration.side_effect = [self.mock_migration, None]

        data = {
            'entity': {
                'old': 'value',
                '_version': '0.1'
            }
        }

        with self.assertRaises(GOBException):
            self.migrations.migrate_event_data(data, 'catalog', 'collection', '0.3')
            mock_logger.assert_called()
