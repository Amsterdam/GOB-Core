import json
import os

from collections import defaultdict

from gobcore.exceptions import GOBException

from gobcore.logging.logger import logger

from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD


class GOBMigrations():
    _migrations = None

    def __init__(self):
        if GOBMigrations._migrations is not None:
            # Migrations already initialised
            return

        path = os.path.join(os.path.dirname(__file__), 'gobmigrations.json')
        with open(path) as file:
            data = json.load(file)

        self._data = data
        self._model = GOBModel()

        GOBMigrations._migrations = defaultdict(lambda: defaultdict(lambda: defaultdict(None)))

        # Extract migrations for easy access in API
        self._extract_migrations()

    def _extract_migrations(self):
        for catalog_name, catalog in self._data.items():
            for collection_name, collection in catalog.items():
                for version, migration in collection.items():
                    # Store the migrations(s) for the catalog - collection - version
                    self._migrations[catalog_name][collection_name][version] = migration

    def _get_migration(self, catalog_name, collection_name, version):
        """
        Get the migration for the specified version in the given catalog - collection

        :param catalog_name:
        :param collection_name:
        :param version:
        :return:
        """
        try:
            return self._migrations[catalog_name][collection_name][version]
        except KeyError:
            return None

    def _apply_migration(self, data, migration):
        for conversion in migration['conversions']:
            if conversion.get('action') == 'rename':
                old_key = conversion.get('old_column')
                new_key = conversion.get('new_column')

                assert all([old_key, new_key]), "Invalid conversion definition"

                # Rename the column
                data['entity'][new_key] = data['entity'].pop(old_key)
            else:
                raise NotImplementedError(f"Conversion {conversion['action']} not implemented")

        # Update the entity version to the new version
        data['entity'][FIELD.VERSION] = migration['target_version']

        return data

    def migrate_event_data(self, data, catalog_name, collection_name, target_version):
        """
        Migrate data to the target version

        :param data:
        :param catalog_name:
        :param collection_name:
        :param target_version:
        :return:
        """
        while data['entity'][FIELD.VERSION] != target_version:
            current_version = data['entity'][FIELD.VERSION]
            migration = self._get_migration(catalog_name, collection_name, current_version)

            if not migration:
                logger.error(f"No migration found for {catalog_name}, {collection_name} {current_version}")
                raise GOBException(
                    f"Not able to migrate event for {catalog_name}, {collection_name} to version {target_version}"
                )
            # Apply all conversions on the data
            data = self._apply_migration(data, migration)

        return data
