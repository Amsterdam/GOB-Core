import json
from os import path
from collections import defaultdict

from gobcore.exceptions import GOBException
from gobcore.parse import json_to_cached_dict
from gobcore.logging.logger import logger


class GOBMigrations:
    _migrations = None

    def __init__(self):
        if GOBMigrations._migrations is not None:
            # Migrations already initialised
            return

        GOBMigrations._migrations = defaultdict(lambda: defaultdict(lambda: defaultdict(None)))

        # Extract migrations for easy access.
        self._extract_migrations()

    def _extract_migrations(self):
        data = json_to_cached_dict(path.join(path.dirname(__file__), 'gobmigrations.json'))
        for catalog_name, catalog in data.items():
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

    def _rename_column(self, event, data, old_key, new_key):
        """
        Rename a column in event data. Based on the event.action the data should be converted.

        :param event:
        :param data:
        :param old_key:
        :param new_key:
        :return:
        """
        if event.action == 'ADD':
            # Rename the column
            data['entity'][new_key] = data['entity'].pop(old_key)
        elif event.action == 'MODIFY':
            # Rename the column in modifications
            for modification in data.get('modifications'):
                if modification.get('key') == old_key:
                    modification['key'] = new_key

    def _delete_column(self, event, data, column):
        if event.action == 'ADD':
            if column in data['entity']:
                data['entity'].pop(column)
        elif event.action == 'MODIFY':
            data['modifications'] = [mod for mod in data['modifications'] if mod.get('key') != column]

    def _add_column(self, event, data, column, default):
        if event.action == 'ADD':
            if column not in data['entity']:
                data['entity'][column] = default

    def _split_json(self, event, data, column, mapping):
        """Splits an existing GOB.JSON field with name :column: in the fields as specified in :mapping:

        For example:
        :column: is 'oldjsoncolumn'
        :mapping: is {'attr1': 'newcolumn1', 'attr2': 'newcolumn2'}

        The result wil be an event with two new attributes 'newcolumn1' and 'newcolumn2' with values
        'oldjsoncolumn.attr1' and 'oldjsoncolumn.attr2' resp.

        Note that 'oldjsoncolumn' will not be removed; use a subsequent 'delete' action in the same migration to
        remove 'oldjsoncolumn' if needed.

        """
        if event.action == 'ADD':
            column_value = data['entity'][column]

            new_values = {new_col: column_value[json_attr] for new_col, json_attr in mapping.items()}
            data['entity'] = {
                **data['entity'],
                **new_values
            }
        elif event.action == 'MODIFY':
            # If old JSON column is present in a modification, add modifications for the newly mapped fields.
            # Leave the old JSON column modification untouched. User should define an explicit delete action for this.
            new_modifications = []
            for modification in data['modifications']:
                if modification.get('key') == column:
                    for new_col, json_attr in mapping.items():
                        new_modifications.append({
                            'key': new_col,
                            'old_value': modification['old_value'].get(json_attr),
                            'new_value': modification['new_value'].get(json_attr)
                        })
            data['modifications'] += new_modifications

    def _apply_migration(self, event, data, migration):
        """
        Apply a migration on an event by converting the data based on all conversion in the migration

        :param event:
        :param data:
        :param old_key:
        :param new_key:
        :return:
        """
        for conversion in migration['conversions']:
            if conversion.get('action') == 'rename':
                old_key = conversion.get('old_column')
                new_key = conversion.get('new_column')

                assert all([old_key, new_key]), "Invalid conversion definition"

                self._rename_column(event, data, old_key, new_key)
            elif conversion.get('action') == 'delete':
                column = conversion.get('column')
                assert column, "Invalid conversion definition"

                self._delete_column(event, data, column)
            elif conversion.get('action') == 'add':
                column = conversion.get('column')
                default = conversion.get('default')
                assert column, "Invalid conversion definition"

                self._add_column(event, data, column, default)
            elif conversion.get('action') == 'split_json':
                column = conversion.get('column')
                mapping = conversion.get('mapping')
                assert column and mapping, "Invalid conversion definition"

                self._split_json(event, data, column, mapping)
            else:
                raise NotImplementedError(f"Conversion {conversion['action']} not implemented")

        # update the event version
        event.version = migration['target_version']

        return data

    def migrate_event_data(self, event, data, catalog_name, collection_name, target_version):
        """
        Migrate data to the target version

        :param event:
        :param data:
        :param catalog_name:
        :param collection_name:
        :param target_version:
        :return:
        """
        while event.version != target_version:
            migration = self._get_migration(catalog_name, collection_name, event.version)

            if not migration:
                logger.error(f"No migration found for {catalog_name}, {collection_name} {event.version}")
                raise GOBException(
                    f"Not able to migrate event for {catalog_name}, {collection_name} to version {target_version}"
                )
            # Apply all conversions on the data
            self._apply_migration(event, data, migration)

        return data
