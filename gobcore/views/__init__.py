import os


class GOBViews():
    """Views are loaded from this directory. New views should be placed under catalog/collection/viewname.sql. No
    further actions needed.

    """
    _data = {}

    def __init__(self):
        if not self._data:
            self._load_views()

    def _load_views(self):
        """Loads views from directory and saves them in self._data

        :return:
        """
        catalogs = self._dirs_in_path(os.path.dirname(__file__))
        self._data = {}

        for catalog, catalog_path in catalogs:
            self._load_catalog_from_dir(catalog, catalog_path)

    def _load_catalog_from_dir(self, catalog_name: str, catalog_path: str):
        """Loads catalog from catalog_path

        :param catalog_name:
        :param catalog_path:
        :return:
        """
        self._data[catalog_name] = {}

        for collection, collection_path in self._dirs_in_path(catalog_path):
            self._load_collection_from_dir(catalog_name, collection, collection_path)

    def _load_collection_from_dir(self, catalog_name: str, collection_name: str, collection_path: str):
        """Loads collection from collection_path

        :param catalog_name:
        :param collection_name:
        :param collection_path:
        :return:
        """
        self._data[catalog_name][collection_name] = {}

        sql_files = self._sql_files_in_dir(collection_path)

        for view_filename, file_location in sql_files:
            with open(file_location) as file:
                view_name = '.'.join(view_filename.split('.')[:-1])
                self._data[catalog_name][collection_name][view_name] = {
                    'query': file.read(),
                    'name': f'{catalog_name}_{collection_name}_{view_name}'
                }

    def _sql_files_in_dir(self, dir: str):
        """Returns list of tuples of (filename.sql, path/to/filename.sql) for all sql files in dir

        :param dir:
        :return:
        """
        files = []
        for item in os.listdir(dir):
            item_path = os.path.join(dir, item)
            if os.path.isfile(item_path) and item.endswith('.sql'):
                files.append((item, item_path))
        return files

    def _dirs_in_path(self, path: str):
        """Returns the directories in path. Result is a list of tuples [(dirname, dirpath)], for example:

        _dirs_in_path('/tmp')
        result: [('dirA', '/tmp/dirA'), ('dirB', '/tmp/dirB')]

        :param path:
        :return:
        """
        dirs = []
        for item in os.listdir(path):
            item_path = os.path.join(path, item)
            if os.path.isdir(item_path) and not item.startswith('__'):
                dirs.append((item, item_path))
        return dirs

    def get_catalogs(self):
        return list(self._data.keys())

    def get_entities(self, catalog_name):
        return list(self._data[catalog_name].keys())

    def get_views(self, catalog_name, entity_name):
        return self._data[catalog_name][entity_name]

    def get_view(self, catalog_name, entity_name, view_name):
        try:
            return self._data[catalog_name][entity_name][view_name]
        except KeyError:
            return None
