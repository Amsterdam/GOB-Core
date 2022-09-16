"""GOBModel backwards compatibility tests."""


from unittest import TestCase

from gobcore.model import GOBModel

gob_model = GOBModel()


class TestCompatibility(TestCase):
    """GOBModel compatibility tests."""

    def test_model_data(self):
        """GOBModel data checks (get_catalogs)."""
        assert GOBModel._data is GOBModel.data is gob_model.data is gob_model._data is gob_model.get_catalogs()

    def test_model_catalogs(self):
        """GOBModel catalogs checks."""
        # get_catalogs().items()
        self.assertEqual(
            gob_model.items(), gob_model.get_catalogs().items(), msg="get_catalog().items() trouble!")
        # gob_model.get_catalogs().keys()
        self.assertEqual(
            gob_model.keys(), gob_model.get_catalogs().keys(), msg="get_catalog().keys() trouble!")
        # gob_model.get_catalog_names()
        self.assertEqual(
            gob_model.keys(), gob_model.get_catalog_names(), msg="get_catalog_names() trouble!")
        # Catalog count.
        self.assertEqual(len(gob_model), 13, msg="catalog count has changed")
        # Catalog 'doesnotexist" should not exist.
        self.assertIsNone(
            gob_model.get('doesnotexist'), msg="Catalog 'doesnotexist' should not exist!")

        # .keys()
        for catalog_name in gob_model.keys():
            # gob_model.get_catalog()
            self.assertEqual(gob_model[catalog_name], gob_model.get_catalog(catalog_name))
            self.assertEqual(gob_model.get(catalog_name), gob_model.get_catalog(catalog_name))
            # Catalog collections.
            for collection in gob_model[catalog_name]['collections']:
                # The number of collections should be more than 8!?
                self.assertTrue(
                    len(gob_model[catalog_name]['collections'][collection]) > 8,
                    msg=f"Not enough collections for {catalog_name}!?"
                )

    def test_model_iter(self):
        """GOBModel catalog and collection iteration checks."""
        for catalog_name in gob_model:
            # __getitem__
            self.assertIs(gob_model[catalog_name], gob_model.get(catalog_name))
            # __contains__
            self.assertIn(catalog_name, gob_model)
            # gob_model.get_catalog()
            self.assertEqual(gob_model[catalog_name], gob_model.get_catalog(catalog_name))
            self.assertEqual(gob_model.get(catalog_name), gob_model.get_catalog(catalog_name))
            # gob_model.get_collection_names(catalog_name)
            self.assertEqual(
                gob_model[catalog_name]['collections'].keys(),
                gob_model.get_collection_names(catalog_name)
            )
            # gob_model.get_collections(catalog_name)
            self.assertIs(
                gob_model[catalog_name]['collections'], gob_model.get_collections(catalog_name))
            #
            # Catalog collection checks.
            for collection in gob_model[catalog_name]['collections']:
                # gob_model.get_collection(catalog_name, collection)
                self.assertEqual(
                    gob_model[catalog_name]['collections'][collection],
                    gob_model.get_collection(catalog_name, collection)

                )
