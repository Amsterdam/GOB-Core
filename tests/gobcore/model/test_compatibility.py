"""GOBModel backwards compatibility tests."""


from gobcore.model import GOBModel

gob_model = GOBModel()


def test_model_data():
    """GOBModel data checks (get_catalogs)."""
    assert GOBModel._data is GOBModel.data is gob_model.data is gob_model._data is gob_model.get_catalogs()

def test_model_catalogs():
    """GOBModel catalogs checks -- get_catalogs().items()."""
    assert gob_model.items() == gob_model.get_catalogs().items()
