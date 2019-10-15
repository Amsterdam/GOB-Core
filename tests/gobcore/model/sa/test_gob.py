import unittest
from unittest.mock import patch

from gobcore.model.sa.gob import models, model, _derive_models, _derive_indexes, GOBModel


class TestGob(unittest.TestCase):

    def setUp(self):
        pass

    def test_model(self):
        for (name, cls) in models.items():
            m = cls()
            self.assertEqual(str(m), name)
