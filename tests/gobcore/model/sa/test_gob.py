import unittest

from gobcore.model.sa.gob import models


class TestManagement(unittest.TestCase):

    def setUp(self):
        pass

    def test_model(self):
        for (name, cls) in models.items():
            m = cls()
            self.assertEqual(str(m), name)
