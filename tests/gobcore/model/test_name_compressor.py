import unittest

from gobcore.model.name_compressor import NameCompressor


class TestNameCompressor(unittest.TestCase):

    def test_conversions(self):
        saved_conversions = NameCompressor._CONVERSIONS
        NameCompressor._CONVERSIONS = {
            "reference": "ref"
        }
        names = [
            "",
            "is_bron_voor_aantekening_kadastraal_object",
            "betrokken_bij_appartementsrechtsplitsing_vve",
            "ontstaan_uit_appartementsrechtsplitsing_vve",
            "some string"
        ]

        # Don't have conversions without any profit
        for key, value in NameCompressor._CONVERSIONS.items():
            conversion = NameCompressor._compressed_value(value)
            self.assertTrue(len(conversion) < len(key))

        # uncompress(compress(name)) == name
        # conversions can never result in longer strings than the originale
        for name in names:
            conversion = NameCompressor.compress_name(name)
            self.assertTrue(len(conversion) <= len(name))
            self.assertEqual(NameCompressor.uncompress_name(conversion), name)

        NameCompressor._CONVERSIONS = saved_conversions
