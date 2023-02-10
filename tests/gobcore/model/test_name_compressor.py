from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.model.name_compressor import NameCompressor

MOCK_CONVERSIONS = {"something very special": "nothing"}


class TestNameCompressor(TestCase):
    @patch("gobcore.model.name_compressor._CONVERSIONS", MagicMock())
    def test_conversions(self):
        names = [
            "",
            "is_bron_voor_aantekening_kadastraal_object",
            "betrokken_bij_appartementsrechtsplitsing_vve",
            "ontstaan_uit_appartementsrechtsplitsing_vve",
            "some string",
            "some",
        ]

        # Don't have conversions without any profit
        for key, value in MOCK_CONVERSIONS.items():
            conversion = NameCompressor._compressed_value(value)
            self.assertTrue(len(conversion) < len(key))

        # uncompress(compress(name)) == name
        # conversions can never result in longer strings than the originale
        for name in names:
            conversion = NameCompressor.compress_name(name)
            self.assertTrue(len(conversion) <= len(name))
            self.assertEqual(NameCompressor.uncompress_name(conversion), name)

        # test for long names
        mock_print = MagicMock()
        with patch("builtins.print", mock_print):
            s = "x" * NameCompressor.LONG_NAME_LENGTH
            NameCompressor.compress_name(s)
            mock_print.assert_not_called()

            s = "x" * (NameCompressor.LONG_NAME_LENGTH + 1)
            NameCompressor.compress_name(s)
            mock_print.assert_called()

    def test_conversion_order(self):
        """Test compressing order."""
        # Table for which a single compression is sufficient.
        single_table_name = "rel_brk_akt_brk_kot_heeft_betrekking_op_kadastraal_object"
        single_compressed_name = "rel_brk_akt_brk_kot__hft_btrk_p__kadastraal_object"
        self.assertEqual(NameCompressor.compress_name(single_table_name), single_compressed_name)

        # Table for which a double compression is needed.
        double_table_name = "rel_brk2_akt_brk2_kot_heeft_betrekking_op_brk_kadastraal_object"
        double_compressed_name = "rel_brk2_akt_brk2_kot__hft_btrk_op_brk_kot_"
        self.assertEqual(NameCompressor.compress_name(double_table_name), double_compressed_name)
