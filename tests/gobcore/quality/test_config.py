import re

from unittest import TestCase

from gobcore.quality.config import QA_LEVEL, QA_CHECK


class TestConfig(TestCase):

    def test_qa_level(self):
        self.assertIsNotNone(QA_LEVEL.FATAL)
        self.assertIsNotNone(QA_LEVEL.WARNING)
        self.assertIsNotNone(QA_LEVEL.INFO)

    def test_qa_check(self):
        for prop in [prop for prop in dir(QA_CHECK) if not prop.startswith('_')]:
            check = getattr(QA_CHECK, prop)
            self.assertTrue(isinstance(check, dict))
            self.assertIsNotNone(check['msg'])
            self.assertEqual(check['id'], prop)

    def test_qa_check_regexes(self):

        regex_tests = {
            # Key is name, value is tuple ([should pass regex], [should fail regex])
            'Format_N8': (['01234567', '98765432'],
                          ['0123456', '9786543', '012345678', '987654321', 'abcdefghi', '0123456a', 'a1234567']),
            'Format_numeric': (['1', '2', '52432134', '0134234'],
                               ['-1', 'abc', '0123456a', 'a1234567']),
            'Format_alphabetic': (['a', 'abc', 'AaBb'],
                                  ['-1', '1', '0123456a', 'a1234567']),
            'Format_ANN': (['a12', 'A12', 'Z43'],
                           ['abc', '12a', '1a22', 'a1', 'a1234567']),
            'Format_AANN': (['ab12', 'AB12', 'xZ43', 'Xz00'],
                            ['abc', '12ab', '1az22', '12ab', 'a1']),
            'Format_AAN_AANN': (['meetbouten/rollagen/al06.jpg'],
                                ['abc', '12ab', 'b12', 'AB']),
            'Format_4_2_2_2_6_HEX_SEQ': (['{012345AB-6789-0ABC-DEF1-23456789ABCD}.1', '{012345ab-6789-0abc-def1-23456789abcd}.2'],
                                     ['012345AB-6789-0ABC-DEF1-23456789ABCD.3', '{Z12345AB-6789-0ABC-DEF1-23456789ABCD.1}']),
            'Format_0363': (
                ['036300000', '0363123456987951'],
                ['045700000', 'abcd', '0361abc', '0363 ']
            ),
            'Value_1_2_3': (['1', '2', '3'],
                            ['4', 'a', '0']),
            'Value_wind_direction_NOZW': (['N', 'NO', 'O', 'ZO', 'Z', 'ZW', 'W', 'NW'],
                                          ['n', 'NZ', '0']),
            'Value_1_0': (['1', '0'],
                          ['true', 'a', '2']),
            'Value_J_N': (['J', 'N'],
                          ['true', '1', 'M']),
            'Value_not_empty': (['J', '1', 'abcd', 'a1b2'],
                                ['']),
            'Value_brondocument_coding': (['AB12345678_AB12AB.abc', 'ab12345678_ab12ab.ABC', 'SV12345678_OV12VL.dgn'],
                                          ['AB12345678AB12AB.abc', 'AB123456789_AB12AB.abc']),
            'Value_woonplaats_bronwaarde_1012_1024_1025_3594': (
                ["{'bronwaarde': '1012'}", "{'bronwaarde': '1025'}"],
                ["{'anders': '1012'}", "{'bronwaarde': '0457'}", '{"bronwaarde": "1012"}'],
            ),
            'Value_woonplaats_1012_1024_1025_3594': (
                ["1012", "1025"],
                ["1020", "0457"],
            )
        }

        # Get all dicts defined in the class
        qa_defs = [v for v in vars(QA_CHECK) if isinstance(getattr(QA_CHECK, v), dict)]

        for qa_def in qa_defs:
            d = getattr(QA_CHECK, qa_def)

            if 'pattern' in d:
                # Make sure we cover all regexes in QA_CHECK
                self.assertIn(qa_def, regex_tests, f"Regex test for {qa_def} is not defined")

                should_pass, should_fail = regex_tests[qa_def]

                for s in should_pass:
                    self.assertIsNotNone(re.match(d['pattern'], s),s)

                for s in should_fail:
                    self.assertIsNone(re.match(d['pattern'], s))
