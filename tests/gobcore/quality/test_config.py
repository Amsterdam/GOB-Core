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
                    self.assertIsNotNone(re.match(d['pattern'], s))

                for s in should_fail:
                    self.assertIsNone(re.match(d['pattern'], s))
