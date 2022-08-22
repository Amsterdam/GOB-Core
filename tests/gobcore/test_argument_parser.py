import argparse

from gobcore.standalone import combine_parsers


class TestArguments:

    def test_combine_parsers(self):
        first_parser = argparse.ArgumentParser(
            description="Main parser"
        )
        additional_parser = argparse.ArgumentParser(
            description="Additional parser"
        )
        combine_parsers(parsers=[first_parser, additional_parser])
