import pytest

from gobcore.standalone import run_as_standalone, parent_argument_parser


class TestStandalone:

    @pytest.fixture
    def arg_parser(self):
        parser, subparsers = parent_argument_parser()

        # import handler parser
        import_parser = subparsers.add_parser(
            name="import",
        )
        import_parser.add_argument(
            "--catalogue",
            required=True,
        )
        return parser

    def test_run_as_standalone(self, arg_parser):
        args = arg_parser.parse_args([
            'import', '--catalogue', 'test_catalogue'
        ])
        SERVICEDEFINITION = {
            "import": {
                "handler": lambda x: {"msg": "data"}
            }
        }
        assert run_as_standalone(args, SERVICEDEFINITION) == {"msg": "data"}
