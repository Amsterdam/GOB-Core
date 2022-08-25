import json

import pytest

from gobcore.standalone import run_as_standalone, parent_argument_parser, _build_message


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
            "import", "--catalogue", "test_catalogue"
        ])
        SERVICEDEFINITION = {
            "import": {
                "handler": lambda x: {"msg": "data"}
            }
        }
        assert run_as_standalone(args, SERVICEDEFINITION) == {"msg": "data"}

    def test_build_message_pass_message(self):
        message_data_json = json.dumps(
            {"header": {"catalogue": "nap", "mode": "full", "collection": "peilmerken",
                        "entity": "peilmerken", "attribute": None,
                        "application": "Grondslag", "source": "AMSBI", "depends_on": {},
                        "enrich": {}, "version": "0.1",
                        "timestamp": "2022-08-25T14:25:37.118522"},
             "summary": {"num_records": 1396, "warnings": [], "errors": [],
                         "log_counts": {"data_warning": 132}},
             "contents_ref": "/app/shared/message_broker/20220825.142531.48048adc-cf34-42b5-a344-c8edbed9ff16"}
        )
        parser, subparsers = parent_argument_parser()
        import_parser = subparsers.add_parser(
            name="compare",
        )

        args = parser.parse_args([
            f"--message-data={message_data_json}",
            "compare",
        ])

        message = _build_message(args)
        assert message["header"]["catalogue"] == "nap"
        assert message["header"]["collection"] == "peilmerken"
        assert message["header"]["source"] == "AMSBI"

    def test_build_message_from_args(self, arg_parser):
        # TODO: do not test private methods
        args = arg_parser.parse_args([
            "import", "--catalogue", "test_catalogue"
        ])
        message = _build_message(args)
        assert message["header"]["catalogue"] == "test_catalogue"
        assert message["header"]["collection"] == None
