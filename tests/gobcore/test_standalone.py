import json
import pytest
from argparse import ArgumentParser
from mock import Mock

from gobcore.standalone import run_as_standalone, parent_argument_parser, _build_message


class TestStandalone:

    @pytest.fixture
    def arg_parser(self) -> ArgumentParser:
        parser, subparsers = parent_argument_parser()

        # import handler parser
        import_parser = subparsers.add_parser(
            name="import",
        )
        import_parser.add_argument(
            "--catalogue",
            required=True,
        )
        import_parser.add_argument(
            "--mode",
            required=False,
            default="full",
            choices=["full", "recent"]
        )
        return parser

    def test_run_as_standalone(self, arg_parser: ArgumentParser):
        args = arg_parser.parse_args([
            "import", "--catalogue", "test_catalogue", "--mode", "full"
        ])
        mock_handler = Mock(return_value={})
        SERVICEDEFINITION = {
            "import": {
                "handler": mock_handler
            }
        }
        assert run_as_standalone(args, SERVICEDEFINITION) == 0
        mock_handler.assert_called_with({
            "header": {
                "catalogue": "test_catalogue",
                "collection": None,
                "entity": None,
                "attribute": None,
                "application": None,
                "mode": "full"
            }
        })

    def test_run_as_standalone_no_mode(self):
        # Arg parser without mode
        arg_parser, subparsers = parent_argument_parser()
        import_parser = subparsers.add_parser(
            name="import",
        )
        import_parser.add_argument(
            "--catalogue",
            required=True,
        )

        args = arg_parser.parse_args([
            "import", "--catalogue", "test_catalogue"
        ])
        mock_handler = Mock(return_value={})
        SERVICEDEFINITION = {
            "import": {
                "handler": mock_handler
            }
        }
        assert run_as_standalone(args, SERVICEDEFINITION) == 0
        mock_handler.assert_called_with({
            "header": {
                "catalogue": "test_catalogue",
                "collection": None,
                "entity": None,
                "attribute": None,
                "application": None
                # node mode key here
            }
        })

    def test_run_as_standalone_error(self, arg_parser: ArgumentParser):
        args = arg_parser.parse_args([
            "import", "--catalogue", "test_catalogue"
        ])
        summary = {
            "errors": ["An error occurred"],
        }
        mock_handler = Mock(return_value={"summary": summary})
        SERVICEDEFINITION = {
            "import": {
                "handler": mock_handler
            }
        }
        assert run_as_standalone(args, SERVICEDEFINITION) == 1  # error
        mock_handler.assert_called()

    def test_run_as_standalone_error_handler_not_defined(
            self, arg_parser: ArgumentParser
    ):
        """Test if an error is raised when handler is not in service definition.

        Handler might be defined in args, but not in servicedefinition. This
        mismatch should fail.
        """
        args = arg_parser.parse_args([
            "import", "--catalogue", "test_catalogue"
        ])
        summary = {
            "errors": ["An error occurred"],
        }
        mock_handler = Mock(return_value={"summary": summary})
        SERVICEDEFINITION = {
            "another_handler": {
                "handler": mock_handler
            }
        }
        with pytest.raises(KeyError):
            run_as_standalone(args, SERVICEDEFINITION)

        mock_handler.assert_not_called()

    def test_build_message_pass_message(self):
        """Test if message-data coming in is just passed along."""
        message_data_json = json.dumps(
            {
                "header": {
                    "catalogue": "nap", "mode": "full", "collection": "peilmerken",
                    "entity": "peilmerken", "attribute": None,
                    "application": "Grondslag", "source": "AMSBI", "depends_on": {},
                    "enrich": {}, "version": "0.1",
                    "timestamp": "2022-08-25T14:25:37.118522"
                },
             "summary": {
                 "num_records": 1396, "warnings": [], "errors": [],
                 "log_counts": {"data_warning": 132}
             },
             "contents_ref": "/app/shared/message_broker/20220825.142531.48048adc-cf34-42b5-a344-c8edbed9ff16"}
        )
        parser, subparsers = parent_argument_parser()
        subparsers.add_parser(
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

    def test_build_message_from_args(self, arg_parser: ArgumentParser):
        args = arg_parser.parse_args([
            "import", "--catalogue", "test_catalogue"
        ])
        message = _build_message(args)
        assert message["header"]["catalogue"] == "test_catalogue"
        assert message["header"]["collection"] == None
