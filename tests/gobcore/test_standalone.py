import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from argparse import ArgumentParser
from unittest.mock import Mock, patch

from gobcore.standalone import run_as_standalone, parent_argument_parser, _build_message, LOG_HANDLERS
from tests.gobcore.fixtures import get_servicedefinition_fixture


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

    @pytest.fixture
    def result_path(self):
        """Use temporary directory for the result.json. The default mount of /airflow/.. is not always available."""
        with TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir, "return.json")

    def test_run_as_standalone(self, arg_parser: ArgumentParser, result_path: Path):
        args = arg_parser.parse_args([
            "import", "--catalogue", "test_catalogue", "--mode", "full"
        ])
        args.message_result_path = result_path

        mock_handler = Mock(return_value={
            "contents": (item for item in [1, 2]),  # should NOT be in return.json
            "contents_ref": "/my/data/json"
        })
        servicedefinition = get_servicedefinition_fixture(mock_handler, "import")
        servicedefinition["import"]["logger"] = "the logger"
        servicedefinition["import"]["pass_args_standalone"] = ["mode"]

        message_in = {'header': {'catalogue': 'test_catalogue', 'collection': None, 'entity': None, 'attribute': None,
                                 'application': None, 'mode': 'full'}}

        with patch("gobcore.standalone.logger") as mock_logger:
            assert run_as_standalone(args, servicedefinition) == 0

            mock_logger.configure_context.assert_called_with(message_in, "THE LOGGER", LOG_HANDLERS)

        mock_handler.assert_called_with(message_in)
        assert result_path.read_text() == '{"contents_ref": "/my/data/json"}'

    def test_run_as_standalone_no_mode(self, result_path: Path):
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
        args.message_result_path = result_path
        mock_handler = Mock(return_value={})
        servicedefinition = get_servicedefinition_fixture(mock_handler, "import")

        assert run_as_standalone(args, servicedefinition) == 0
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

    def test_run_as_standalone_error(self, arg_parser: ArgumentParser, result_path: Path):
        args = arg_parser.parse_args([
            "import", "--catalogue", "test_catalogue"
        ])
        args.message_result_path = result_path

        summary = {
            "errors": ["An error occurred"],
        }
        mock_handler = Mock(return_value={"summary": summary})
        servicedefinition = get_servicedefinition_fixture(mock_handler, "import")

        assert run_as_standalone(args, servicedefinition) == 1  # error
        mock_handler.assert_called()

    def test_run_as_standalone_error_handler_not_defined(
            self, arg_parser: ArgumentParser, result_path: Path
    ):
        """Test if an error is raised when handler is not in service definition.

        Handler might be defined in args, but not in servicedefinition. This
        mismatch should fail.
        """
        args = arg_parser.parse_args([
            "import", "--catalogue", "test_catalogue"
        ])
        args.message_result_path = result_path
        summary = {
            "errors": ["An error occurred"],
        }
        mock_handler = Mock(return_value={"summary": summary})
        servicedefinition = get_servicedefinition_fixture(mock_handler, "another_handler")

        with pytest.raises(KeyError):
            run_as_standalone(args, servicedefinition)

        mock_handler.assert_not_called()

    def test_build_message_pass_message(self):
        """Test if message-data coming in is just passed along."""

        message_data = {
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
            "contents_ref": "/app/shared/message_broker/20220825.142531.48048adc-cf34-42b5-a344-c8edbed9ff16"
        }
        message_data_json = json.dumps(message_data)
        parser, subparsers = parent_argument_parser()
        subparsers.add_parser(
            name="compare",
        )

        args = parser.parse_args([
            f"--message-data={message_data_json}",
            "compare",
        ])

        message = _build_message(args, [])
        assert message == message_data

    def test_build_message_from_args(self, arg_parser: ArgumentParser):
        args = arg_parser.parse_args([
            "import", "--catalogue", "test_catalogue"
        ])
        message = _build_message(args, [])
        assert message["header"]["catalogue"] == "test_catalogue"
        assert message["header"]["collection"] is None

    def test_build_message_override_command_line_args(self, arg_parser: ArgumentParser):
        message_data = {
            "header": {
                "catalogue": "nap",
                "entity": "peilmerken",
            },
            "other": {
                "some": "value",
            },
            "str": "some string",
        }

        args = arg_parser.parse_args([
            "--message-data", json.dumps(message_data), "import", "--catalogue", "test_catalogue",
        ])
        message = _build_message(args, [])

        assert message == {
            "header": {
                "catalogue": "test_catalogue",
                "entity": "peilmerken",
            },
            "other": {
                "some": "value",
            },
            "str": "some string",
        }
        assert message["header"]["catalogue"] == "test_catalogue"
        assert "collection" not in message["header"], "Should not initialise missing values"
