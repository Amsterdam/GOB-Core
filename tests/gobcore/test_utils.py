from unittest import TestCase
from unittest.mock import patch, mock_open, call

from tests.gobcore.fixtures import get_service_fixture

from gobcore.utils import ProgressTicker, get_dns, get_logger_name


class TestProgressTicker(TestCase):

    def setUp(self) -> None:
        self.ticker = ProgressTicker('TickerName', 15)

    def test_init(self):
        self.assertEqual(self.ticker._name, 'TickerName')
        self.assertEqual(self.ticker._report_interval, 15)
        self.assertEqual(self.ticker._count, 0)

    @patch("builtins.print")
    def test_enter(self, mock_print):
        res = self.ticker.__enter__()

        self.assertEqual(res, self.ticker, "__enter__ should return self")
        mock_print.assert_called_with("Start TickerName")

    @patch("builtins.print")
    def test_exit(self, mock_print):
        self.ticker._count = 18004
        self.ticker.__exit__()
        mock_print.assert_called_with("End TickerName - 18004")

    @patch("builtins.print")
    def test_tick(self, mock_print):
        self.ticker._report_interval = 3

        ticks = [(i, i % 3 == 0) for i in range(1, 20)]

        for cnt, do_print in ticks:
            self.ticker.tick()
            self.assertEqual(cnt, self.ticker._count)

            if do_print:
                mock_print.assert_called_with(f'TickerName - {cnt}')
                mock_print.reset_mock()
            else:
                mock_print.assert_not_called()

    @patch("builtins.print")
    def test_ticks(self, mock_print):
        with self.ticker:
            self.ticker.ticks(10)

            mock_print.assert_called_once()
            assert self.ticker._count == 10

            self.ticker.ticks(5)
            mock_print.assert_called_with("TickerName - 15")
            assert self.ticker._count == 15

            self.ticker.ticks(32)
            mock_print.assert_has_calls([
                call("Start TickerName"),
                call("TickerName - 15"),
                call("TickerName - 30"),
                call("TickerName - 45")
            ])
            assert self.ticker._count == 47

            self.ticker.ticks(1000)
            mock_print.assert_called_with("TickerName - 1,035")


class TestUtilFunctions(TestCase):

    resolve_file = """
# Some comments
#
domain example.com
nameserver 1.2.3.4
"""

    @patch('builtins.open', mock_open(read_data=resolve_file))
    def test_get_dns(self):
        res = get_dns()
        open.assert_called_with('/etc/resolv.conf')

        self.assertEqual('1.2.3.4', res)

        open.side_effect = IOError
        self.assertIsNone(get_dns())

    def test_get_logger_name(self):
        service = get_service_fixture(lambda msg: True)
        service["queue"] = "gob.workflow.import.queue"

        self.assertEqual("IMPORT", get_logger_name(service))

        service["logger"] = "use this logger now"
        self.assertEqual("USE THIS LOGGER NOW", get_logger_name(service))

        service["logger"] = None
        with self.assertRaisesRegex(TypeError, "Name must be str type"):
            get_logger_name(service)

