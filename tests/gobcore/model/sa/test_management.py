import unittest

from gobcore.model.sa.management import Log, Service, ServiceTask


class TestManagement(unittest.TestCase):

    def setUp(self):
        pass

    def test_log(self):
        log = Log(msg="message contents")
        self.assertEqual(str(log), "<Msg message contents>")

    def test_services(self):
        srv = Service(name="service name")
        self.assertEqual(str(srv), "<Service service name>")

    def test_log(self):
        task = ServiceTask(service_name="service name", name="task name")
        self.assertEqual(str(task), "<ServiceTask service name:task name>")
