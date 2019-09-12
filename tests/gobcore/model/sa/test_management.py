import unittest

from gobcore.model.sa.management import Log, Service, ServiceTask, Job, JobStep, Task


class TestManagement(unittest.TestCase):

    def setUp(self):
        pass

    def test_log(self):
        log = Log(msg="message contents")
        self.assertEqual(str(log), "<Msg message contents>")

    def test_services(self):
        srv = Service(name="service name")
        self.assertEqual(str(srv), "<Service service name>")

    def test_servicetask(self):
        task = ServiceTask(service_name="service name", name="task name")
        self.assertEqual(str(task), "<ServiceTask service name:task name>")

    def test_job(self):
        job = Job(name='Jobname')
        self.assertEqual(str(job), "<Job Jobname>")

    def test_jobstep(self):
        jobstep = JobStep(name='Jobstepname')
        self.assertEqual(str(jobstep), "<Job Jobstepname>")

    def test_task(self):
        task = Task(name='Taskname', stepid='stepid')
        self.assertEqual(str(task), "<Task Taskname (stepid)>")
