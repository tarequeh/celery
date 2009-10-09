from celery.tasks import Task
from celery.registry import tasks


class TestAppTask(Task):

    def run(self, **kwargs):
        logger = self.get_logger(**kwargs)
        logger.info("Running task: %s" % self.name)
