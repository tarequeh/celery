from celery.task.base import Task, TaskSet, PeriodicTask
from celery.registry import tasks
from celery.storage import default_storage
from datetime import timedelta
from celery.serialization import pickle


class DeleteExpiredTaskMetaTask(PeriodicTask):
    """A periodic task that deletes expired task metadata every day.

    This runs the current backend's
    :meth:`celery.storage.base.BaseBackend.cleanup` method.

    """
    name = "celery.delete_expired_task_meta"
    run_every = timedelta(days=1)

    def run(self, **kwargs):
        """The method run by ``celeryd``."""
        logger = self.get_logger(**kwargs)
        logger.info("Deleting expired task meta objects...")
        default_storage.cleanup()
tasks.register(DeleteExpiredTaskMetaTask)


class PingTask(Task):
    """The task used by :func:`ping`."""
    name = "celery.ping"

    def run(self, **kwargs):
        """:returns: the string ``"pong"``."""
        return "pong"
tasks.register(PingTask)
