"""celery.managers"""
from django.db import models, connection, transaction
from django.db.models.query import QuerySet
from celery.registry import tasks
from celery.conf import TASK_RESULT_EXPIRES
from datetime import datetime, timedelta
from django.conf import settings
import random

# server_drift can be negative, but timedelta supports addition on
# negative seconds.
SERVER_DRIFT = timedelta(seconds=random.vonmisesvariate(1, 4))


def update_with_dict(obj, fields):
    [setattr(obj, name, val) for name, val in fields.items()]
    obj.save()
    return obj


class ExtendedQuerySet(QuerySet):

    def update_or_create(self, **kwargs):
        obj, created = self.get_or_create(**kwargs)

        if not created:
            fields = dict(kwargs.pop("defaults", {}))
            fields.update(kwargs)
            update_with_dict(obj, fields)

        return obj


class ExtendedManager(models.Manager):

    def get_query_set(self):
        return ExtendedQuerySet(self.model)

    def update_or_create(self, **kwargs):
        update_method = self._update_or_create
        if kwargs.pop("exception_retry", False):
            update_method = self._update_or_create_safe
        return update_method(**kwargs)

    def _update_or_create(self, **kwargs):
        return self.get_query_set().update_or_create(**kwargs)

    def _update_or_create_safe(self, **kwargs):
        try:
            return self._update_or_create(**kwargs)
        except Exception, exc:
            # depending on the database backend we can get various exceptions.
            # for excample, psycopg2 raises an exception if some operation
            # breaks transaction, and saving task result won't be possible
            # until we rollback transaction
            transaction.rollback_unless_managed()
            self._update_or_create(**kwargs)


class TableLock(object):
    """Base class for database table locks. Also works as a NOOP lock."""

    def __init__(self, table, type="read"):
        self.table = table
        self.type = type
        self.cursor = None

    def lock_table(self):
        """Lock the table."""
        pass

    def unlock_table(self):
        """Release previously locked tables."""
        pass

    @classmethod
    def acquire(cls, table, type=None):
        """Acquire table lock."""
        lock = cls(table, type)
        lock.lock_table()
        return lock

    def release(self):
        """Release the lock."""
        self.unlock_table()
        if self.cursor:
            self.cursor.close()
            self.cursor = None


class MySQLTableLock(TableLock):
    """Table lock support for MySQL."""

    def lock_table(self):
        """Lock MySQL table."""
        self.cursor = connection.cursor()
        self.cursor.execute("LOCK TABLES %s %s" % (
            self.table, self.type.upper()))

    def unlock_table(self):
        """Unlock MySQL table."""
        self.cursor.execute("UNLOCK TABLES")

TABLE_LOCK_FOR_ENGINE = {"mysql": MySQLTableLock}
table_lock = TABLE_LOCK_FOR_ENGINE.get(settings.DATABASE_ENGINE, TableLock)

class TaskManager(ExtendedManager):
    """Manager for :class:`celery.models.Task` models."""

    def get_task(self, task_id):
        """Get task meta for task by ``task_id``."""
        task, created = self.get_or_create(task_id=task_id)
        return task

    def is_done(self, task_id):
        """Returns ``True`` if the task was executed successfully."""
        return self.get_task(task_id).status == "DONE"

    def get_all_expired(self):
        """Get all expired task results."""
        return self.filter(date_done__lt=datetime.now() - TASK_RESULT_EXPIRES)

    def delete_expired(self):
        """Delete all expired task results."""
        self.get_all_expired().delete()

    def store_result(self, task_id, result, status, traceback=None,
            exception_retry=True):
        """Store the result and status of a task.

        :param task_id: task id

        :param result: The return value of the task, or an exception
            instance raised by the task.

        :param status: Task status. See
            :meth:`celery.result.AsyncResult.get_status` for a list of
            possible status values.

        :keyword traceback: The traceback at the point of exception (if the
            task failed).

        :keyword exception_retry: If True, we try a single retry with
            transaction rollback on exception
        """
        self.update_or_create(task_id=task_id,
                              exception_retry=exception_retry,
                              defaults={"status": status,
                                        "result": result,
                                        "traceback": traceback})

class TaskSetManager(ExtendedManager):
    """Manager for :class:`celery.models.TaskSet` models."""

    def get_all_expired(self):
        """Get all expired taskset results."""
        return self.filter(date_done__lt=datetime.now() - TASK_RESULT_EXPIRES)

    def delete_expired(self):
        """Delete all expired taskset results."""
        self.get_all_expired().delete()

    def get_taskset(self, taskset_id):
        """Get taskset meta for task by ``taskset_id``."""
        try:
            return self.get(taskset_id=taskset_id)
        except self.model.DoesNotExist:
            return None

    def store_task_ids(self, taskset_id, task_ids, exception_retry=True):
        """Store the task ids associated with a taskset.

        :param taskset_id: task set id.
        :param task_ids: the list of task ids

        """
        self.update_or_create(taskset_id=taskset_id,
                              exception_retry=exception_retry,
                              defaults={"task_ids": task_ids})

    def store_result(self, taskset_id, result, exception_retry=True):
        """Store the result of a taskset.

        :param taskset_id: task set id

        :param result: The return value of the taskset

        """
        self.update_or_create(taskset_id=taskset_id,
                              exception_retry=exception_retry,
                              defaults={"result": result})


class PeriodicTaskManager(models.Manager):
    """Manager for :class:`celery.models.PeriodicTask` models."""

    def init_entries(self):
        """Add entries for all registered periodic tasks.

        Should be run at worker start.
        """
        periodic_tasks = tasks.get_all_periodic()
        for task_name in periodic_tasks.keys():
            task_meta, created = self.get_or_create(name=task_name)

    def is_time(self, last_run_at, run_every):
        """Check if if it is time to run the periodic task.

        :param last_run_at: Last time the periodic task was run.
        :param run_every: How often to run the periodic task.

        :rtype bool:

        """
        run_every_drifted = run_every + SERVER_DRIFT
        run_at = last_run_at + run_every_drifted
        if datetime.now() > run_at:
            return True
        return False

    def get_waiting_tasks(self):
        """Get all waiting periodic tasks.

        :returns: list of :class:`celery.models.PeriodicTaskMeta` objects.
        """
        periodic_tasks = tasks.get_all_periodic()
        db_table = self.model._meta.db_table

        # Find all periodic tasks to be run.
        waiting = []
        for task_meta in self.all():
            if task_meta.name in periodic_tasks:
                task = periodic_tasks[task_meta.name]
                run_every = task.run_every
                if self.is_time(task_meta.last_run_at, run_every):
                    # Get the object again to be sure noone else
                    # has already taken care of it.
                    lock = table_lock.acquire(db_table, "write")
                    try:
                        secure = self.get(pk=task_meta.pk)
                        if self.is_time(secure.last_run_at, run_every):
                            secure.last_run_at = datetime.now()
                            secure.save()
                            waiting.append(secure)
                    finally:
                        lock.release()
        return waiting
