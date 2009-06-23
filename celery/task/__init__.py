"""

Working with tasks and task sets.

"""
from carrot.connection import DjangoAMQPConnection
from celery.messaging import TaskConsumer
from celery.conf import AMQP_CONNECTION_TIMEOUT
from celery.registry import tasks
from celery.backends import default_backend
from celery.task.base import Task, TaskSet, PeriodicTask
from celery.task.builtins import AsynchronousMapTask, ExecuteRemoteTask
from celery.task.builtins import DeleteExpiredTaskMetaTask, PingTask
from celery.execute import apply_async, delay_task
try:
    import cPickle as pickle
except ImportError:
    import pickle


def discard_all(connect_timeout=AMQP_CONNECTION_TIMEOUT):
    amqp_connection = DjangoAMQPConnection(connect_timeout=connect_timeout)
    consumer = TaskConsumer(connection=amqp_connection)
    discarded_count = consumer.discard_all()
    amqp_connection.close()
    return discarded_count


def is_done(task_id):
    return default_backend.is_done(task_id)


def dmap(func, args, timeout=None):
    return TaskSet.map(func, args, timeout=timeout)


def dmap_async(func, args, timeout=None):
    return TaskSet.map_async(func, args, timeout=timeout)


def execute_remote(func, *args, **kwargs):
    return ExecuteRemoteTask.delay(pickle.dumps(func), args, kwargs)


def ping():
    return PingTask.apply_async().get()
