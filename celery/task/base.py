from carrot.connection import DjangoAMQPConnection
from celery.conf import AMQP_CONNECTION_TIMEOUT
from celery.messaging import TaskPublisher, TaskConsumer
from celery.log import setup_logger
from celery.result import TaskSetResult
from celery.execute import apply_async, delay_task
from datetime import timedelta
import uuid
try:
    import cPickle as pickle
except ImportError:
    import pickle


class Task(object):
    name = None
    type = "regular"
    routing_key = None
    immediate = False
    mandatory = False
    priority = None
    ignore_result = False
    disable_error_emails = False

    def __init__(self):
        if not self.name:
            raise NotImplementedError("Tasks must define a name attribute.")

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def run(self, *args, **kwargs):
        raise NotImplementedError("Tasks must define a run method.")

    def get_logger(self, **kwargs):
        return setup_logger(**kwargs)

    def get_publisher(self):
        return TaskPublisher(connection=DjangoAMQPConnection(
                                connect_timeout=AMQP_CONNECTION_TIMEOUT))

    def get_consumer(self):
        return TaskConsumer(connection=DjangoAMQPConnection(
                                connect_timeout=AMQP_CONNECTION_TIMEOUT))

    @classmethod
    def delay(cls, *args, **kwargs):
        return apply_async(cls, args, kwargs)

    @classmethod
    def apply_async(cls, args=None, kwargs=None, **options):
        return apply_async(cls, args, kwargs, **options)


class TaskSet(object):
    
    def __init__(self, task, args):
        try:
            task_name = task.name
            task_obj = task
        except AttributeError:
            task_name = task
            task_obj = tasks[task_name]

        self.task = task_obj
        self.task_name = task_name
        self.arguments = args
        self.total = len(args)

    def run(self, connect_timeout=AMQP_CONNECTION_TIMEOUT):
        taskset_id = str(uuid.uuid4())
        conn = DjangoAMQPConnection(connect_timeout=connect_timeout)
        publisher = TaskPublisher(connection=conn)
        subtasks = [apply_async(self.task, args, kwargs,
                                taskset_id=taskset_id, publisher=publisher)
                        for args, kwargs in self.arguments]
        publisher.close()
        conn.close()
        return TaskSetResult(taskset_id, subtasks)

    def iterate(self):
        return iter(self.run())

    def join(self, timeout=None):
        return self.run().join(timeout=timeout)

    @classmethod
    def remote_execute(cls, func, args):
        pickled = pickle.dumps(func)
        arguments = [[[pickled, arg, {}], {}] for arg in args]
        return cls(ExecuteRemoteTask, arguments)

    @classmethod
    def map(cls, func, args, timeout=None):
        remote_task = cls.remote_execute(func, args)
        return remote_task.join(timeout=timeout)

    @classmethod
    def map_async(cls, func, args, timeout=None):
        serfunc = pickle.dumps(func)
        return AsynchronousMapTask.delay(serfunc, args, timeout=timeout)


class PeriodicTask(Task):
    run_every = timedelta(days=1)
    type = "periodic"

    def __init__(self):
        if not self.run_every:
            raise NotImplementedError(
                    "Periodic tasks must have a run_every attribute")

        # If run_every is a integer, convert it to timedelta seconds.
        if isinstance(self.run_every, int):
            self.run_every = timedelta(seconds=self.run_every)

        super(PeriodicTask, self).__init__()
