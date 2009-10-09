from celery.messaging import MonitorConsumer, MonitorPublisher
from carrot.connection import DjangoBrokerConnection
from celery.datastructures import AttributeDict


# ###
# state=received hostname=h8 task_id=... task_name=refresh_feed result=None
#   args=("http://example/rss", ) kwargs={} time="2008-10-06 6:23 PM"

# state=started hostname=h8 task_id=... task_name=refresh_feed result=None
#   args=("http://example/rss", ) kwargs={} time="2008-10-06 6:23 PM"

# state=succeeded hostname=h8 task_id=... task_name=refresh_feed result=8
#   args=("http://example/rss", ) kwargs={} time="2008-10-06 6:23 PM"


class TaskState(AttributeDict):
    """Represents the state of a task.

    .. attribute state

        Can be one of ``"received"``, ``"started"``, ``"succeeded"``,
                      ``"failed``".

    .. attribute:: hostname

        Hostname of the worker processing the task.

    .. attribute:: task_id

        Unique id of the task being processed.

    .. attribute:: task_name

        Name of the task being processed.

    .. attribute:: args

        The tasks positional arguments.

    .. attribute:: kwargs

        The tasks keyword arguments

    .. attribute:: eta
        When the task is to be executed (``None`` if immediately).

    .. attribute:: time

        The time as a datetime object when the task entered this state.

    """
    fields = ["state", "hostname", "task_id", "task_name",
               "args", "kwargs", "result", "time", "eta"]


class StatePublisher(object):
    Publisher = MonitorPublisher

    def __init__(self, connection=None):
        if not connection:
            connection = DjangoBrokerConnection()
        self.connection = connection
        self.publisher = self.Publisher(connection)

    def publish(state):
        self.publisher.send(dict(state))


class TaskMonitor(object):
    tasks = None

    def __init__(self, connection=None):
        if not connection:
            connection = DjangoBrokerConnection()

        self.connection = connection

        self.tasks = []

    def receive_info(self, message_data, message):
        state = TaskState(**message_data)
        print(state)
        self.tasks.append(TaskState(**message_data))

    def consume(self):
        consumer = MonitorConsumer(self.connection)
        consumer.register_callback(self.receive_info)
        it = consumer.iterconsume()
        while True:
            it.next()
