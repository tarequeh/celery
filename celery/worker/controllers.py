"""

Worker Controller Threads

Documentation for this module is in
``docs/reference/celery.worker.controllers.rst``.

"""
from celery.backends import default_periodic_status_backend
from Queue import Empty as QueueEmpty
from datetime import datetime
import threading
import time


class Mediator(threading.Thread):
    """Thread continuously passing tasks in the queue
    to the pool."""

    def __init__(self, bucket_queue, callback):
        super(Mediator, self).__init__()
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self.bucket_queue = bucket_queue
        self.callback = callback

    def run(self):
        """Run the thread. Not used directly, instead use :meth:`start`."""
        while True:
            if self._shutdown.isSet():
                break
            # This blocks until there's a message in the queue.
            try:
                task = self.bucket_queue.get(timeout=1)
            except QueueEmpty:
                pass
            else:
                self.callback(task)
        self._stopped.set() # indicate that we are stopped

    def stop(self):
        """Shutdown the thread."""
        self._shutdown.set()
        self._stopped.wait() # block until this thread is done


class PeriodicWorkController(threading.Thread):
    """A thread that continuously checks if there are
    :class:`celery.task.PeriodicTask` tasks waiting for execution,
    and executes them. It also finds tasks in the hold queue that is
    ready for execution and moves them to the bucket queue."""

    def __init__(self, bucket_queue, hold_queue):
        super(PeriodicWorkController, self).__init__()
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self.hold_queue = hold_queue
        self.bucket_queue = bucket_queue

    def run(self):
        """Run the thread. Not used directly, instead use :meth:`start`."""
        while True:
            if self._shutdown.isSet():
                break
            default_periodic_status_backend.run_periodic_tasks()
            self.process_hold_queue()
            time.sleep(1)
        self._stopped.set() # indicate that we are stopped

    def process_hold_queue(self):
        """Finds paused tasks that are ready for execution and move
        them to the bucket queue."""
        try:
            task, eta = self.hold_queue.get_nowait()
        except QueueEmpty:
            return
        if datetime.now() >= eta:
            self.bucket_queue.put(task)
        else:
            self.hold_queue.put((task, eta))

    def stop(self):
        """Shutdown the thread."""
        self._shutdown.set()
        self._stopped.wait() # block until this thread is done
