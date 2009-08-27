"""celery.log"""
import os
import sys
import time
import logging
from celery.conf import LOG_FORMAT, DAEMON_LOG_LEVEL
from multiprocessing.process import Process
from multiprocessing.queues import SimpleQueue


def setup_logger(loglevel=DAEMON_LOG_LEVEL, logfile=None, format=LOG_FORMAT,
        **kwargs):
    """Setup the ``multiprocessing`` logger. If ``logfile`` is not specified,
    ``stderr`` is used.

    Returns logger object.
    """
    import multiprocessing
    logger = multiprocessing.get_logger()
    logger.setLevel(loglevel)
    if logger.handlers:
        return logger
    if logfile:
        if hasattr(logfile, "write"):
            log_file_handler = logging.StreamHandler(logfile)
        else:
            log_file_handler = logging.FileHandler(logfile)
        formatter = logging.Formatter(format)
        log_file_handler.setFormatter(formatter)
        logger.addHandler(log_file_handler)
    else:
        multiprocessing.log_to_stderr()
    return logger


def emergency_error(logfile, message):
    """Emergency error logging, for when there's no standard file
    descriptors open because the process has been daemonized or for
    some other reason."""
    logfh_needs_to_close = False
    if not logfile:
        logfile = sys.stderr
    if hasattr(logfile, "write"):
        logfh = logfile
    else:
        logfh = open(logfile, "a")
        logfh_needs_to_close = True
    logfh.write("[%(asctime)s: FATAL/%(pid)d]: %(message)s\n" % {
                    "asctime": time.asctime(),
                    "pid": os.getpid(),
                    "message": message})
    if logfh_needs_to_close:
        logfh.close()


class Logwriter(Process):
    """Process flushing log messages to a logfile."""

    def start(self, log_queue, logfile="process.log"):
        self.log_queue = log_queue
        self.logfile = logfile
        super(Logwriter, self).start()

    def run(self):
        self.process_logs(self.log_queue, self.logfile)

    def process_logs(self, log_queue, logfile):
        need_to_close_fh = False
        logfh = logfile
        if isinstance(logfile, basestring):
            need_to_close_fh = True
            logfh = open(logfile, "a")

        logfh = open(logfile, "a")
        while 1:
            message = log_queue.get()
            if message is None: # received sentinel
                break
            logfh.write(message)

        log_queue.put(None) # cascade sentinel

        if need_to_close_fh:
            logfh.close()


class QueueLogger(object):
    """File like object logging contents to a queue,
    which the :class:`Logwriter` later writes to disk."""

    def __init__(self, log_queue, log_process):
        self.log_queue = log_queue
        self.log_process = log_process

    @classmethod
    def start(cls):
        log_queue = SimpleQueue()
        log_process = Logwriter()
        log_process.start(log_queue)
        return cls(log_queue, log_process)

    def write(self, message):
        self.log_queue.put(message)

    def stop(self):
        self.log_queue.put(None) # send sentinel

    def flush(self):
        """This filehandle does automatic flushing. So flush requests
        are ignored."""
        pass
