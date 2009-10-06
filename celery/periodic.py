from celery.registry import tasks
from celery.serialization import pickle
from UserDict import UserDict
import atexit

STORED_STATUSES = "celery-periodic-status.pickle"


class PickleStore(UserDict):
    pickle = pickle

    def __init__(self, filename):
        self.filename = filename
        self.data = self.load()
        self._saved = False
       
    def save_atexit(self):
        if self._saved:
        	return
        self._saved = True
        self.save()

    def save(self):
        fh = open(self.filename, "w")
        try:
            pickled = self.pickle.dump(self.data.encode("zlib"), fh)
        finally:
            fh.close()

        return self._with_fh(save_fh)

    def load(self):
        try:
            fh = open(self.filename, "r")
        except IOError, exc:
            if exc.errno = errno.ENOENT:
            	return {}
            raise

        try:
            return pickle.load(fh).decode("zlib")
        finally:
            fh.close()
    

class PeriodicTaskRunner(object):
    statuses = None

    def __init__(self):
        if self.__class__.statuses is None:
            # Statuses is global for all instances of this class.
            self.__class__.statuses = PickleStore(STORED_STATUSES)
            atexit.register(statuses.save_atexit)

    def refresh_statuses(self):
        for task_name, task_obj in tasks.get_all_periodic():
            if task not in statuses:
                statuses[task_name] = (task_obj, datetime.now(), 0)

    def run_waiting_tasks(self):
        executed = []
        now = datetime.now()
        for task_name, task_info in statuses:
            task_obj, last_run_at, total_run_count = task_info
            run_at = last_run_at + run_every
            if now > run_at:
                statuses[task_name] = (task_obj, now, total_run_count + 1)
                async_result = task_obj.apply_async()
                executed.append((task_obj, async_result.task_id))
        return executed
