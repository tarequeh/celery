"""celery.storage"""
from functools import partial
from celery.loaders import settings
import sys

DEFAULT_STORAGE = "database"
DEFAULT_PERIODIC_STATUS_BACKEND = "database"
CELERY_STORAGE = getattr(settings, "CELERY_STORAGE",
                    getattr(settings, "CELERY_BACKEND",
                            DEFAULT_STORAGE))
CELERY_PERIODIC_STATUS_BACKEND = getattr(settings,
                                    "CELERY_PERIODIC_STATUS_BACKEND",
                                    DEFAULT_PERIODIC_STATUS_BACKEND)


def get_backend_cls(backend):
    """Get backend class by name.

    If the name does not include "``.``" (is not fully qualified),
    ``"celery.storage."`` will be prepended to the name. e.g.
    ``"database"`` becomes ``"celery.storage.database"``.

    """
    if backend.find(".") == -1:
        backend = "celery.storage.%s" % backend
    __import__(backend)
    backend_module = sys.modules[backend]
    return getattr(backend_module, "Backend")

"""
.. function:: get_default_storage_cls()

    Get the backend class specified in :setting:`CELERY_STORAGE`.

"""
get_default_storage_cls = partial(get_backend_cls, CELERY_STORAGE)


"""
.. function:: get_default_periodicstatus_backend_cls()

    Get the backend class specified in
    :setting:`CELERY_PERIODIC_STATUS_BACKEND`.

"""
get_default_periodicstatus_backend_cls = partial(get_backend_cls,
                                            CELERY_PERIODIC_STATUS_BACKEND)


"""
.. class:: DefaultStorage

    The default storage backend class used for storing task results and
    status, specified in :setting:`CELERY_STORAGE`.

"""
DefaultStorage = get_default_storage_cls()


"""
.. class:: DefaultPeriodicStatusBackend

    The default backend for storing periodic task metadata, specified
    in :setting:`CELERY_PERIODIC_STATUS_BACKEND`.

"""
DefaultPeriodicStatusBackend = get_default_periodicstatus_backend_cls()

"""
.. data:: default_storage

    An instance of :class:`DefaultStorage`.

"""
default_storage = DefaultStorage()

"""
.. data:: default_periodic_status_backend

    An instance of :class:`DefaultPeriodicStatusBackend`.

"""
default_periodic_status_backend = DefaultPeriodicStatusBackend()
