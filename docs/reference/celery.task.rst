==============================================
 Task Information and Utilities - celery.task
==============================================

.. currentmodule:: celery.task

.. function:: discard_all(connect_timeout=AMQP_CONNECTION_TIMEOUT)
    
    Discard all waiting tasks.

    This will ignore all tasks waiting for execution, and they will
    be deleted from the messaging server.

    :returns: the number of tasks discarded.

    :rtype: int

.. function:: is_done(task_id)

    Returns ``True`` if task with ``task_id`` has been executed.

    :rtype: bool

.. function:: dmap(func, args, timeout=None)

    Distribute processing of the arguments and collect the results.

    Example

        >>> from celery.task import map
        >>> import operator
        >>> dmap(operator.add, [[2, 2], [4, 4], [8, 8]])
        [4, 8, 16]

.. function:: dmap_async(func, args, timeout=None)

    Distribute processing of the arguments and collect the results
    asynchronously.

    :returns: :class:`celery.result.AsyncResult` object.

    Example

        >>> from celery.task import dmap_async
        >>> import operator
        >>> presult = dmap_async(operator.add, [[2, 2], [4, 4], [8, 8]])
        >>> presult
        <AsyncResult: 373550e8-b9a0-4666-bc61-ace01fa4f91d>
        >>> presult.status
        'DONE'
        >>> presult.result
        [4, 8, 16]

.. function:: execute_remote(func, \*args, \*\*kwargs):
    
    Execute arbitrary function/object remotely.

    :param func: A callable function or object.

    :param \*args: Positional arguments to apply to the function.

    :param \*\*kwargs: Keyword arguments to apply to the function.

    The object must be picklable, so you can't use lambdas or functions
    defined in the REPL (the objects must have an associated module).

    :returns: class:`celery.result.AsyncResult`.

.. function:: ping()

    Test if the server is alive.

    Example:

        >>> from celery.task import ping
        >>> ping()
        'pong'
