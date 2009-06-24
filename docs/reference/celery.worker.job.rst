=====================================
 Executable Jobs - celery.worker.job
=====================================

.. currentmodule:: celery.worker.job


.. exception:: UnknownTaskError
    Got an unknown task in the queue. The message is requeued and ignored.


.. function:: jail(task_id, task_name, func, args, kwargs)

    Wraps the task in a jail, which catches all exceptions, and
    saves the status and result of the task execution to the task
    meta backend.

    If the call was successful, it saves the result to the task result
    backend, and sets the task status to ``"DONE"``.

    If the call results in an exception, it saves the exception as the task
    result, and sets the task status to ``"FAILURE"``.

    :param task_id: The id of the task.
    :param task_name: The name of the task.
    :param func: Callable object to execute.
    :param args: List of positional args to pass on to the function.
    :param kwargs: Keyword arguments mapping to pass on to the function.

    :returns: the function return value on success, or
        the exception instance on failure.

.. class:: TaskWrapper(object)

    Class wrapping a task to be run.

    :param task_name: see :attr:`task_name`.

    :param task_id: see :attr:`task_id`.

    :param task_func: see :attr:`task_func`

    :param args: see :attr:`args`

    :param kwargs: see :attr:`kwargs`.

    .. attribute:: task_name

        Kind of task. Must be a name registered in the task registry.

    .. attribute:: task_id

        UUID of the task.

    .. attribute:: task_func

        The tasks callable object.

    .. attribute:: args

        List of positional arguments to apply to the task.

    .. attribute:: kwargs

        Mapping of keyword arguments to apply to the task.

    .. attribute:: message

        The original message sent. Used for acknowledging the message.

    .. classmethod:: from_message(cls, message, message_data, logger)

        Create a :class:`TaskWrapper` from a task message sent by
        :class:`celery.messaging.TaskPublisher`.

        :raises UnknownTaskError: if the message does not describe a task,
            the message is also rejected.

        :returns: :class:`TaskWrapper` instance.

    .. method:: extend_with_default_kwargs(self, loglevel, logfile)
        
        Extend the tasks keyword arguments with standard task arguments.

        These are ``logfile``, ``loglevel``, ``task_id`` and ``task_name``.

    .. method:: execute(loglevel=None, logfile=None)

        Execute the task in a :func:`jail` and store its return value
        and status in the task meta backend.

        :keyword loglevel: The loglevel used by the task.

        :keyword logfile: The logfile used by the task.

    .. method:: on_success(ret_value, meta)

        The handler used if the task was successfully processed 
        without raising an exception.

    .. method:: on_failure(ret_value, meta)
        
        The handler used if the task raised an exception.

    .. method:: execute_using_pool(pool, loglevel=None, logfile=None)

        Like :meth:`execute`, but using the multiprocessing pool.

        :param pool: A :class:`celery.pool.TaskPool` instance.

        :keyword loglevel: The loglevel used by the task.

        :keyword logfile: The logfile used by the task.

        :returns :class:`multiprocessing.AsyncResult` instance.
