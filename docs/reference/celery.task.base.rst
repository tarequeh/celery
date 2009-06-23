===================================
 Defining Tasks - celery.task.base
===================================

.. currentmodule:: celery.task.base

.. class:: Task

    A task that can be delayed for execution by the ``celery`` daemon.

    All subclasses of :class:`Task` must define the :meth:`run` method,
    which is the actual method the ``celery`` daemon executes.

    The :meth:`run` method supports both positional, and keyword arguments.

    .. attribute:: name

        *REQUIRED* All subclasses of :class:`Task` has to define the
        :attr:`name` attribute. This is the name of the task, registered
        in the task registry, and passed to :func:`delay_task`.

    .. attribute:: type

        The type of task, currently this can be ``regular``, or ``periodic``,
        however if you want a periodic task, you should subclass
        :class:`PeriodicTask` instead.

    .. attribute:: routing_key

        Override the global default ``routing_key`` for this task.

    .. attribute:: mandatory

        If set, the message has mandatory routing. By default the message
        is silently dropped by the broker if it can't be routed to a queue.
        However - If the message is mandatory, an exception will be raised
        instead.

    .. attribute:: immediate:
            
        Request immediate delivery. If the message cannot be routed to a
        task worker immediately, an exception will be raised. This is
        instead of the default behaviour, where the broker will accept and
        queue the message, but with no guarantee that the message will ever
        be consumed.

    .. attribute:: priority:
    
        The message priority. A number from ``0`` to ``9``.

    .. attribute:: ignore_result

        Don't store the status and return value. This means you can't
        use the :class:`celery.result.AsyncResult` to check if the task is
        done, or get its return value. Only use if you need the performance
        and is able live without these features. Any exceptions raised will
        store the return value/status as usual.

    .. attribute:: disable_error_emails

        Disable all error e-mails for this task (only applicable if
        ``settings.SEND_CELERY_ERROR_EMAILS`` is on.)

    :raises NotImplementedError: if the :attr:`name` attribute is not set.

    The resulting class is callable, which if called will apply the
    :meth:`run` method.

    Examples

    This is a simple task just logging a message,

        >>> from celery.task import tasks, Task
        >>> class MyTask(Task):
        ...     name = "mytask"
        ...
        ...     def run(self, some_arg=None, \*\*kwargs):
        ...         logger = self.get_logger(\*\*kwargs)
        ...         logger.info("Running MyTask with arg some_arg=%s" %
        ...                     some_arg))
        ...         return 42
        ... tasks.register(MyTask)

    You can delay the task using the classmethod :meth:`delay`...

        >>> result = MyTask.delay(some_arg="foo")
        >>> result.status # after some time
        'DONE'
        >>> result.result
        42

    ...or using the :func:`delay_task` function, by passing the name of
    the task.

        >>> from celery.task import delay_task
        >>> result = delay_task(MyTask.name, some_arg="foo")

    .. method:: run(\*args, \*\*kwargs)

        *REQUIRED* The actual task.

        All subclasses of :class:`Task` must define the run method.

        :raises NotImplementedError: by default, so you have to override
            this method in your subclass.


    .. method:: get_logger(\*\*kwargs)

        Get process-aware logger object.

        See :func:`celery.log.setup_logger`.

    .. method:: get_publisher()

        Get a celery task message publisher.

        :rtype: :class:`celery.messaging.TaskPublisher`.

        Please be sure to close the AMQP connection when you're done
        with this object, i.e.:

            >>> publisher = self.get_publisher()
            >>> # do something with publisher
            >>> publisher.connection.close()

    .. method:: get_consumer()
        
        Get a celery task message consumer.

        :rtype: :class:`celery.messaging.TaskConsumer`.

        Please be sure to close the AMQP connection when you're done
        with this object. i.e.:

            >>> consumer = self.get_consumer()
            >>> # do something with consumer
            >>> consumer.connection.close()



    .. classmethod:: delay(\*args, \*\*kwargs)

        Delay this task for execution by the ``celery`` daemon(s).

        :param \*args: positional arguments passed on to the task.

        :param \*\*kwargs: keyword arguments passed on to the task.

        :rtype: :class:`celery.result.AsyncResult`

        See :func:`celery.execute.delay_task`.

       
    .. classmethod:: apply_async(args=None, kwargs=None, \*\*options):
        
        Delay this task for execution by the ``celery`` daemon(s).

        :param args: positional arguments passed on to the task.

        :param kwargs: keyword arguments passed on to the task.

        :rtype: :class:`celery.result.AsyncResult`

        See :func:`celery.execute.apply_async`.


.. class:: TaskSet

    A task containing several subtasks, making it possible
    to track how many, or when all of the tasks has been completed.

    :param task: The task class or name.
        Can either be a fully qualified task name, or a task class.

    :param args: A list of args, kwargs pairs.
        e.g. ``[[args1, kwargs1], [args2, kwargs2], ..., [argsN, kwargsN]]``


    .. attribute:: task_name

        The name of the task.

    .. attribute:: arguments

        The arguments, as passed to the task set constructor.

    .. attribute:: total

        Total number of tasks in this task set.

    Example

        >>> from djangofeeds.tasks import RefreshFeedTask
        >>> taskset = TaskSet(RefreshFeedTask, args=[
        ...                 [], {"feed_url": "http://cnn.com/rss"},
        ...                 [], {"feed_url": "http://bbc.com/rss"},
        ...                 [], {"feed_url": "http://xkcd.com/rss"}])

        >>> taskset_result = taskset.run()
        >>> list_of_return_values = taskset.join()

   
    .. method:: run(connect_timeout=AMQP_CONNECTION_TIMEOUT) 
        
        Run all tasks in the taskset.

        :returns: A :class:`celery.result.TaskSetResult` instance.

        Example

            >>> ts = TaskSet(RefreshFeedTask, [
            ...         ["http://foo.com/rss", {}],
            ...         ["http://bar.com/rss", {}],
            ... )
            >>> result = ts.run()
            >>> result.taskset_id
            "d2c9b261-8eff-4bfb-8459-1e1b72063514"
            >>> result.subtask_ids
            ["b4996460-d959-49c8-aeb9-39c530dcde25",
            "598d2d18-ab86-45ca-8b4f-0779f5d6a3cb"]
            >>> result.waiting()
            True
            >>> time.sleep(10)
            >>> result.ready()
            True
            >>> result.successful()
            True
            >>> result.failed()
            False
            >>> result.join()
            [True, True]

    .. method:: iterate()

        Iterate over the results returned after calling :meth:`run`.

        If any of the tasks raises an exception, the exception will
        be re-raised.

    .. method:: join(timeout=None)

        Gather the results for all of the tasks in the taskset,
        and return a list with them ordered by the order of which they
        were called.

        :keyword timeout: The time in seconds, how long
            it will wait for results, before the operation times out.

        :raises celery.timer.TimeoutError: if ``timeout`` is not ``None``
            and the operation takes longer than ``timeout`` seconds.

        If any of the tasks raises an exception, the exception
        will be reraised by :meth:`join`.

        :returns: list of return values for all tasks in the taskset.

    .. classmethod:: remote_execute(func, args)

        Apply ``args`` to function by distributing the args to the
        celery server(s).

    .. classmethod:: map(func, args, timeout=None)

        Distribute processing of the arguments and collect the results.

    .. classmethod:: map_async(func, args, timeout=None)

        Distribute processing of the arguments and collect the results
        asynchronously.

        :returns: :class:`celery.result.AsyncResult` instance.


.. class:: PeriodicTask

    A periodic task is a task that behaves like a :manpage:`cron` job.

    .. attribute:: run_every

        *REQUIRED* Defines how often the task is run (its interval),
        it can be either a :class:`datetime.timedelta` object or an
        integer specifying the time in seconds.

    :raises NotImplementedError: if the :attr:`run_every` attribute is
        not defined.

    You have to register the periodic task in the task registry.

    Example

        >>> from celery.task import tasks, PeriodicTask
        >>> from datetime import timedelta
        >>> class MyPeriodicTask(PeriodicTask):
        ...     name = "my_periodic_task"
        ...     run_every = timedelta(seconds=30)
        ...
        ...     def run(self, \*\*kwargs):
        ...         logger = self.get_logger(\*\*kwargs)
        ...         logger.info("Running MyPeriodicTask")
        >>> tasks.register(MyPeriodicTask)
