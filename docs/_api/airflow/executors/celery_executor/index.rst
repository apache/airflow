:mod:`airflow.executors.celery_executor`
========================================

.. py:module:: airflow.executors.celery_executor

.. autoapi-nested-parse::

   CeleryExecutor

   .. seealso::
       For more information on how the CeleryExecutor works, take a look at the guide:
       :ref:`executor:CeleryExecutor`



Module Contents
---------------

.. data:: log
   

   

.. data:: CELERY_FETCH_ERR_MSG_HEADER
   :annotation: = Error fetching Celery task state

   

.. data:: CELERY_SEND_ERR_MSG_HEADER
   :annotation: = Error sending Celery task

   

.. data:: OPERATION_TIMEOUT
   

   To start the celery worker, run the command:
   airflow celery worker


.. data:: celery_configuration
   

   

.. data:: app
   

   

.. function:: execute_command(command_to_exec: CommandType) -> None
   Executes command.


.. function:: _execute_in_fork(command_to_exec: CommandType) -> None

.. function:: _execute_in_subprocess(command_to_exec: CommandType) -> None

.. py:class:: ExceptionWithTraceback(exception: Exception, exception_traceback: str)

   Wrapper class used to propagate exceptions to parent processes from subprocesses.

   :param exception: The exception to wrap
   :type exception: Exception
   :param exception_traceback: The stacktrace to wrap
   :type exception_traceback: str


.. data:: TaskInstanceInCelery
   

   

.. function:: send_task_to_executor(task_tuple: TaskInstanceInCelery) -> Tuple[TaskInstanceKey, CommandType, Union[AsyncResult, ExceptionWithTraceback]]
   Sends task to executor.


.. function:: on_celery_import_modules(*args, **kwargs)
   Preload some "expensive" airflow modules so that every task process doesn't have to import it again and
   again.

   Loading these for each task adds 0.3-0.5s *per task* before the task can run. For long running tasks this
   doesn't matter, but for short tasks this starts to be a noticeable impact.


.. py:class:: CeleryExecutor

   Bases: :class:`airflow.executors.base_executor.BaseExecutor`

   CeleryExecutor is recommended for production use of Airflow. It allows
   distributing the execution of task instances to multiple worker nodes.

   Celery is a simple, flexible and reliable distributed system to process
   vast amounts of messages, while providing operations with the tools
   required to maintain such a system.

   
   .. method:: start(self)



   
   .. method:: _num_tasks_per_send_process(self, to_send_count: int)

      How many Celery tasks should each worker process send.

      :return: Number of tasks that should be sent per process
      :rtype: int



   
   .. method:: trigger_tasks(self, open_slots: int)

      Overwrite trigger_tasks function from BaseExecutor

      :param open_slots: Number of open slots
      :return:



   
   .. method:: _process_tasks(self, task_tuples_to_send: List[TaskInstanceInCelery])



   
   .. method:: _send_tasks_to_celery(self, task_tuples_to_send: List[TaskInstanceInCelery])



   
   .. method:: sync(self)



   
   .. method:: _check_for_stalled_adopted_tasks(self)

      See if any of the tasks we adopted from another Executor run have not
      progressed after the configured timeout.

      If they haven't, they likely never made it to Celery, and we should
      just resend them. We do that by clearing the state and letting the
      normal scheduler loop deal with that



   
   .. method:: debug_dump(self)

      Called in response to SIGUSR2 by the scheduler



   
   .. method:: update_all_task_states(self)

      Updates states of the tasks.



   
   .. method:: change_state(self, key: TaskInstanceKey, state: str, info=None)



   
   .. method:: update_task_state(self, key: TaskInstanceKey, state: str, info: Any)

      Updates state of a single task.



   
   .. method:: end(self, synchronous: bool = False)



   
   .. method:: execute_async(self, key: TaskInstanceKey, command: CommandType, queue: Optional[str] = None, executor_config: Optional[Any] = None)

      Do not allow async execution for Celery executor.



   
   .. method:: terminate(self)



   
   .. method:: try_adopt_task_instances(self, tis: List[TaskInstance])




.. function:: fetch_celery_task_state(async_result: AsyncResult) -> Tuple[str, Union[str, ExceptionWithTraceback], Any]
   Fetch and return the state of the given celery task. The scope of this function is
   global so that it can be called by subprocesses in the pool.

   :param async_result: a tuple of the Celery task key and the async Celery object used
       to fetch the task's state
   :type async_result: tuple(str, celery.result.AsyncResult)
   :return: a tuple of the Celery task key and the Celery state and the celery info
       of the task
   :rtype: tuple[str, str, str]


.. function:: _tasks_list_to_task_ids(async_tasks) -> Set[str]

.. py:class:: BulkStateFetcher(sync_parralelism=None)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Gets status for many Celery tasks using the best method available

   If BaseKeyValueStoreBackend is used as result backend, the mget method is used.
   If DatabaseBackend is used as result backend, the SELECT ...WHERE task_id IN (...) query is used
   Otherwise, multiprocessing.Pool will be used. Each task status will be downloaded individually.

   
   .. method:: get_many(self, async_results)

      Gets status for many Celery tasks using the best method available.



   
   .. method:: _get_many_from_kv_backend(self, async_tasks)



   
   .. method:: _get_many_from_db_backend(self, async_tasks)



   
   .. staticmethod:: _prepare_state_and_info_by_task_dict(task_ids, task_results_by_task_id)



   
   .. method:: _get_many_using_multiprocessing(self, async_results)




