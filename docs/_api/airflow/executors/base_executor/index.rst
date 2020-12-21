:mod:`airflow.executors.base_executor`
======================================

.. py:module:: airflow.executors.base_executor

.. autoapi-nested-parse::

   Base executor - this is the base class for all the implemented executors.



Module Contents
---------------

.. data:: PARALLELISM
   :annotation: :int

   

.. data:: NOT_STARTED_MESSAGE
   :annotation: = The executor should be started first!

   

.. data:: CommandType
   

   

.. data:: QueuedTaskInstanceType
   

   

.. data:: EventBufferValueType
   

   

.. py:class:: BaseExecutor(parallelism: int = PARALLELISM)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Class to derive in order to interface with executor-type systems
   like Celery, Kubernetes, Local, Sequential and the likes.

   :param parallelism: how many jobs should run at one time. Set to
       ``0`` for infinity

   .. attribute:: job_id
      :annotation: :Optional[str]

      

   .. attribute:: slots_available
      

      Number of new tasks this executor instance can accept


   
   .. method:: start(self)

      Executors may need to get things started.



   
   .. method:: queue_command(self, task_instance: TaskInstance, command: CommandType, priority: int = 1, queue: Optional[str] = None)

      Queues command to task



   
   .. method:: queue_task_instance(self, task_instance: TaskInstance, mark_success: bool = False, pickle_id: Optional[str] = None, ignore_all_deps: bool = False, ignore_depends_on_past: bool = False, ignore_task_deps: bool = False, ignore_ti_state: bool = False, pool: Optional[str] = None, cfg_path: Optional[str] = None)

      Queues task instance.



   
   .. method:: has_task(self, task_instance: TaskInstance)

      Checks if a task is either queued or running in this executor.

      :param task_instance: TaskInstance
      :return: True if the task is known to this executor



   
   .. method:: sync(self)

      Sync will get called periodically by the heartbeat method.
      Executors should override this to perform gather statuses.



   
   .. method:: heartbeat(self)

      Heartbeat sent to trigger new jobs.



   
   .. method:: order_queued_tasks_by_priority(self)

      Orders the queued tasks by priority.

      :return: List of tuples from the queued_tasks according to the priority.



   
   .. method:: trigger_tasks(self, open_slots: int)

      Triggers tasks

      :param open_slots: Number of open slots



   
   .. method:: change_state(self, key: TaskInstanceKey, state: str, info=None)

      Changes state of the task.

      :param info: Executor information for the task instance
      :param key: Unique key for the task instance
      :param state: State to set for the task.



   
   .. method:: fail(self, key: TaskInstanceKey, info=None)

      Set fail state for the event.

      :param info: Executor information for the task instance
      :param key: Unique key for the task instance



   
   .. method:: success(self, key: TaskInstanceKey, info=None)

      Set success state for the event.

      :param info: Executor information for the task instance
      :param key: Unique key for the task instance



   
   .. method:: get_event_buffer(self, dag_ids=None)

      Returns and flush the event buffer. In case dag_ids is specified
      it will only return and flush events for the given dag_ids. Otherwise
      it returns and flushes all events.

      :param dag_ids: to dag_ids to return events for, if None returns all
      :return: a dict of events



   
   .. method:: execute_async(self, key: TaskInstanceKey, command: CommandType, queue: Optional[str] = None, executor_config: Optional[Any] = None)

      This method will execute the command asynchronously.

      :param key: Unique key for the task instance
      :param command: Command to run
      :param queue: name of the queue
      :param executor_config: Configuration passed to the executor.



   
   .. method:: end(self)

      This method is called when the caller is done submitting job and
      wants to wait synchronously for the job submitted previously to be
      all done.



   
   .. method:: terminate(self)

      This method is called when the daemon receives a SIGTERM



   
   .. method:: try_adopt_task_instances(self, tis: List[TaskInstance])

      Try to adopt running task instances that have been abandoned by a SchedulerJob dying.

      Anything that is not adopted will be cleared by the scheduler (and then become eligible for
      re-scheduling)

      :return: any TaskInstances that were unable to be adopted
      :rtype: list[airflow.models.TaskInstance]



   
   .. staticmethod:: validate_command(command: List[str])

      Check if the command to execute is airflow command



   
   .. method:: debug_dump(self)

      Called in response to SIGUSR2 by the scheduler




