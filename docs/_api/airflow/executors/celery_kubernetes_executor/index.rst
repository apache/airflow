:mod:`airflow.executors.celery_kubernetes_executor`
===================================================

.. py:module:: airflow.executors.celery_kubernetes_executor


Module Contents
---------------

.. py:class:: CeleryKubernetesExecutor(celery_executor, kubernetes_executor)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   CeleryKubernetesExecutor consists of CeleryExecutor and KubernetesExecutor.
   It chooses an executor to use based on the queue defined on the task.
   When the queue is `kubernetes`, KubernetesExecutor is selected to run the task,
   otherwise, CeleryExecutor is used.

   .. attribute:: KUBERNETES_QUEUE
      

      

   .. attribute:: queued_tasks
      

      Return queued tasks from celery and kubernetes executor


   .. attribute:: running
      

      Return running tasks from celery and kubernetes executor


   
   .. method:: start(self)

      Start celery and kubernetes executor



   
   .. method:: queue_command(self, task_instance: TaskInstance, command: CommandType, priority: int = 1, queue: Optional[str] = None)

      Queues command via celery or kubernetes executor



   
   .. method:: queue_task_instance(self, task_instance: TaskInstance, mark_success: bool = False, pickle_id: Optional[str] = None, ignore_all_deps: bool = False, ignore_depends_on_past: bool = False, ignore_task_deps: bool = False, ignore_ti_state: bool = False, pool: Optional[str] = None, cfg_path: Optional[str] = None)

      Queues task instance via celery or kubernetes executor



   
   .. method:: has_task(self, task_instance: TaskInstance)

      Checks if a task is either queued or running in either celery or kubernetes executor.

      :param task_instance: TaskInstance
      :return: True if the task is known to this executor



   
   .. method:: heartbeat(self)

      Heartbeat sent to trigger new jobs in celery and kubernetes executor



   
   .. method:: get_event_buffer(self, dag_ids=None)

      Returns and flush the event buffer from celery and kubernetes executor

      :param dag_ids: to dag_ids to return events for, if None returns all
      :return: a dict of events



   
   .. method:: try_adopt_task_instances(self, tis: List[TaskInstance])

      Try to adopt running task instances that have been abandoned by a SchedulerJob dying.

      Anything that is not adopted will be cleared by the scheduler (and then become eligible for
      re-scheduling)

      :return: any TaskInstances that were unable to be adopted
      :rtype: list[airflow.models.TaskInstance]



   
   .. method:: end(self)

      End celery and kubernetes executor



   
   .. method:: terminate(self)

      Terminate celery and kubernetes executor



   
   .. method:: _router(self, simple_task_instance: SimpleTaskInstance)

      Return either celery_executor or kubernetes_executor

      :param simple_task_instance: SimpleTaskInstance
      :return: celery_executor or kubernetes_executor
      :rtype: Union[CeleryExecutor, KubernetesExecutor]



   
   .. method:: debug_dump(self)

      Called in response to SIGUSR2 by the scheduler




