 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

airflow.providers.cncf.kubernetes.executors.local_kubernetes_executor
=====================================================================

.. py:module:: airflow.providers.cncf.kubernetes.executors.local_kubernetes_executor


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.executors.local_kubernetes_executor.LocalKubernetesExecutor


Module Contents
---------------

.. py:class:: LocalKubernetesExecutor(local_executor, kubernetes_executor)

   Bases: :py:obj:`airflow.executors.base_executor.BaseExecutor`


   Chooses between LocalExecutor and KubernetesExecutor based on the queue defined on the task.

   When the task's queue is the value of ``kubernetes_queue`` in section ``[local_kubernetes_executor]``
   of the configuration (default value: ``kubernetes``), KubernetesExecutor is selected to run the task,
   otherwise, LocalExecutor is used.


   .. py:attribute:: supports_ad_hoc_ti_run
      :type:  bool
      :value: True



   .. py:attribute:: supports_pickling
      :type:  bool
      :value: False



   .. py:attribute:: supports_sentry
      :type:  bool
      :value: False



   .. py:attribute:: is_local
      :type:  bool
      :value: False



   .. py:attribute:: is_single_threaded
      :type:  bool
      :value: False



   .. py:attribute:: is_production
      :type:  bool
      :value: True



   .. py:attribute:: serve_logs
      :type:  bool
      :value: True



   .. py:attribute:: change_sensor_mode_to_reschedule
      :type:  bool
      :value: False



   .. py:attribute:: callback_sink
      :type:  airflow.callbacks.base_callback_sink.BaseCallbackSink | None
      :value: None



   .. py:attribute:: KUBERNETES_QUEUE


   .. py:attribute:: local_executor


   .. py:attribute:: kubernetes_executor


   .. py:property:: queued_tasks
      :type: dict[airflow.models.taskinstance.TaskInstanceKey, airflow.executors.base_executor.QueuedTaskInstanceType]


      Return queued tasks from local and kubernetes executor.



   .. py:property:: running
      :type: set[airflow.models.taskinstance.TaskInstanceKey]


      Return running tasks from local and kubernetes executor.



   .. py:property:: job_id
      :type: int | str | None


      Inherited attribute from BaseExecutor.

      Since this is not really an executor, but a wrapper of executors
      we implemented it as property, so we can have custom setter.



   .. py:method:: start()

      Start local and kubernetes executor.



   .. py:property:: slots_available
      :type: int


      Number of new tasks this executor instance can accept.



   .. py:property:: slots_occupied

      Number of tasks this executor instance is currently managing.



   .. py:method:: queue_command(task_instance, command, priority = 1, queue = None)

      Queues command via local or kubernetes executor.



   .. py:method:: queue_task_instance(task_instance, mark_success = False, ignore_all_deps = False, ignore_depends_on_past = False, wait_for_past_depends_before_skipping = False, ignore_task_deps = False, ignore_ti_state = False, pool = None, cfg_path = None, **kwargs)

      Queues task instance via local or kubernetes executor.



   .. py:method:: get_task_log(ti, try_number)

      Fetch task log from kubernetes executor.



   .. py:method:: has_task(task_instance)

      Check if a task is either queued or running in either local or kubernetes executor.

      :param task_instance: TaskInstance
      :return: True if the task is known to this executor



   .. py:method:: heartbeat()

      Heartbeat sent to trigger new jobs in local and kubernetes executor.



   .. py:method:: get_event_buffer(dag_ids = None)

      Return and flush the event buffer from local and kubernetes executor.

      :param dag_ids: dag_ids to return events for, if None returns all
      :return: a dict of events



   .. py:method:: try_adopt_task_instances(tis)

      Try to adopt running task instances that have been abandoned by a SchedulerJob dying.

      Anything that is not adopted will be cleared by the scheduler (and then become eligible for
      re-scheduling)

      :return: any TaskInstances that were unable to be adopted



   .. py:method:: cleanup_stuck_queued_tasks(tis)

      Handle remnants of tasks that were failed because they were stuck in queued.

      Tasks can get stuck in queued. If such a task is detected, it will be marked
      as `UP_FOR_RETRY` if the task instance has remaining retries or marked as `FAILED`
      if it doesn't.

      :param tis: List of Task Instances to clean up
      :return: List of readable task instances for a warning message



   .. py:method:: revoke_task(*, ti)

      Attempt to remove task from executor.

      It should attempt to ensure that the task is no longer running on the worker,
      and ensure that it is cleared out from internal data structures.

      It should *not* change the state of the task in airflow, or add any events
      to the event buffer.

      It should not raise any error.

      :param ti: Task instance to remove



   .. py:method:: end()

      End local and kubernetes executor.



   .. py:method:: terminate()

      Terminate local and kubernetes executor.



   .. py:method:: debug_dump()

      Debug dump; called in response to SIGUSR2 by the scheduler.



   .. py:method:: send_callback(request)

      Send callback for execution.

      :param request: Callback request to be executed.



   .. py:method:: get_cli_commands()
      :staticmethod:


      Vends CLI commands to be included in Airflow CLI.

      Override this method to expose commands via Airflow CLI to manage this executor. This can
      be commands to setup/teardown the executor, inspect state, etc.
      Make sure to choose unique names for those commands, to avoid collisions.
