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

airflow.providers.cncf.kubernetes.executors.kubernetes_executor
===============================================================

.. py:module:: airflow.providers.cncf.kubernetes.executors.kubernetes_executor

.. autoapi-nested-parse::

   KubernetesExecutor.

   .. seealso::
       For more information on how the KubernetesExecutor works, take a look at the guide:
       :doc:`/kubernetes_executor`



Attributes
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.executors.kubernetes_executor.ARG_NAMESPACE
   airflow.providers.cncf.kubernetes.executors.kubernetes_executor.ARG_MIN_PENDING_MINUTES
   airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KUBERNETES_COMMANDS


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor


Module Contents
---------------

.. py:data:: ARG_NAMESPACE

.. py:data:: ARG_MIN_PENDING_MINUTES

.. py:data:: KUBERNETES_COMMANDS

.. py:class:: KubernetesExecutor

   Bases: :py:obj:`airflow.executors.base_executor.BaseExecutor`


   Executor for Kubernetes.


   .. py:attribute:: RUNNING_POD_LOG_LINES
      :value: 100



   .. py:attribute:: supports_ad_hoc_ti_run
      :type:  bool
      :value: True



   .. py:attribute:: kube_config


   .. py:attribute:: task_queue
      :type:  queue.Queue[airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types.KubernetesJobType]


   .. py:attribute:: result_queue
      :type:  queue.Queue[airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types.KubernetesResultsType]


   .. py:attribute:: kube_scheduler
      :type:  airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.AirflowKubernetesScheduler | None
      :value: None



   .. py:attribute:: kube_client
      :type:  kubernetes.client.CoreV1Api | None
      :value: None



   .. py:attribute:: scheduler_job_id
      :type:  str | None
      :value: None



   .. py:attribute:: last_handled
      :type:  dict[airflow.models.taskinstancekey.TaskInstanceKey, float]


   .. py:attribute:: kubernetes_queue
      :type:  str | None
      :value: None



   .. py:attribute:: task_publish_retries
      :type:  collections.Counter[airflow.models.taskinstancekey.TaskInstanceKey]


   .. py:attribute:: task_publish_max_retries


   .. py:method:: get_pod_combined_search_str_to_pod_map()

      List the worker pods owned by this scheduler and create a map containing pod combined search str -> pod.

      For every pod, it creates two below entries in the map
      dag_id={dag_id},task_id={task_id},airflow-worker={airflow_worker},<map_index={map_index}>,run_id={run_id}



   .. py:method:: start()

      Start the executor.



   .. py:method:: execute_async(key, command, queue = None, executor_config = None)

      Execute task asynchronously.



   .. py:method:: sync()

      Synchronize task state.



   .. py:method:: get_task_log(ti, try_number)

      Return the task logs.

      :param ti: A TaskInstance object
      :param try_number: current try_number to read log from
      :return: tuple of logs and messages



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

      Revoke task that may be running.

      :param ti: task instance to revoke



   .. py:method:: adopt_launched_task(kube_client, pod, tis_to_flush_by_key)

      Patch existing pod so that the current KubernetesJobWatcher can monitor it via label selectors.

      :param kube_client: kubernetes client for speaking to kube API
      :param pod: V1Pod spec that we will patch with new label
      :param tis_to_flush_by_key: TIs that will be flushed if they aren't adopted



   .. py:method:: end()

      Shut down the executor.



   .. py:method:: terminate()

      Terminate the executor is not doing anything.



   .. py:method:: get_cli_commands()
      :staticmethod:


      Vends CLI commands to be included in Airflow CLI.

      Override this method to expose commands via Airflow CLI to manage this executor. This can
      be commands to setup/teardown the executor, inspect state, etc.
      Make sure to choose unique names for those commands, to avoid collisions.
