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

airflow.providers.cncf.kubernetes.triggers.pod
==============================================

.. py:module:: airflow.providers.cncf.kubernetes.triggers.pod


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.triggers.pod.ContainerState
   airflow.providers.cncf.kubernetes.triggers.pod.KubernetesPodTrigger


Module Contents
---------------

.. py:class:: ContainerState

   Bases: :py:obj:`str`, :py:obj:`enum.Enum`


   Possible container states.

   See https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase.


   .. py:attribute:: WAITING
      :value: 'waiting'



   .. py:attribute:: RUNNING
      :value: 'running'



   .. py:attribute:: TERMINATED
      :value: 'terminated'



   .. py:attribute:: FAILED
      :value: 'failed'



   .. py:attribute:: UNDEFINED
      :value: 'undefined'



.. py:class:: KubernetesPodTrigger(pod_name, pod_namespace, trigger_start_time, base_container_name, kubernetes_conn_id = None, poll_interval = 2, cluster_context = None, config_dict = None, in_cluster = None, get_logs = True, startup_timeout = 120, startup_check_interval = 5, on_finish_action = 'delete_pod', last_log_time = None, logging_interval = None)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`


   KubernetesPodTrigger run on the trigger worker to check the state of Pod.

   :param pod_name: The name of the pod.
   :param pod_namespace: The namespace of the pod.
   :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
       for the Kubernetes cluster.
   :param cluster_context: Context that points to kubernetes cluster.
   :param config_dict: Content of kubeconfig file in dict format.
   :param poll_interval: Polling period in seconds to check for the status.
   :param trigger_start_time: time in Datetime format when the trigger was started
   :param in_cluster: run kubernetes client with in_cluster configuration.
   :param get_logs: get the stdout of the container as logs of the tasks.
   :param startup_timeout: timeout in seconds to start up the pod.
   :param startup_check_interval: interval in seconds to check if the pod has already started.
   :param on_finish_action: What to do when the pod reaches its final state, or the execution is interrupted.
       If "delete_pod", the pod will be deleted regardless its state; if "delete_succeeded_pod",
       only succeeded pod will be deleted. You can set to "keep_pod" to keep the pod.
   :param logging_interval: number of seconds to wait before kicking it back to
       the operator to print latest logs. If ``None`` will wait until container done.
   :param last_log_time: where to resume logs from


   .. py:attribute:: pod_name


   .. py:attribute:: pod_namespace


   .. py:attribute:: trigger_start_time


   .. py:attribute:: base_container_name


   .. py:attribute:: kubernetes_conn_id
      :value: None



   .. py:attribute:: poll_interval
      :value: 2



   .. py:attribute:: cluster_context
      :value: None



   .. py:attribute:: config_dict
      :value: None



   .. py:attribute:: in_cluster
      :value: None



   .. py:attribute:: get_logs
      :value: True



   .. py:attribute:: startup_timeout
      :value: 120



   .. py:attribute:: startup_check_interval
      :value: 5



   .. py:attribute:: last_log_time
      :value: None



   .. py:attribute:: logging_interval
      :value: None



   .. py:attribute:: on_finish_action


   .. py:method:: serialize()

      Serialize KubernetesCreatePodTrigger arguments and classpath.



   .. py:method:: run()
      :async:


      Get current pod status and yield a TriggerEvent.



   .. py:property:: hook
      :type: airflow.providers.cncf.kubernetes.hooks.kubernetes.AsyncKubernetesHook



   .. py:method:: define_container_state(pod)


   .. py:method:: should_wait(pod_phase, container_state)
      :staticmethod:
