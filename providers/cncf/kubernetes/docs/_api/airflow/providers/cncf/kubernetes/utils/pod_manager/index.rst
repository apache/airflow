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

airflow.providers.cncf.kubernetes.utils.pod_manager
===================================================

.. py:module:: airflow.providers.cncf.kubernetes.utils.pod_manager

.. autoapi-nested-parse::

   Launches PODs.



Exceptions
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.utils.pod_manager.PodLaunchFailedException
   airflow.providers.cncf.kubernetes.utils.pod_manager.PodLaunchTimeoutException
   airflow.providers.cncf.kubernetes.utils.pod_manager.PodNotFoundException


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.utils.pod_manager.PodPhase
   airflow.providers.cncf.kubernetes.utils.pod_manager.PodOperatorHookProtocol
   airflow.providers.cncf.kubernetes.utils.pod_manager.PodLoggingStatus
   airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager
   airflow.providers.cncf.kubernetes.utils.pod_manager.OnFinishAction


Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.utils.pod_manager.should_retry_start_pod
   airflow.providers.cncf.kubernetes.utils.pod_manager.get_container_status
   airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running
   airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_completed
   airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_succeeded
   airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_wait
   airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_terminated
   airflow.providers.cncf.kubernetes.utils.pod_manager.get_container_termination_message
   airflow.providers.cncf.kubernetes.utils.pod_manager.check_exception_is_kubernetes_api_unauthorized
   airflow.providers.cncf.kubernetes.utils.pod_manager.is_log_group_marker


Module Contents
---------------

.. py:exception:: PodLaunchFailedException

   Bases: :py:obj:`airflow.exceptions.AirflowException`


   When pod launching fails in KubernetesPodOperator.


.. py:function:: should_retry_start_pod(exception)

   Check if an Exception indicates a transient error and warrants retrying.


.. py:class:: PodPhase

   Possible pod phases.

   See https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase.


   .. py:attribute:: PENDING
      :value: 'Pending'



   .. py:attribute:: RUNNING
      :value: 'Running'



   .. py:attribute:: FAILED
      :value: 'Failed'



   .. py:attribute:: SUCCEEDED
      :value: 'Succeeded'



   .. py:attribute:: terminal_states


.. py:class:: PodOperatorHookProtocol

   Bases: :py:obj:`Protocol`


   Protocol to define methods relied upon by KubernetesPodOperator.

   Subclasses of KubernetesPodOperator, such as GKEStartPodOperator, may use
   hooks that don't extend KubernetesHook.  We use this protocol to document the
   methods used by KPO and ensure that these methods exist on such other hooks.


   .. py:property:: core_v1_client
      :type: kubernetes.client.CoreV1Api


      Get authenticated client object.



   .. py:property:: is_in_cluster
      :type: bool


      Expose whether the hook is configured with ``load_incluster_config`` or not.



   .. py:method:: get_pod(name, namespace)

      Read pod object from kubernetes API.



   .. py:method:: get_namespace()

      Return the namespace that defined in the connection.



   .. py:method:: get_xcom_sidecar_container_image()

      Return the xcom sidecar image that defined in the connection.



   .. py:method:: get_xcom_sidecar_container_resources()

      Return the xcom sidecar resources that defined in the connection.



.. py:function:: get_container_status(pod, container_name)

   Retrieve container status.


.. py:function:: container_is_running(pod, container_name)

   Examine V1Pod ``pod`` to determine whether ``container_name`` is running.

   If that container is present and running, returns True.  Returns False otherwise.


.. py:function:: container_is_completed(pod, container_name)

   Examine V1Pod ``pod`` to determine whether ``container_name`` is completed.

   If that container is present and completed, returns True.  Returns False otherwise.


.. py:function:: container_is_succeeded(pod, container_name)

   Examine V1Pod ``pod`` to determine whether ``container_name`` is completed and succeeded.

   If that container is present and completed and succeeded, returns True.  Returns False otherwise.


.. py:function:: container_is_wait(pod, container_name)

   Examine V1Pod ``pod`` to determine whether ``container_name`` is waiting.

   If that container is present and waiting, returns True.  Returns False otherwise.


.. py:function:: container_is_terminated(pod, container_name)

   Examine V1Pod ``pod`` to determine whether ``container_name`` is terminated.

   If that container is present and terminated, returns True.  Returns False otherwise.


.. py:function:: get_container_termination_message(pod, container_name)

.. py:function:: check_exception_is_kubernetes_api_unauthorized(exc)

.. py:exception:: PodLaunchTimeoutException

   Bases: :py:obj:`airflow.exceptions.AirflowException`


   When pod does not leave the ``Pending`` phase within specified timeout.


.. py:exception:: PodNotFoundException

   Bases: :py:obj:`airflow.exceptions.AirflowException`


   Expected pod does not exist in kube-api.


.. py:class:: PodLoggingStatus

   Return the status of the pod and last log time when exiting from ``fetch_container_logs``.


   .. py:attribute:: running
      :type:  bool


   .. py:attribute:: last_log_time
      :type:  pendulum.DateTime | None


.. py:class:: PodManager(kube_client, callbacks = None)

   Bases: :py:obj:`airflow.utils.log.logging_mixin.LoggingMixin`


   Create, monitor, and otherwise interact with Kubernetes pods for use with the KubernetesPodOperator.


   .. py:method:: run_pod_async(pod, **kwargs)

      Run POD asynchronously.



   .. py:method:: delete_pod(pod)

      Delete POD.



   .. py:method:: create_pod(pod)

      Launch the pod asynchronously.



   .. py:method:: await_pod_start(pod, startup_timeout = 120, startup_check_interval = 1)

      Wait for the pod to reach phase other than ``Pending``.

      :param pod:
      :param startup_timeout: Timeout (in seconds) for startup of the pod
          (if pod is pending for too long, fails task)
      :param startup_check_interval: Interval (in seconds) between checks
      :return:



   .. py:method:: await_container_completion(pod, container_name)

      Wait for the given container in the given pod to be completed.

      :param pod: pod spec that will be monitored
      :param container_name: name of the container within the pod to monitor



   .. py:method:: await_pod_completion(pod, istio_enabled = False, container_name = 'base')

      Monitor a pod and return the final state.

      :param istio_enabled: whether istio is enabled in the namespace
      :param pod: pod spec that will be monitored
      :param container_name: name of the container within the pod
      :return: tuple[State, str | None]



   .. py:method:: parse_log_line(line)

      Parse K8s log line and returns the final state.

      :param line: k8s log line
      :return: timestamp and log message



   .. py:method:: container_is_running(pod, container_name)

      Read pod and checks if container is running.



   .. py:method:: container_is_terminated(pod, container_name)

      Read pod and checks if container is terminated.



   .. py:method:: read_pod_logs(pod, container_name, tail_lines = None, timestamps = False, since_seconds = None, follow=True, post_termination_timeout = 120, **kwargs)

      Read log from the POD.



   .. py:method:: read_pod_events(pod)

      Read events from the POD.



   .. py:method:: read_pod(pod)

      Read POD information.



   .. py:method:: await_xcom_sidecar_container_start(pod, timeout = 900, log_interval = 30)

      Check if the sidecar container has reached the 'Running' state before performing do_xcom_push.



   .. py:method:: extract_xcom(pod)

      Retrieve XCom value and kill xcom sidecar container.



   .. py:method:: extract_xcom_json(pod)

      Retrieve XCom value and also check if xcom json is valid.



   .. py:method:: extract_xcom_kill(pod)

      Kill xcom sidecar container.



.. py:class:: OnFinishAction

   Bases: :py:obj:`str`, :py:obj:`enum.Enum`


   Action to take when the pod finishes.


   .. py:attribute:: KEEP_POD
      :value: 'keep_pod'



   .. py:attribute:: DELETE_POD
      :value: 'delete_pod'



   .. py:attribute:: DELETE_SUCCEEDED_POD
      :value: 'delete_succeeded_pod'



.. py:function:: is_log_group_marker(line)

   Check if the line is a log group marker like ``::group::`` or ``::endgroup::``.
