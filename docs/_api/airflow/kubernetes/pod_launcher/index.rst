:mod:`airflow.kubernetes.pod_launcher`
======================================

.. py:module:: airflow.kubernetes.pod_launcher

.. autoapi-nested-parse::

   Launches PODs



Module Contents
---------------

.. py:class:: PodStatus

   Status of the PODs

   .. attribute:: PENDING
      :annotation: = pending

      

   .. attribute:: RUNNING
      :annotation: = running

      

   .. attribute:: FAILED
      :annotation: = failed

      

   .. attribute:: SUCCEEDED
      :annotation: = succeeded

      


.. py:class:: PodLauncher(kube_client: client.CoreV1Api = None, in_cluster: bool = True, cluster_context: Optional[str] = None, extract_xcom: bool = False)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Launches PODS

   
   .. method:: run_pod_async(self, pod: V1Pod, **kwargs)

      Runs POD asynchronously



   
   .. method:: delete_pod(self, pod: V1Pod)

      Deletes POD



   
   .. method:: start_pod(self, pod: V1Pod, startup_timeout: int = 120)

      Launches the pod synchronously and waits for completion.

      :param pod:
      :param startup_timeout: Timeout for startup of the pod (if pod is pending for too long, fails task)
      :return:



   
   .. method:: monitor_pod(self, pod: V1Pod, get_logs: bool)

      Monitors a pod and returns the final state

      :param pod: pod spec that will be monitored
      :type pod : V1Pod
      :param get_logs: whether to read the logs locally
      :return:  Tuple[State, Optional[str]]



   
   .. method:: parse_log_line(self, line: str)

      Parse K8s log line and returns the final state

      :param line: k8s log line
      :type line: str
      :return: timestamp and log message
      :rtype: Tuple[str, str]



   
   .. method:: _task_status(self, event)



   
   .. method:: pod_not_started(self, pod: V1Pod)

      Tests if pod has not started



   
   .. method:: pod_is_running(self, pod: V1Pod)

      Tests if pod is running



   
   .. method:: base_container_is_running(self, pod: V1Pod)

      Tests if base container is running



   
   .. method:: read_pod_logs(self, pod: V1Pod, tail_lines: Optional[int] = None, timestamps: bool = False, since_seconds: Optional[int] = None)

      Reads log from the POD



   
   .. method:: read_pod_events(self, pod)

      Reads events from the POD



   
   .. method:: read_pod(self, pod: V1Pod)

      Read POD information



   
   .. method:: _extract_xcom(self, pod: V1Pod)



   
   .. method:: _exec_pod_command(self, resp, command)



   
   .. method:: process_status(self, job_id, status)

      Process status information for the JOB




