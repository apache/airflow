:mod:`airflow.providers.amazon.aws.sensors.emr_job_flow`
========================================================

.. py:module:: airflow.providers.amazon.aws.sensors.emr_job_flow


Module Contents
---------------

.. py:class:: EmrJobFlowSensor(*, job_flow_id: str, target_states: Optional[Iterable[str]] = None, failed_states: Optional[Iterable[str]] = None, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.sensors.emr_base.EmrBaseSensor`

   Asks for the state of the EMR JobFlow (Cluster) until it reaches
   any of the target states.
   If it fails the sensor errors, failing the task.

   With the default target states, sensor waits cluster to be terminated.
   When target_states is set to ['RUNNING', 'WAITING'] sensor waits
   until job flow to be ready (after 'STARTING' and 'BOOTSTRAPPING' states)

   :param job_flow_id: job_flow_id to check the state of
   :type job_flow_id: str
   :param target_states: the target states, sensor waits until
       job flow reaches any of these states
   :type target_states: list[str]
   :param failed_states: the failure states, sensor fails when
       job flow reaches any of these states
   :type failed_states: list[str]

   .. attribute:: template_fields
      :annotation: = ['job_flow_id', 'target_states', 'failed_states']

      

   .. attribute:: template_ext
      :annotation: = []

      

   
   .. method:: get_emr_response(self)

      Make an API call with boto3 and get cluster-level details.

      .. seealso::
          https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_cluster

      :return: response
      :rtype: dict[str, Any]



   
   .. staticmethod:: state_from_response(response: Dict[str, Any])

      Get state from response dictionary.

      :param response: response from AWS API
      :type response: dict[str, Any]
      :return: current state of the cluster
      :rtype: str



   
   .. staticmethod:: failure_message_from_response(response: Dict[str, Any])

      Get failure message from response dictionary.

      :param response: response from AWS API
      :type response: dict[str, Any]
      :return: failure message
      :rtype: Optional[str]




