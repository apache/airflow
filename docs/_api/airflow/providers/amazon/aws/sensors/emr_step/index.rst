:mod:`airflow.providers.amazon.aws.sensors.emr_step`
====================================================

.. py:module:: airflow.providers.amazon.aws.sensors.emr_step


Module Contents
---------------

.. py:class:: EmrStepSensor(*, job_flow_id: str, step_id: str, target_states: Optional[Iterable[str]] = None, failed_states: Optional[Iterable[str]] = None, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.sensors.emr_base.EmrBaseSensor`

   Asks for the state of the step until it reaches any of the target states.
   If it fails the sensor errors, failing the task.

   With the default target states, sensor waits step to be completed.

   :param job_flow_id: job_flow_id which contains the step check the state of
   :type job_flow_id: str
   :param step_id: step to check the state of
   :type step_id: str
   :param target_states: the target states, sensor waits until
       step reaches any of these states
   :type target_states: list[str]
   :param failed_states: the failure states, sensor fails when
       step reaches any of these states
   :type failed_states: list[str]

   .. attribute:: template_fields
      :annotation: = ['job_flow_id', 'step_id', 'target_states', 'failed_states']

      

   .. attribute:: template_ext
      :annotation: = []

      

   
   .. method:: get_emr_response(self)

      Make an API call with boto3 and get details about the cluster step.

      .. seealso::
          https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_step

      :return: response
      :rtype: dict[str, Any]



   
   .. staticmethod:: state_from_response(response: Dict[str, Any])

      Get state from response dictionary.

      :param response: response from AWS API
      :type response: dict[str, Any]
      :return: execution state of the cluster step
      :rtype: str



   
   .. staticmethod:: failure_message_from_response(response: Dict[str, Any])

      Get failure message from response dictionary.

      :param response: response from AWS API
      :type response: dict[str, Any]
      :return: failure message
      :rtype: Optional[str]




