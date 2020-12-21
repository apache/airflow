:mod:`airflow.providers.amazon.aws.sensors.emr_base`
====================================================

.. py:module:: airflow.providers.amazon.aws.sensors.emr_base


Module Contents
---------------

.. py:class:: EmrBaseSensor(*, aws_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Contains general sensor behavior for EMR.

   Subclasses should implement following methods:
       - ``get_emr_response()``
       - ``state_from_response()``
       - ``failure_message_from_response()``

   Subclasses should set ``target_states`` and ``failed_states`` fields.

   :param aws_conn_id: aws connection to uses
   :type aws_conn_id: str

   .. attribute:: ui_color
      :annotation: = #66c3ff

      

   
   .. method:: get_hook(self)

      Get EmrHook



   
   .. method:: poke(self, context)



   
   .. method:: get_emr_response(self)

      Make an API call with boto3 and get response.

      :return: response
      :rtype: dict[str, Any]



   
   .. staticmethod:: state_from_response(response: Dict[str, Any])

      Get state from response dictionary.

      :param response: response from AWS API
      :type response: dict[str, Any]
      :return: state
      :rtype: str



   
   .. staticmethod:: failure_message_from_response(response: Dict[str, Any])

      Get failure message from response dictionary.

      :param response: response from AWS API
      :type response: dict[str, Any]
      :return: failure message
      :rtype: Optional[str]




