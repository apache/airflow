:mod:`airflow.providers.amazon.aws.sensors.sagemaker_base`
==========================================================

.. py:module:: airflow.providers.amazon.aws.sensors.sagemaker_base


Module Contents
---------------

.. py:class:: SageMakerBaseSensor(*, aws_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Contains general sensor behavior for SageMaker.
   Subclasses should implement get_sagemaker_response()
   and state_from_response() methods.
   Subclasses should also implement NON_TERMINAL_STATES and FAILED_STATE methods.

   .. attribute:: ui_color
      :annotation: = #ededed

      

   
   .. method:: get_hook(self)

      Get SageMakerHook



   
   .. method:: poke(self, context)



   
   .. method:: non_terminal_states(self)

      Placeholder for returning states with should not terminate.



   
   .. method:: failed_states(self)

      Placeholder for returning states with are considered failed.



   
   .. method:: get_sagemaker_response(self)

      Placeholder for checking status of a SageMaker task.



   
   .. method:: get_failed_reason_from_response(self, response: dict)

      Placeholder for extracting the reason for failure from an AWS response.



   
   .. method:: state_from_response(self, response: dict)

      Placeholder for extracting the state from an AWS response.




