:mod:`airflow.providers.amazon.aws.sensors.sagemaker_endpoint`
==============================================================

.. py:module:: airflow.providers.amazon.aws.sensors.sagemaker_endpoint


Module Contents
---------------

.. py:class:: SageMakerEndpointSensor(*, endpoint_name, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.sensors.sagemaker_base.SageMakerBaseSensor`

   Asks for the state of the endpoint state until it reaches a terminal state.
   If it fails the sensor errors, the task fails.

   :param job_name: job_name of the endpoint instance to check the state of
   :type job_name: str

   .. attribute:: template_fields
      :annotation: = ['endpoint_name']

      

   .. attribute:: template_ext
      :annotation: = []

      

   
   .. method:: non_terminal_states(self)



   
   .. method:: failed_states(self)



   
   .. method:: get_sagemaker_response(self)



   
   .. method:: get_failed_reason_from_response(self, response)



   
   .. method:: state_from_response(self, response)




