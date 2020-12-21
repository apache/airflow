:mod:`airflow.providers.amazon.aws.sensors.sagemaker_transform`
===============================================================

.. py:module:: airflow.providers.amazon.aws.sensors.sagemaker_transform


Module Contents
---------------

.. py:class:: SageMakerTransformSensor(*, job_name: str, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.sensors.sagemaker_base.SageMakerBaseSensor`

   Asks for the state of the transform state until it reaches a terminal state.
   The sensor will error if the job errors, throwing a AirflowException
   containing the failure reason.

   :param job_name: job_name of the transform job instance to check the state of
   :type job_name: str

   .. attribute:: template_fields
      :annotation: = ['job_name']

      

   .. attribute:: template_ext
      :annotation: = []

      

   
   .. method:: non_terminal_states(self)



   
   .. method:: failed_states(self)



   
   .. method:: get_sagemaker_response(self)



   
   .. method:: get_failed_reason_from_response(self, response)



   
   .. method:: state_from_response(self, response)




