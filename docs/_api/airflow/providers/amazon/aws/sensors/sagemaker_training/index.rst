:mod:`airflow.providers.amazon.aws.sensors.sagemaker_training`
==============================================================

.. py:module:: airflow.providers.amazon.aws.sensors.sagemaker_training


Module Contents
---------------

.. py:class:: SageMakerTrainingSensor(*, job_name, print_log=True, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.sensors.sagemaker_base.SageMakerBaseSensor`

   Asks for the state of the training state until it reaches a terminal state.
   If it fails the sensor errors, failing the task.

   :param job_name: name of the SageMaker training job to check the state of
   :type job_name: str
   :param print_log: if the operator should print the cloudwatch log
   :type print_log: bool

   .. attribute:: template_fields
      :annotation: = ['job_name']

      

   .. attribute:: template_ext
      :annotation: = []

      

   
   .. method:: init_log_resource(self, hook: SageMakerHook)

      Set tailing LogState for associated training job.



   
   .. method:: non_terminal_states(self)



   
   .. method:: failed_states(self)



   
   .. method:: get_sagemaker_response(self)



   
   .. method:: get_failed_reason_from_response(self, response)



   
   .. method:: state_from_response(self, response)




