:mod:`airflow.providers.amazon.aws.operators.sagemaker_tuning`
==============================================================

.. py:module:: airflow.providers.amazon.aws.operators.sagemaker_tuning


Module Contents
---------------

.. py:class:: SageMakerTuningOperator(*, config: dict, wait_for_completion: bool = True, check_interval: int = 30, max_ingestion_time: Optional[int] = None, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.operators.sagemaker_base.SageMakerBaseOperator`

   Initiate a SageMaker hyperparameter tuning job.

   This operator returns The ARN of the tuning job created in Amazon SageMaker.

   :param config: The configuration necessary to start a tuning job (templated).

       For details of the configuration parameter see
       :py:meth:`SageMaker.Client.create_hyper_parameter_tuning_job`
   :type config: dict
   :param aws_conn_id: The AWS connection ID to use.
   :type aws_conn_id: str
   :param wait_for_completion: Set to True to wait until the tuning job finishes.
   :type wait_for_completion: bool
   :param check_interval: If wait is set to True, the time interval, in seconds,
       that this operation waits to check the status of the tuning job.
   :type check_interval: int
   :param max_ingestion_time: If wait is set to True, the operation fails
       if the tuning job doesn't finish within max_ingestion_time seconds. If you
       set this parameter to None, the operation does not timeout.
   :type max_ingestion_time: int

   .. attribute:: integer_fields
      :annotation: = [['HyperParameterTuningJobConfig', 'ResourceLimits', 'MaxNumberOfTrainingJobs'], ['HyperParameterTuningJobConfig', 'ResourceLimits', 'MaxParallelTrainingJobs'], ['TrainingJobDefinition', 'ResourceConfig', 'InstanceCount'], ['TrainingJobDefinition', 'ResourceConfig', 'VolumeSizeInGB'], ['TrainingJobDefinition', 'StoppingCondition', 'MaxRuntimeInSeconds']]

      

   
   .. method:: expand_role(self)



   
   .. method:: execute(self, context)




