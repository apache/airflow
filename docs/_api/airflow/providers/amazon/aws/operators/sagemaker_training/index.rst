:mod:`airflow.providers.amazon.aws.operators.sagemaker_training`
================================================================

.. py:module:: airflow.providers.amazon.aws.operators.sagemaker_training


Module Contents
---------------

.. py:class:: SageMakerTrainingOperator(*, config: dict, wait_for_completion: bool = True, print_log: bool = True, check_interval: int = 30, max_ingestion_time: Optional[int] = None, action_if_job_exists: str = 'increment', **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.operators.sagemaker_base.SageMakerBaseOperator`

   Initiate a SageMaker training job.

   This operator returns The ARN of the training job created in Amazon SageMaker.

   :param config: The configuration necessary to start a training job (templated).

       For details of the configuration parameter see :py:meth:`SageMaker.Client.create_training_job`
   :type config: dict
   :param aws_conn_id: The AWS connection ID to use.
   :type aws_conn_id: str
   :param wait_for_completion: If wait is set to True, the time interval, in seconds,
       that the operation waits to check the status of the training job.
   :type wait_for_completion: bool
   :param print_log: if the operator should print the cloudwatch log during training
   :type print_log: bool
   :param check_interval: if wait is set to be true, this is the time interval
       in seconds which the operator will check the status of the training job
   :type check_interval: int
   :param max_ingestion_time: If wait is set to True, the operation fails if the training job
       doesn't finish within max_ingestion_time seconds. If you set this parameter to None,
       the operation does not timeout.
   :type max_ingestion_time: int
   :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "increment"
       (default) and "fail".
   :type action_if_job_exists: str

   .. attribute:: integer_fields
      :annotation: = [['ResourceConfig', 'InstanceCount'], ['ResourceConfig', 'VolumeSizeInGB'], ['StoppingCondition', 'MaxRuntimeInSeconds']]

      

   
   .. method:: expand_role(self)



   
   .. method:: execute(self, context)




