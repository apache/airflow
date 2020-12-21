:mod:`airflow.providers.amazon.aws.operators.sagemaker_transform`
=================================================================

.. py:module:: airflow.providers.amazon.aws.operators.sagemaker_transform


Module Contents
---------------

.. py:class:: SageMakerTransformOperator(*, config: dict, wait_for_completion: bool = True, check_interval: int = 30, max_ingestion_time: Optional[int] = None, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.operators.sagemaker_base.SageMakerBaseOperator`

   Initiate a SageMaker transform job.

   This operator returns The ARN of the model created in Amazon SageMaker.

   :param config: The configuration necessary to start a transform job (templated).

       If you need to create a SageMaker transform job based on an existed SageMaker model::

           config = transform_config

       If you need to create both SageMaker model and SageMaker Transform job::

           config = {
               'Model': model_config,
               'Transform': transform_config
           }

       For details of the configuration parameter of transform_config see
       :py:meth:`SageMaker.Client.create_transform_job`

       For details of the configuration parameter of model_config, See:
       :py:meth:`SageMaker.Client.create_model`

   :type config: dict
   :param aws_conn_id: The AWS connection ID to use.
   :type aws_conn_id: str
   :param wait_for_completion: Set to True to wait until the transform job finishes.
   :type wait_for_completion: bool
   :param check_interval: If wait is set to True, the time interval, in seconds,
       that this operation waits to check the status of the transform job.
   :type check_interval: int
   :param max_ingestion_time: If wait is set to True, the operation fails
       if the transform job doesn't finish within max_ingestion_time seconds. If you
       set this parameter to None, the operation does not timeout.
   :type max_ingestion_time: int

   
   .. method:: create_integer_fields(self)

      Set fields which should be casted to integers.



   
   .. method:: expand_role(self)



   
   .. method:: execute(self, context)




