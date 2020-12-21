:mod:`airflow.providers.amazon.aws.operators.sagemaker_endpoint`
================================================================

.. py:module:: airflow.providers.amazon.aws.operators.sagemaker_endpoint


Module Contents
---------------

.. py:class:: SageMakerEndpointOperator(*, config: dict, wait_for_completion: bool = True, check_interval: int = 30, max_ingestion_time: Optional[int] = None, operation: str = 'create', **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.operators.sagemaker_base.SageMakerBaseOperator`

   Create a SageMaker endpoint.

   This operator returns The ARN of the endpoint created in Amazon SageMaker

   :param config:
       The configuration necessary to create an endpoint.

       If you need to create a SageMaker endpoint based on an existed
       SageMaker model and an existed SageMaker endpoint config::

           config = endpoint_configuration;

       If you need to create all of SageMaker model, SageMaker endpoint-config and SageMaker endpoint::

           config = {
               'Model': model_configuration,
               'EndpointConfig': endpoint_config_configuration,
               'Endpoint': endpoint_configuration
           }

       For details of the configuration parameter of model_configuration see
       :py:meth:`SageMaker.Client.create_model`

       For details of the configuration parameter of endpoint_config_configuration see
       :py:meth:`SageMaker.Client.create_endpoint_config`

       For details of the configuration parameter of endpoint_configuration see
       :py:meth:`SageMaker.Client.create_endpoint`

   :type config: dict
   :param aws_conn_id: The AWS connection ID to use.
   :type aws_conn_id: str
   :param wait_for_completion: Whether the operator should wait until the endpoint creation finishes.
   :type wait_for_completion: bool
   :param check_interval: If wait is set to True, this is the time interval, in seconds, that this operation
       waits before polling the status of the endpoint creation.
   :type check_interval: int
   :param max_ingestion_time: If wait is set to True, this operation fails if the endpoint creation doesn't
       finish within max_ingestion_time seconds. If you set this parameter to None it never times out.
   :type max_ingestion_time: int
   :param operation: Whether to create an endpoint or update an endpoint. Must be either 'create or 'update'.
   :type operation: str

   
   .. method:: create_integer_fields(self)

      Set fields which should be casted to integers.



   
   .. method:: expand_role(self)



   
   .. method:: execute(self, context)




