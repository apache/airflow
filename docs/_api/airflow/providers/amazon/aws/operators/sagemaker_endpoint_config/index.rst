:mod:`airflow.providers.amazon.aws.operators.sagemaker_endpoint_config`
=======================================================================

.. py:module:: airflow.providers.amazon.aws.operators.sagemaker_endpoint_config


Module Contents
---------------

.. py:class:: SageMakerEndpointConfigOperator(*, config: dict, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.operators.sagemaker_base.SageMakerBaseOperator`

   Create a SageMaker endpoint config.

   This operator returns The ARN of the endpoint config created in Amazon SageMaker

   :param config: The configuration necessary to create an endpoint config.

       For details of the configuration parameter see :py:meth:`SageMaker.Client.create_endpoint_config`
   :type config: dict
   :param aws_conn_id: The AWS connection ID to use.
   :type aws_conn_id: str

   .. attribute:: integer_fields
      :annotation: = [['ProductionVariants', 'InitialInstanceCount']]

      

   
   .. method:: execute(self, context)




