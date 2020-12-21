:mod:`airflow.providers.amazon.aws.operators.sagemaker_model`
=============================================================

.. py:module:: airflow.providers.amazon.aws.operators.sagemaker_model


Module Contents
---------------

.. py:class:: SageMakerModelOperator(*, config, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.operators.sagemaker_base.SageMakerBaseOperator`

   Create a SageMaker model.

   This operator returns The ARN of the model created in Amazon SageMaker

   :param config: The configuration necessary to create a model.

       For details of the configuration parameter see :py:meth:`SageMaker.Client.create_model`
   :type config: dict
   :param aws_conn_id: The AWS connection ID to use.
   :type aws_conn_id: str

   
   .. method:: expand_role(self)



   
   .. method:: execute(self, context)




