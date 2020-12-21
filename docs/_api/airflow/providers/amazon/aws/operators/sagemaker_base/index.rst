:mod:`airflow.providers.amazon.aws.operators.sagemaker_base`
============================================================

.. py:module:: airflow.providers.amazon.aws.operators.sagemaker_base


Module Contents
---------------

.. py:class:: SageMakerBaseOperator(*, config: dict, aws_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This is the base operator for all SageMaker operators.

   :param config: The configuration necessary to start a training job (templated)
   :type config: dict
   :param aws_conn_id: The AWS connection ID to use.
   :type aws_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['config']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #ededed

      

   .. attribute:: integer_fields
      :annotation: :Iterable[Iterable[str]] = []

      

   
   .. method:: parse_integer(self, config, field)

      Recursive method for parsing string fields holding integer values to integers.



   
   .. method:: parse_config_integers(self)

      Parse the integer fields of training config to integers in case the config is rendered by Jinja and
      all fields are str.



   
   .. method:: expand_role(self)

      Placeholder for calling boto3's expand_role(), which expands an IAM role name into an ARN.



   
   .. method:: preprocess_config(self)

      Process the config into a usable form.



   
   .. method:: execute(self, context)



   
   .. method:: hook(self)

      Return SageMakerHook




