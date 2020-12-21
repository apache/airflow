:mod:`airflow.providers.amazon.aws.operators.cloud_formation`
=============================================================

.. py:module:: airflow.providers.amazon.aws.operators.cloud_formation

.. autoapi-nested-parse::

   This module contains CloudFormation create/delete stack operators.



Module Contents
---------------

.. py:class:: CloudFormationCreateStackOperator(*, stack_name: str, params: dict, aws_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   An operator that creates a CloudFormation stack.

   :param stack_name: stack name (templated)
   :type stack_name: str
   :param params: parameters to be passed to CloudFormation.

       .. seealso::
           https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudformation.html#CloudFormation.Client.create_stack
   :type params: dict
   :param aws_conn_id: aws connection to uses
   :type aws_conn_id: str

   .. attribute:: template_fields
      :annotation: :List[str] = ['stack_name']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #6b9659

      

   
   .. method:: execute(self, context)




.. py:class:: CloudFormationDeleteStackOperator(*, stack_name: str, params: Optional[dict] = None, aws_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   An operator that deletes a CloudFormation stack.

   :param stack_name: stack name (templated)
   :type stack_name: str
   :param params: parameters to be passed to CloudFormation.

       .. seealso::
           https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudformation.html#CloudFormation.Client.delete_stack
   :type params: dict
   :param aws_conn_id: aws connection to uses
   :type aws_conn_id: str

   .. attribute:: template_fields
      :annotation: :List[str] = ['stack_name']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #1d472b

      

   .. attribute:: ui_fgcolor
      :annotation: = #FFF

      

   
   .. method:: execute(self, context)




