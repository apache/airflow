:mod:`airflow.providers.amazon.aws.operators.step_function_get_execution_output`
================================================================================

.. py:module:: airflow.providers.amazon.aws.operators.step_function_get_execution_output


Module Contents
---------------

.. py:class:: StepFunctionGetExecutionOutputOperator(*, execution_arn: str, aws_conn_id: str = 'aws_default', region_name: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   An Operator that begins execution of an Step Function State Machine

   Additional arguments may be specified and are passed down to the underlying BaseOperator.

   .. seealso::
       :class:`~airflow.models.BaseOperator`

   :param execution_arn: ARN of the Step Function State Machine Execution
   :type execution_arn: str
   :param aws_conn_id: aws connection to use, defaults to 'aws_default'
   :type aws_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['execution_arn']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #f9c915

      

   
   .. method:: execute(self, context)




