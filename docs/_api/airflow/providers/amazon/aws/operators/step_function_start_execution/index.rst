:mod:`airflow.providers.amazon.aws.operators.step_function_start_execution`
===========================================================================

.. py:module:: airflow.providers.amazon.aws.operators.step_function_start_execution


Module Contents
---------------

.. py:class:: StepFunctionStartExecutionOperator(*, state_machine_arn: str, name: Optional[str] = None, state_machine_input: Union[dict, str, None] = None, aws_conn_id: str = 'aws_default', region_name: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   An Operator that begins execution of an Step Function State Machine

   Additional arguments may be specified and are passed down to the underlying BaseOperator.

   .. seealso::
       :class:`~airflow.models.BaseOperator`

   :param state_machine_arn: ARN of the Step Function State Machine
   :type state_machine_arn: str
   :param name: The name of the execution.
   :type name: Optional[str]
   :param state_machine_input: JSON data input to pass to the State Machine
   :type state_machine_input: Union[Dict[str, any], str, None]
   :param aws_conn_id: aws connection to uses
   :type aws_conn_id: str
   :param do_xcom_push: if True, execution_arn is pushed to XCom with key execution_arn.
   :type do_xcom_push: bool

   .. attribute:: template_fields
      :annotation: = ['state_machine_arn', 'name', 'input']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #f9c915

      

   
   .. method:: execute(self, context)




