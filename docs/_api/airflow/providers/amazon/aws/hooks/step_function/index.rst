:mod:`airflow.providers.amazon.aws.hooks.step_function`
=======================================================

.. py:module:: airflow.providers.amazon.aws.hooks.step_function


Module Contents
---------------

.. py:class:: StepFunctionHook(region_name: Optional[str] = None, *args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with an AWS Step Functions State Machine.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   
   .. method:: start_execution(self, state_machine_arn: str, name: Optional[str] = None, state_machine_input: Union[dict, str, None] = None)

      Start Execution of the State Machine.
      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.start_execution

      :param state_machine_arn: AWS Step Function State Machine ARN
      :type state_machine_arn: str
      :param name: The name of the execution.
      :type name: Optional[str]
      :param state_machine_input: JSON data input to pass to the State Machine
      :type state_machine_input: Union[Dict[str, any], str, None]
      :return: Execution ARN
      :rtype: str



   
   .. method:: describe_execution(self, execution_arn: str)

      Describes a State Machine Execution
      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.describe_execution

      :param execution_arn: ARN of the State Machine Execution
      :type execution_arn: str
      :return: Dict with Execution details
      :rtype: dict




