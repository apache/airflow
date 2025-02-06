 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

:py:mod:`airflow.providers.amazon.aws.hooks.step_function`
==========================================================

.. py:module:: airflow.providers.amazon.aws.hooks.step_function


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.step_function.StepFunctionHook




.. py:class:: StepFunctionHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with an AWS Step Functions State Machine.

   Provide thin wrapper around :external+boto3:py:class:`boto3.client("stepfunctions") <SFN.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:method:: start_execution(state_machine_arn, name = None, state_machine_input = None)

      Start Execution of the State Machine.

      .. seealso::
          - :external+boto3:py:meth:`SFN.Client.start_execution`

      :param state_machine_arn: AWS Step Function State Machine ARN.
      :param name: The name of the execution.
      :param state_machine_input: JSON data input to pass to the State Machine.
      :return: Execution ARN.


   .. py:method:: describe_execution(execution_arn)

      Describe a State Machine Execution.

      .. seealso::
          - :external+boto3:py:meth:`SFN.Client.describe_execution`

      :param execution_arn: ARN of the State Machine Execution.
      :return: Dict with execution details.
