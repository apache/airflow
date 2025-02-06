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

:py:mod:`airflow.providers.amazon.aws.operators.step_function`
==============================================================

.. py:module:: airflow.providers.amazon.aws.operators.step_function


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.step_function.StepFunctionStartExecutionOperator
   airflow.providers.amazon.aws.operators.step_function.StepFunctionGetExecutionOutputOperator




.. py:class:: StepFunctionStartExecutionOperator(*, state_machine_arn, name = None, state_machine_input = None, aws_conn_id = 'aws_default', region_name = None, waiter_max_attempts = 30, waiter_delay = 60, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   An Operator that begins execution of an AWS Step Function State Machine.

   Additional arguments may be specified and are passed down to the underlying BaseOperator.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:StepFunctionStartExecutionOperator`

   :param state_machine_arn: ARN of the Step Function State Machine
   :param name: The name of the execution.
   :param state_machine_input: JSON data input to pass to the State Machine
   :param aws_conn_id: aws connection to uses
   :param do_xcom_push: if True, execution_arn is pushed to XCom with key execution_arn.
   :param waiter_max_attempts: Maximum number of attempts to poll the execution.
   :param waiter_delay: Number of seconds between polling the state of the execution.
   :param deferrable: If True, the operator will wait asynchronously for the job to complete.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False, but can be overridden in config file by setting default_deferrable to True)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('state_machine_arn', 'name', 'input')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#f9c915'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event = None)



.. py:class:: StepFunctionGetExecutionOutputOperator(*, execution_arn, aws_conn_id = 'aws_default', region_name = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   An Operator that returns the output of an AWS Step Function State Machine execution.

   Additional arguments may be specified and are passed down to the underlying BaseOperator.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:StepFunctionGetExecutionOutputOperator`

   :param execution_arn: ARN of the Step Function State Machine Execution
   :param aws_conn_id: aws connection to use, defaults to 'aws_default'

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('execution_arn',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#f9c915'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
