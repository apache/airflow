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

:py:mod:`airflow.providers.amazon.aws.triggers.step_function`
=============================================================

.. py:module:: airflow.providers.amazon.aws.triggers.step_function


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.step_function.StepFunctionsExecutionCompleteTrigger




.. py:class:: StepFunctionsExecutionCompleteTrigger(*, execution_arn, waiter_delay = 60, waiter_max_attempts = 30, aws_conn_id = None, region_name = None)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger to poll for the completion of a Step Functions execution.

   :param execution_arn: ARN of the state machine to poll
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.

   .. py:method:: hook()

      Override in subclasses to return the right hook.
