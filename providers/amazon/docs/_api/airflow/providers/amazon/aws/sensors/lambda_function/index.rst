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

:py:mod:`airflow.providers.amazon.aws.sensors.lambda_function`
==============================================================

.. py:module:: airflow.providers.amazon.aws.sensors.lambda_function


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.lambda_function.LambdaFunctionStateSensor




.. py:class:: LambdaFunctionStateSensor(*, function_name, qualifier = None, target_states = ['Active'], aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Poll the deployment state of the AWS Lambda function until it reaches a target state.

   Fails if the query fails.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:LambdaFunctionStateSensor`

   :param function_name: The name of the AWS Lambda function, version, or alias.
   :param qualifier: Specify a version or alias to get details about a published version of the function.
   :param target_states: The Lambda states desired.
   :param aws_conn_id: aws connection to use, defaults to 'aws_default'

   .. py:attribute:: FAILURE_STATES
      :value: ('Failed',)



   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('function_name', 'qualifier')



   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: hook()
