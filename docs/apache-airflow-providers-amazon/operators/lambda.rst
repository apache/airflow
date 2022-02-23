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


.. _howto/operator:AwsLambdaInvokeFunctionOperator:

AWS Lambda Invoke Function Operator
===================================

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Using Operator
--------------
Use the
:class:`~airflow.providers.amazon.aws.operators.aws_lambda.AwsLambdaInvokeFunctionOperator`
to invoke an AWS Lambda function.  To get started with AWS Lambda please visit
`aws.amazon.com/lambda <https://aws.amazon.com/lambda/>`_


In the following example, we invoke an AWS Lambda function.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_lambda.py
    :language: python
    :start-after: [START howto_lambda_operator]
    :end-before: [END howto_lambda_operator]

More information
----------------

For further information, look at the documentation of :meth:`~Lambda.Client.invoke` method
in `boto3`_.

.. _boto3: https://pypi.org/project/boto3/
