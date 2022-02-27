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


AWS Lambda Operators
==================================================

`AWS Lambda <https://aws.amazon.com/lambda/>`__   is a
serverless, event-driven compute service that lets you
run code for virtually any type of application
or backend service without provisioning or managing servers.
You can trigger Lambda from over 200 AWS services and software as a service (SaaS) applications,
and only pay for what you use.

Airflow provides an operator to invoke an AWS Lambda function.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst


.. _howto/operator:AwsLambdaInvokeFunctionOperator:

Invoke an existing AWS Lambda function with a payload
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To publish a message to an Amazon SNS Topic you can use
:class:`~airflow.providers.amazon.aws.operators.aws_lambda.AwsLambdaInvokeFunctionOperator`.


.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_lambda.py
    :language: python
    :dedent: 4
    :start-after: [START howto_lambda_operator]
    :end-before: [END howto_lambda_operator]


Reference
^^^^^^^^^

For further information, look at:

* `Boto3 Library Documentation for Lambda <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html>`__
