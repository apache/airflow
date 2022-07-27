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

==========
AWS Lambda
==========

With `AWS Lambda <https://aws.amazon.com/lambda/>`__, you can run code without provisioning or managing servers.
You pay only for the compute time that you consume—there's no charge when your code isn't running.
You can run code for virtually any type of application or backend service—all with zero administration.
Just upload your code and Lambda takes care of everything required to run and scale your code with high availability.
You can set up your code to automatically trigger from other AWS services or call it directly from any web or mobile app.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:AwsLambdaInvokeFunctionOperator:

Invoke an AWS Lambda function
=============================

To invoke an AWS lambda function you can use
:class:`~airflow.providers.amazon.aws.operators.aws_lambda.AwsLambdaInvokeFunctionOperator`.


.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_lambda.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_lambda]
    :end-before: [END howto_operator_lambda]

Reference
---------

* `AWS boto3 library documentation for Lambda <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html>`__
