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

Amazon SQS Operators
====================

`Amazon Simple Queue Service (SQS) <https://aws.amazon.com/sqs/>`__  is a fully managed message queuing
service that enables you to decouple and scale microservices, distributed systems, and serverless
applications. SQS eliminates the complexity and overhead associated with managing and operating
message-oriented middleware, and empowers developers to focus on differentiating work. Using SQS, you
can send, store, and receive messages between software components at any volume, without losing messages
or requiring other services to be available.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

.. _howto/operator:SqsPublishOperator:

Amazon SQS Publish Operator
"""""""""""""""""""""""""""

To publish a message to an Amazon SQS queue use the
:class:`~airflow.providers.amazon.aws.operators.sqs.SqsPublishOperator`

In the following example, the task "publish_to_queue" publishes a message containing
the task instance and the execution date to a queue with a default name of ``Airflow-Example-Queue``.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sqs.py
    :language: python
    :start-after: [START howto_sqs_operator]
    :end-before: [END howto_sqs_operator]

.. _howto/operator:SqsSensor:

Amazon SQS Sensor
"""""""""""""""""

To read messages from an Amazon SQS queue until exhausted use the
:class:`~airflow.providers.amazon.aws.operators.sqs.SqsPublishOperator`

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sqs.py
    :language: python
    :start-after: [START howto_sqs_sensor]
    :end-before: [END howto_sqs_sensor]

References
----------

For further information, look at:

* `Boto3 Library Documentation for SQS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html>`__
