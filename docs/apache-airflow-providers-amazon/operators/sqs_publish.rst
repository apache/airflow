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


.. _howto/operator:SqsPublishOperator:

SQS Publish Operator
====================

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Using Operator
--------------

Use the
:class:`~airflow.providers.amazon.aws.operators.sqs.SqsPublishOperator`
to publish a message to Amazon Simple Queue Service (SQS).

In the following example, the task "publish_to_queue" publishes a message containing
the task instance and the execution date to a queue named ``Airflow-Example-Queue``.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sqs.py
    :language: python
    :start-after: [START howto_sqs_operator_and_sensor]
    :end-before: [END howto_sqs_operator_and_sensor]

More information
----------------

For further information, look at the documentation of :meth:`~SQS.Client.send_message` method
in `boto3`_.

.. _boto3: https://pypi.org/project/boto3/
