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

========================================
Amazon Simple Notification Service (SNS)
========================================

`Amazon Simple Notification Service (Amazon SNS) <https://aws.amazon.com/sns/>`__  is a managed
service that provides message delivery from publishers to subscribers (also known as producers
and consumers). Publishers communicate asynchronously with subscribers by sending messages to
a topic, which is a logical access point and communication channel. Clients can subscribe to the
SNS topic and receive published messages using a supported endpoint type, such as Amazon Kinesis
Data Firehose, Amazon SQS, AWS Lambda, HTTP, email, mobile push notifications, and mobile text
messages (SMS).

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:SnsPublishOperator:

Publish a message to an existing SNS topic
==========================================

To publish a message to an Amazon SNS Topic you can use
:class:`~airflow.providers.amazon.aws.operators.sns.SnsPublishOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_sns.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sns_publish_operator]
    :end-before: [END howto_operator_sns_publish_operator]

Reference
---------

* `AWS boto3 library documentation for SNS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html>`__
