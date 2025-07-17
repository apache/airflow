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

Amazon Messaging Queues
=======================

Amazon SQS Queue Provider
-------------------------

Implemented by :class:`~airflow.providers.amazon.aws.queues.sqs.SqsMessageQueueProvider`

The Amazon SQS Queue Provider is a message queue provider that uses
Amazon Simple Queue Service (SQS) as the underlying message queue system.
It allows you to send and receive messages using SQS queues in your Airflow workflows.
The provider supports both standard and FIFO queues, and it provides features
such as message visibility timeout, message retention period, and dead-letter queues.

The queue must be matching this regex:

.. exampleinclude:: /../src/airflow/providers/amazon/aws/queues/sqs.py
    :language: python
    :dedent: 0
    :start-after: [START queue_regexp]
    :end-before: [END queue_regexp]


The queue parameter is passed directly to ``sqs_queue`` parameter of the underlying
:class:`~airflow.providers.amazon.aws.triggers.sqs.SqsSensorTrigger` class, and passes
all the kwargs directly to the trigger constructor if added.
