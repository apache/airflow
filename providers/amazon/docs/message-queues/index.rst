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


The Amazon SQS Queue Provider is a :class:`~airflow.providers.common.messaging.providers.base_provider.BaseMessageQueueProvider` that uses
Amazon Simple Queue Service (SQS) as the underlying message queue system.
It allows you to send and receive messages using SQS queues in your Airflow workflows with :class:`~airflow.providers.common.messaging.triggers.msg_queue.MessageQueueTrigger` common message queue interface.


.. include:: /../src/airflow/providers/amazon/aws/queues/sqs.py
    :start-after: [START sqs_message_queue_provider_description]
    :end-before: [END sqs_message_queue_provider_description]
