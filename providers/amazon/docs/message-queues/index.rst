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


* It uses ``sqs`` as scheme for identifying SQS queues.
* For parameter definitions take a look at :class:`~airflow.providers.amazon.aws.triggers.sqs.SqsSensorTrigger`.

.. code-block:: python

    from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
    from airflow.sdk import Asset, AssetWatcher

    trigger = MessageQueueTrigger(
        scheme="sqs",
        # Additional AWS SqsSensorTrigger parameters as needed
        sqs_queue="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
        aws_conn_id="aws_default",
    )

    asset = Asset("sqs_queue_asset", watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)])

For a complete example, see:
:mod:`tests.system.amazon.aws.example_dag_sqs_message_queue_trigger`


Amazon Kinesis Data Streams Provider
------------------------------------

Implemented by :class:`~airflow.providers.amazon.aws.queues.kinesis.KinesisMessageQueueProvider`

The Amazon Kinesis Data Streams Provider is a :class:`~airflow.providers.common.messaging.providers.base_provider.BaseMessageQueueProvider` that uses
Amazon Kinesis Data Streams as the underlying messaging system.
It enables event-driven scheduling with :class:`~airflow.providers.common.messaging.triggers.msg_queue.MessageQueueTrigger` using ``scheme="kinesis"``.
For parameter definitions take a look at :class:`~airflow.providers.amazon.aws.triggers.kinesis.KinesisTrigger`.

.. code-block:: python

    from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
    from airflow.sdk import Asset, AssetWatcher

    trigger = MessageQueueTrigger(
        scheme="kinesis",
        stream_name="my-kinesis-stream",
        aws_conn_id="aws_default",
    )

    watcher = AssetWatcher(name="kinesis_watcher", trigger=trigger)
    asset = Asset("kinesis_stream_asset", watchers=[watcher])

Delivery semantics and considerations:

* **Record payload**: Record data in the trigger event payload (``message_batch``) is base64-encoded and must be decoded by consuming tasks.
* **Shard iterator type**: When no checkpoint exists, ``LATEST`` only sees records that arrive after the watcher starts polling. If the watcher is down or new shards are discovered, earlier records may be skipped. Use ``TRIM_HORIZON`` to process from the oldest available record.
* **Checkpointing**: Checkpointing shard progress is supported when a single asset is watched in an Airflow runtime providing an asset state store.
* **Best-effort delivery**: Delivery is best-effort. In the event of triggerer restarts or transient failures, records may be re-delivered or missed around failure windows. It does not provide exactly-once guarantees.

.. _howto/triggers:KinesisMessageQueueTrigger:

Amazon Kinesis Data Streams Message Queue Trigger
-------------------------------------------------

Implemented by :class:`~airflow.providers.amazon.aws.triggers.kinesis.KinesisTrigger`

Dispatched by :class:`~airflow.providers.common.messaging.triggers.msg_queue.MessageQueueTrigger` for ``scheme="kinesis"``

Wait for records in a stream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Below is an example of how you can configure an Airflow Dag to be triggered by records published to an
Amazon Kinesis data stream.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_kinesis_message_queue.py
    :language: python
    :start-after: [START howto_trigger_kinesis_message_queue]
    :end-before: [END howto_trigger_kinesis_message_queue]

For how to use the trigger, refer to the documentation of the
:ref:`Messaging Trigger <howto/trigger:MessageQueueTrigger>`
