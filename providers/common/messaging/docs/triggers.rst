
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

Messaging Triggers
==================

.. _howto/trigger:MessageQueueTrigger:

Wait for a message in a queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.common.messaging.triggers.msg_queue.MessageQueueTrigger` to wait for a message in a
queue. Mandatory parameters of the trigger are:

- ``scheme`` - the queue scheme (e.g., 'kafka', 'redis+pubsub', 'sqs') you are using

Additional parameters can be provided depending on the queue provider. Connections needs to be provided with the relevant
default connection ID, for example, when connecting to a queue in AWS SQS, the connection ID should be
``aws_default``.

Below is an example of how you can configure an Airflow Dag to be triggered by a message in Amazon SQS.

.. exampleinclude:: /../tests/system/common/messaging/example_message_queue_trigger.py
    :language: python
    :start-after: [START howto_trigger_message_queue]
    :end-before: [END howto_trigger_message_queue]

How it works
------------
1. **Message Queue Trigger**: The ``MessageQueueTrigger`` listens for messages from an external queue
(e.g., AWS SQS, Kafka, or another messaging system).

2. **Asset and Watcher**: The ``Asset`` abstracts the external entity, the SQS queue in this example.
The ``AssetWatcher`` associate a trigger with a name. This name helps you identify which trigger is associated to which
asset.

3. **Event-Driven Dag**: Instead of running on a fixed schedule, the Dag executes when the asset receives an update
(e.g., a new message in the queue).

Use message payload in the Dag
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a message is received from the queue, the trigger passes the message payload as part of the trigger event.
You can access this payload in your DAG tasks using the ``triggering_asset_events`` parameter.

.. code-block:: python

    from airflow.decorators import task
    from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
    from airflow.sdk import DAG, Asset, AssetWatcher, chain

    # Define the asset with trigger
    trigger = MessageQueueTrigger(scheme="kafka", topics=["my-kafka-topic"])
    asset = Asset("kafka_queue_asset", watchers=[AssetWatcher(name="kafka_watcher", trigger=trigger)])

    with DAG(dag_id="example_msgq_payload", schedule=[asset]) as dag:

        @task
        def process_message(triggering_asset_events):
            for event in triggering_asset_events[asset]:
                # Access the message payload
                payload = event.extra["payload"]
                # Process the payload as needed
                print(f"Received message: {payload}")

        chain(process_message())

The ``triggering_asset_events`` parameter contains the events that triggered the DAG run, indexed by asset.
Each event includes an ``extra`` dictionary where the message payload is stored under the ``payload`` key.
