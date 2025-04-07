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

Event-driven scheduling
=======================

.. versionadded:: 3.0

Apache Airflow allows for event-driven scheduling, enabling dags to be triggered based on external events rather than
predefined time-based schedules.
This is particularly useful in modern data architectures where workflows need to react to real-time data changes,
messages, or system signals.

By using assets, as described in :doc:`datasets`, you can configure dags to start execution when specific external events
occur. Assets provide a mechanism to establish dependencies between external events and DAG execution, ensuring that
workflows react dynamically to changes in the external environment.

The ``AssetWatcher`` class plays a crucial role in this mechanism. It monitors an external event source, such as a
message queue, and triggers an asset update when a relevant event occurs.
The ``watchers`` parameter in the ``Asset`` definition allows you to associate multiple ``AssetWatcher`` instances with an
asset, enabling it to respond to various event sources.

Example: Triggering a DAG from an external message queue
--------------------------------------------------------

Below is an example of how you can configure an Airflow DAG to be triggered by an external message queue, such as AWS
SQS:

.. code-block:: python

    from airflow.sdk import Asset, AssetWatcher
    from airflow.providers.common.msgq.triggers.msg_queue import MessageQueueTrigger
    from airflow.sdk import DAG
    from datetime import datetime

    # Define a trigger that listens to an external message queue (AWS SQS in this case)
    trigger = MessageQueueTrigger(queue="https://sqs.us-east-1.amazonaws.com/0123456789/my-queue")

    # Define an asset that watches for messages on the queue
    asset = Asset("sqs_queue_asset", watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)])

    # Define the DAG that will be triggered when the asset is updated
    with DAG(dag_id="event_driven_dag", schedule=[asset], catchup=False) as dag:
        ...

How it works
------------
1. **Message Queue Trigger**: The ``MessageQueueTrigger`` listens for messages from an external queue
(e.g., AWS SQS, Kafka, or another messaging system).

2. **Asset and Watcher**: The ``Asset`` abstracts the external entity, the SQS queue in this example.
The ``AssetWatcher`` associate a trigger with a name. This name helps you identify which trigger is associated to which
asset.

3. **Event-Driven DAG**: Instead of running on a fixed schedule, the DAG executes when the asset receives an update
(e.g., a new message in the queue).

Supported triggers for event-driven scheduling
----------------------------------------------
Not all :doc:`triggers <deferring>` in Airflow can be used for event-driven scheduling. As opposed to all triggers that
inherit from ``BaseTrigger``, only a subset that inherit from ``BaseEventTrigger`` are compatible.
The reason for this restriction is that some triggers are not designed for event-driven scheduling, and using them to
schedule dags could lead to unintended results.

``BaseEventTrigger`` ensures that triggers used for scheduling adhere to an event-driven paradigm, reacting appropriately
to external event changes without causing unexpected DAG behavior.

Writing event-driven compatible triggers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To make a trigger compatible with event-driven scheduling, it must inherit from ``BaseEventTrigger``. There are three
main scenarios for working with triggers in this context:

1. **Creating a new event-driven trigger**: If you need a new trigger for an unsupported event source, you should create
a new class inheriting from ``BaseEventTrigger`` and implement its logic.

2. **Adapting an existing compatible trigger**: If an existing trigger (inheriting from ``BaseEvent``) is proven to be
already compatible with event-driven scheduling, then you just need to change the base class from ``BaseTrigger`` to
``BaseEventTrigger``.

3. **Adapting an existing incompatible trigger**: If an existing trigger does not appear to be compatible with
event-driven scheduling, then a new trigger must be created.
This new trigger must inherit ``BaseEventTrigger`` and ensure it properly works with event-driven scheduling.
It might inherit from the existing trigger as well if both triggers share some common code.

Use cases for event-driven dags
-------------------------------

* **Data ingestion pipelines**: Trigger ETL workflows when new data arrives in a storage system.

* **Machine learning workflows**: Start training models when new datasets become available.

* **IoT and real-time analytics**: React to sensor data, logs, or application events in real-time.

* **Microservices and event-driven architectures**: Orchestrate workflows based on service-to-service messages.
