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

Apache Airflow allows for event-driven scheduling, enabling Dags to be triggered based on external events rather than
predefined time-based schedules.
This is particularly useful in modern data architectures where workflows need to react to real-time data changes,
messages, or system signals.

By using assets, as described in :doc:`asset-scheduling`, you can configure Dags to start execution when specific external events
occur. Assets provide a mechanism to establish dependencies between external events and Dag execution, ensuring that
workflows react dynamically to changes in the external environment.

The ``AssetWatcher`` class plays a crucial role in this mechanism. It monitors an external event source, such as a
message queue, and triggers an asset update when a relevant event occurs.
The ``watchers`` parameter in the ``Asset`` definition allows you to associate multiple ``AssetWatcher`` instances with an
asset, enabling it to respond to various event sources.

See the :doc:`common.messaging provider docs <apache-airflow-providers-common-messaging:triggers>` for more information and examples.

Supported triggers for event-driven scheduling
----------------------------------------------
Not all :doc:`triggers <deferring>` in Airflow can be used for event-driven scheduling. As opposed to all triggers that
inherit from ``BaseTrigger``, only a subset that inherit from ``BaseEventTrigger`` are compatible.
The reason for this restriction is that some triggers are not designed for event-driven scheduling, and using them to
schedule Dags could lead to unintended results.

``BaseEventTrigger`` ensures that triggers used for scheduling adhere to an event-driven paradigm, reacting appropriately
to external event changes without causing unexpected Dag behavior.

Writing event-driven compatible triggers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To make a trigger compatible with event-driven scheduling, it must inherit from ``BaseEventTrigger``. There are three
main scenarios for working with triggers in this context:

1. **Creating a new event-driven trigger**: If you need a new trigger for an unsupported event source, you should create
a new class inheriting from ``BaseEventTrigger`` and implement its logic.

2. **Adapting an existing compatible trigger**: If an existing trigger (inheriting from ``BaseTrigger``) is proven to be
already compatible with event-driven scheduling, then you just need to change the base class from ``BaseTrigger`` to
``BaseEventTrigger``.

3. **Adapting an existing incompatible trigger**: If an existing trigger does not appear to be compatible with
event-driven scheduling, then a new trigger must be created.
This new trigger must inherit ``BaseEventTrigger`` and ensure it properly works with event-driven scheduling.
It might inherit from the existing trigger as well if both triggers share some common code.

Avoid infinite scheduling
~~~~~~~~~~~~~~~~~~~~~~~~~

The reason why some triggers are not compatible with event-driven scheduling is that they are waiting
for an external resource to reach a given state. Examples:

* Wait for a file to exist in a storage service
* Wait for a job to be in a success state
* Wait for a row to be present in a database

Scheduling under such conditions can lead to infinite rescheduling. This is because once the condition becomes true,
it is likely to remain true for an extended period.

For example, consider a Dag scheduled to run when a specific job reaches a "success" state.
Once the job succeeds, it will typically remain in that state. As a result, the Dag will be triggered repeatedly every
time the triggerer checks the condition.

Another example is the ``S3KeyTrigger``, which checks for the presence of a specific file in an S3 bucket.
Once the file is created, the trigger will continue to succeed on every check, since the condition
"is file X present in bucket Y" remains true.
This leads to the Dag being triggered indefinitely every time the trigger mechanism runs.

When creating custom triggers, be cautious about using conditions that remain permanently true once met.
This can unintentionally result in infinite Dag executions and overwhelm your system.

Use cases for event-driven Dags
-------------------------------

* **Data ingestion pipelines**: Trigger ETL workflows when new data arrives in a storage system.

* **Machine learning workflows**: Start training models when new datasets become available.

* **IoT and real-time analytics**: React to sensor data, logs, or application events in real-time.

* **Microservices and event-driven architectures**: Orchestrate workflows based on service-to-service messages.
