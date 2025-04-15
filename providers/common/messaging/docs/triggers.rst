
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
queue. Parameters of the trigger are:

- ``queue`` - the queue identifier

Additional parameters can be provided depending on the queue provider. Connections needs to be provided with the relevant
default connection ID, for example, when connecting to a queue in AWS SQS, the connection ID should be
``aws_default``.

Below is an example of how you can configure an Airflow DAG to be triggered by a message in Amazon SQS.

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

3. **Event-Driven DAG**: Instead of running on a fixed schedule, the DAG executes when the asset receives an update
(e.g., a new message in the queue).
