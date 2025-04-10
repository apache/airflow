
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

Additional parameters can be provided depending on the queue provider.

The example below shows how to schedule a DAG using MessageQueueTrigger.

.. exampleinclude:: /../tests/system/common/messaging/example_message_queue_trigger.py
    :language: python
    :start-after: [START howto_trigger_message_queue]
    :end-before: [END howto_trigger_message_queue]
