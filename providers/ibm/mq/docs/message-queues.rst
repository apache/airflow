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

.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.


IBM MQ Message Queue
====================

.. contents::
   :local:
   :depth: 2


IBM MQ Queue Provider
---------------------

Implemented by :class:`~airflow.providers.ibm.mq.queues.mq.IBMMQMessageQueueProvider`

The IBM MQ Queue Provider is a
:class:`~airflow.providers.common.messaging.providers.base_provider.BaseMessageQueueProvider`
that uses IBM MQ as the underlying message queue system.

It allows you to send and receive messages using IBM MQ queues in your Airflow workflows
via the common message queue interface
:class:`~airflow.providers.common.messaging.triggers.msg_queue.MessageQueueTrigger`.


.. include:: /../src/airflow/providers/ibm/mq/queues/mq.py
    :start-after: [START ibmmq_message_queue_provider_description]
    :end-before: [END ibmmq_message_queue_provider_description]


.. _howto/triggers:IBMMQMessageQueueTrigger:


IBM MQ Message Queue Trigger
----------------------------

Implemented by :class:`~airflow.providers.ibm.mq.triggers.mq.AwaitMessageTrigger`

Inherited from
:class:`~airflow.providers.common.messaging.triggers.msg_queue.MessageQueueTrigger`

Wait for a message in a queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Below is an example of how you can configure an Airflow DAG to be triggered
by a message arriving in an IBM MQ queue.

.. exampleinclude:: /../tests/system/ibm/mq/example_dag_message_queue_trigger.py
    :language: python
    :start-after: [START howto_trigger_message_queue]
    :end-before: [END howto_trigger_message_queue]


How it works
------------

1. **IBM MQ Message Queue Trigger**
   The ``AwaitMessageTrigger`` listens for messages from an IBM MQ queue.

2. **Asset and Watcher**
   The ``Asset`` abstracts the external entity, the IBM MQ queue in this example.
   The ``AssetWatcher`` associates a trigger with a name. This name helps you
   identify which trigger is associated with which asset.

3. **Event-Driven DAG**
   Instead of running on a fixed schedule, the DAG executes when the asset receives
   an update (for example, when a new message arrives in the queue).

For how to use the trigger, refer to the documentation of the
:ref:`Messaging Trigger <howto/trigger:MessageQueueTrigger>`.
