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

Azure Service Bus Message Queue
================================

.. contents::
   :local:
   :depth: 2

Azure Service Bus Queue Provider
---------------------------------

Implemented by :class:`~airflow.providers.microsoft.azure.queues.asb.AzureServiceBusMessageQueueProvider`

The Azure Service Bus Queue Provider is a :class:`~airflow.providers.common.messaging.providers.base_provider.BaseMessageQueueProvider` that uses
Azure Service Bus as the underlying message queue system.
It allows you to send and receive messages using Azure Service Bus queues in your Airflow workflows with :class:`~airflow.providers.common.messaging.triggers.msg_queue.MessageQueueTrigger` common message queue interface.


.. include:: /../src/airflow/providers/microsoft/azure/queues/asb.py
    :start-after: [START azure_servicebus_message_queue_provider_description]
    :end-before: [END azure_servicebus_message_queue_provider_description]

Azure Service Bus Message Queue Trigger
-----------------------------------------

Implemented by :class:`~airflow.providers.microsoft.azure.triggers.message_bus.AzureServiceBusQueueTrigger`

Inherited from :class:`~airflow.providers.common.messaging.triggers.msg_queue.MessageQueueTrigger`

Wait for a message in a queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Below is an example of how you can configure an Airflow DAG to be triggered by a message in Azure Service Bus.

.. exampleinclude:: /../tests/system/microsoft/azure/example_event_schedule_asb.py
    :language: python
    :start-after: [START howto_trigger_asb_message_queue]
    :end-before: [END howto_trigger_asb_message_queue]

How it works
------------

1. **Azure Service Bus Message Queue Trigger**: The ``AzureServiceBusQueueTrigger`` listens for messages from Azure Service Bus queue(s).

2. **Asset and Watcher**: The ``Asset`` abstracts the external entity, the Azure Service Bus queue in this example.
   The ``AssetWatcher`` associate a trigger with a name. This name helps you identify which trigger is associated to which
   asset.

3. **Event-Driven DAG**: Instead of running on a fixed schedule, the DAG executes when the asset receives an update
   (e.g., a new message in the queue).

For how to use the trigger, refer to the documentation of the
:ref:`Messaging Trigger <howto/trigger:MessageQueueTrigger>`
