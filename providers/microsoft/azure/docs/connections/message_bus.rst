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

.. _howto/trigger:azure_service_bus_queue:

Microsoft Azure Service Bus Queue Trigger
=========================================

The Microsoft Azure Service Bus Queue trigger enables you to monitor Azure Service Bus queues for new messages and trigger DAG runs when messages arrive.

Authenticating to Azure
-----------------------

The trigger uses the connection you have configured in Airflow to connect to Azure.
Please refer to the :ref:`howto/connection:azure_service_bus` documentation for details on how to configure your connection.

Using the Trigger
-----------------

This example shows how to use the ``AzureServiceBusQueueTrigger``.

.. code-block:: python

      from airflow.providers.microsoft.azure.triggers.message_bus import AzureServiceBusQueueTrigger

      trigger = AzureServiceBusQueueTrigger(
          queues=["my_queue"],
          azure_service_bus_conn_id="azure_service_bus_default",
          max_message_count=1,
          poll_interval=5.0,
      )

      # The trigger will fire when a message is available in the queue.

.. _howto/trigger:azure_service_bus_subscription:

Microsoft Azure Service Bus Subscription Trigger
================================================

The Microsoft Azure Service Bus Subscription trigger enables you to monitor Azure Service Bus topic subscriptions for new messages and trigger DAG runs when messages arrive.

Authenticating to Azure
-----------------------

The trigger uses the connection you have configured in Airflow to connect to Azure.
Please refer to the :ref:`howto/connection:azure_service_bus` documentation for details on how to configure your connection.

Using the Trigger
-----------------

This example shows how to use the ``AzureServiceBusSubscriptionTrigger``.

.. code-block:: python

      from airflow.providers.microsoft.azure.triggers.message_bus import AzureServiceBusSubscriptionTrigger

      trigger = AzureServiceBusSubscriptionTrigger(
          topics=["my_topic"],
          subscription_name="my_subscription",
          azure_service_bus_conn_id="azure_service_bus_default",
          max_message_count=1,
          poll_interval=5.0,
      )

      # The trigger will fire when a message is available in the topic subscription.

Azure Service Bus Message Queue
===============================

Azure Service Bus Queue Provider
--------------------------------

Implemented by :class:`~airflow.providers.microsoft.azure.triggers.message_bus.AzureServiceBusQueueTrigger`

The Azure Service Bus Queue Provider is a message queue provider that uses Azure Service Bus queues.
It allows you to monitor Azure Service Bus queues for new messages and trigger DAG runs when messages arrive.


Azure Service Bus Subscription Provider
---------------------------------------

Implemented by :class:`~airflow.providers.microsoft.azure.triggers.message_bus.AzureServiceBusSubscriptionTrigger`

The Azure Service Bus Subscription Provider is a message queue provider that uses Azure Service Bus topic subscriptions.
It allows you to monitor Azure Service Bus topic subscriptions for new messages and trigger DAG runs when messages arrive.
