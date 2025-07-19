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

.. example::

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

.. example::

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

Queue URI Format:

.. code-block:: text

   azure_service_bus_queue://<connection_id>/<queue_list>

Where:

- ``connection_id``: The Airflow connection ID for Azure Service Bus.
- ``queue_list``: Comma-separated list of Azure Service Bus queues to monitor.

The ``queues`` parameter is used to configure the underlying
:class:`~airflow.providers.microsoft.azure.triggers.message_bus.AzureServiceBusQueueTrigger` class.

Queues can also be specified via the Queue URI instead of the ``queues`` kwarg. The provider will extract queues from the URI.

Azure Service Bus Subscription Provider
---------------------------------------

Implemented by :class:`~airflow.providers.microsoft.azure.triggers.message_bus.AzureServiceBusSubscriptionTrigger`

The Azure Service Bus Subscription Provider is a message queue provider that uses Azure Service Bus topic subscriptions.
It allows you to monitor Azure Service Bus topic subscriptions for new messages and trigger DAG runs when messages arrive.

Queue URI Format:

.. code-block:: text

   azure_service_bus_subscription://<connection_id>/<topic_list>?subscription_name=<subscription_name>

Where:

- ``connection_id``: The Airflow connection ID for Azure Service Bus.
- ``topic_list``: Comma-separated list of Azure Service Bus topics to monitor.
- ``subscription_name``: The name of the subscription to use.

The ``topics`` and ``subscription_name`` parameters are used to configure the underlying
:class:`~airflow.providers.microsoft.azure.triggers.message_bus.AzureServiceBusSubscriptionTrigger` class.

Topics can also be specified via the Queue URI instead of the ``topics`` kwarg. The provider will extract topics from the URI.
``subscription_name`` must be specified in the URI.
