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

Azure Service Bus Operators
============================
Azure Service Bus is a fully managed enterprise message broker with message queues and
publish-subscribe topics (in a namespace). Service Bus is used to decouple applications
and services from each other. Service Bus that perform operations on
entities, such as namespaces, queues, and topics.

The Service Bus REST API provides operations for working with the following resources:
  - Azure Resource Manager
  - Service Bus service

Azure Service Bus Queue Operators
---------------------------------
Azure Service Bus Operators helps to interact with Azure Bus Queue based operation like Create, Delete,
Send and Receive message in Queue.

.. _howto/operator:AzureServiceBusCreateQueueOperator:

Create Azure Service Bus Queue
===============================

To create Azure service bus queue with specific Parameter you can use
:class:`~airflow.providers.microsoft.azure.operators.asb.AzureServiceBusCreateQueueOperator`.

Below is an example of using this operator to execute an Azure Service Bus Create Queue.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_azure_service_bus.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_service_bus_queue]
    :end-before: [END howto_operator_create_service_bus_queue]


.. _howto/operator:AzureServiceBusSendMessageOperator:

Send Message to Azure Service Bus Queue
=======================================

To Send message or list of message or batch Message to the Azure Service Bus Queue. You can use
:class:`~airflow.providers.microsoft.azure.operators.asb.AzureServiceBusSendMessageOperator`.

Below is an example of using this operator to execute an Azure Service Bus Send Message to Queue.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_azure_service_bus.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_send_message_to_service_bus_queue]
    :end-before: [END howto_operator_send_message_to_service_bus_queue]


.. _howto/operator:AzureServiceBusReceiveMessageOperator:

Receive Message Azure Service Bus Queue
========================================

To Receive Message or list of message or Batch message in a Queue you can use
:class:`~airflow.providers.microsoft.azure.operators.asb.AzureServiceBusReceiveMessageOperator`.

Below is an example of using this operator to execute an Azure Service Bus Create Queue.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_azure_service_bus.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_receive_message_service_bus_queue]
    :end-before: [END howto_operator_receive_message_service_bus_queue]


.. _howto/operator:AzureServiceBusDeleteQueueOperator:

Delete Azure Service Bus Queue
===============================

To Delete the Azure service bus queue you can use
:class:`~airflow.providers.microsoft.azure.operators.asb.AzureServiceBusDeleteQueueOperator`.

Below is an example of using this operator to execute an Azure Service Bus Delete Queue.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_azure_service_bus.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_delete_service_bus_queue]
    :end-before: [END howto_operator_delete_service_bus_queue]

Azure Service Bus Topic Operators
-----------------------------------------
Azure Service Bus Topic based Operators helps to interact with topic in service bus namespace
and it helps to Create, Delete operation for topic.

.. _howto/operator:AzureServiceBusTopicCreateOperator:

Create Azure Service Bus Topic
======================================

To create Azure service bus topic with specific Parameter you can use
:class:`~airflow.providers.microsoft.azure.operators.asb.AzureServiceBusTopicCreateOperator`.

Below is an example of using this operator to execute an Azure Service Bus Create Topic.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_azure_service_bus.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_service_bus_topic]
    :end-before: [END howto_operator_create_service_bus_topic]

.. _howto/operator:AzureServiceBusTopicDeleteOperator:

Delete Azure Service Bus Topic
======================================

To Delete the Azure service bus topic you can use
:class:`~airflow.providers.microsoft.azure.operators.asb.AzureServiceBusTopicDeleteOperator`.

Below is an example of using this operator to execute an Azure Service Bus Delete topic.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_azure_service_bus.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_delete_service_bus_topic]
    :end-before: [END howto_operator_delete_service_bus_topic]

Azure Service Bus Subscription Operators
-----------------------------------------
Azure Service Bus Subscription based Operators helps to interact topic Subscription in service bus namespace
and it helps to Create, Delete operation for subscription under topic.

.. _howto/operator:AzureServiceBusSubscriptionCreateOperator:

Create Azure Service Bus Subscription
======================================

To create Azure service bus topic Subscription with specific Parameter you can use
:class:`~airflow.providers.microsoft.azure.operators.asb.AzureServiceBusSubscriptionCreateOperator`.

Below is an example of using this operator to execute an Azure Service Bus Create Subscription.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_azure_service_bus.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_service_bus_subscription]
    :end-before: [END howto_operator_create_service_bus_subscription]

.. _howto/operator:AzureServiceBusUpdateSubscriptionOperator:

Update Azure Service Bus Subscription
======================================

To Update the Azure service bus topic Subscription which is already created, with specific Parameter you can use
:class:`~airflow.providers.microsoft.azure.operators.asb.AzureServiceBusUpdateSubscriptionOperator`.

Below is an example of using this operator to execute an Azure Service Bus Update Subscription.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_azure_service_bus.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_update_service_bus_subscription]
    :end-before: [END howto_operator_update_service_bus_subscription]

.. _howto/operator:ASBReceiveSubscriptionMessageOperator:

Receive Azure Service Bus Subscription Message
===============================================

To Receive a Batch messages from a Service Bus Subscription under specific Topic, you can use
:class:`~airflow.providers.microsoft.azure.operators.asb.ASBReceiveSubscriptionMessageOperator`.

Below is an example of using this operator to execute an Azure Service Bus Receive Subscription Message.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_azure_service_bus.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_receive_message_service_bus_subscription]
    :end-before: [END howto_operator_receive_message_service_bus_subscription]

.. _howto/operator:AzureServiceBusSubscriptionDeleteOperator:

Delete Azure Service Bus Subscription
======================================

To Delete the Azure service bus topic Subscription you can use
:class:`~airflow.providers.microsoft.azure.operators.asb.AzureServiceBusSubscriptionDeleteOperator`.

Below is an example of using this operator to execute an Azure Service Bus Delete Subscription under topic.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_azure_service_bus.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_delete_service_bus_subscription]
    :end-before: [END howto_operator_delete_service_bus_subscription]



Reference
---------

For further information, please refer to the Microsoft documentation:

  * `Azure Service Bus Documentation <https://azure.microsoft.com/en-us/services/service-bus/>`__
