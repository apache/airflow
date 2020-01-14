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



Google Cloud PubSub Operators
=============================

`Google Cloud PubSub <https://cloud.google.com/pubsub/>`__ is a fully-managed real-time
messaging service that allows you to send and receive messages between independent applications.
You can leverage Cloud Pub/Sub’s flexibility to decouple systems and components hosted
on Google Cloud Platform or elsewhere on the Internet.

Publisher applications can send messages to a topic and other applications can subscribe to that topic to receive the messages.
By decoupling senders and receivers Google Cloud PubSub allows developers to communicate between independently written applications.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst

.. _howto/operator:PubSubCreateTopicOperator:

Creating a PubSub topic
^^^^^^^^^^^^^^^^^^^^^^^

The PubSub topic is a named resource to which messages are sent by publishers.
The :class:`~airflow.providers.google.cloud.operators.pubsub.PubSubCreateTopicOperator` operator creates a topic.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_pubsub.py
    :language: python
    :start-after: [START howto_operator_gcp_pubsub_create_topic]
    :end-before: [END howto_operator_gcp_pubsub_create_topic]


.. _howto/operator:PubSubCreateSubscriptionOperator:

Creating a PubSub subscription
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A ``Subscription`` is a named resource representing the stream of messages from a single, specific topic,
to be delivered to the subscribing application.
The :class:`~airflow.providers.google.cloud.operators.pubsub.PubSubCreateSubscriptionOperator` operator creates the subscription.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_pubsub.py
    :language: python
    :start-after: [START howto_operator_gcp_pubsub_create_subscription]
    :end-before: [END howto_operator_gcp_pubsub_create_subscription]


.. _howto/operator:PubSubPublishMessageOperator:

Publishing PubSub messages
^^^^^^^^^^^^^^^^^^^^^^^^^^

A ``Message`` is a combination of data and (optional) attributes that a publisher sends to a topic and is eventually delivered to subscribers.
The :class:`~airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator` operator would publish messages.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_pubsub.py
    :language: python
    :start-after: [START howto_operator_gcp_pubsub_publish]
    :end-before: [END howto_operator_gcp_pubsub_publish]


.. _howto/operator:PubSubPullSensor:

Pulling messages from a PubSub subscription
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.google.cloud.sensors.pubsub.PubSubPullSensor` sensor pulls messages from a PubSub subscription
and pass them through XCom.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_pubsub.py
    :language: python
    :start-after: [START howto_operator_gcp_pubsub_pull_message]
    :end-before: [END howto_operator_gcp_pubsub_pull_message]

To pull messages from XCom use the :class:`~airflow.operators.bash_operator.BashOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_pubsub.py
    :language: python
    :start-after: [START howto_operator_gcp_pubsub_pull_messages_result_cmd]
    :end-before: [END howto_operator_gcp_pubsub_pull_messages_result_cmd]

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_pubsub.py
    :language: python
    :start-after: [START howto_operator_gcp_pubsub_pull_messages_result]
    :end-before: [END howto_operator_gcp_pubsub_pull_messages_result]


.. _howto/operator:PubSubDeleteSubscriptionOperator:

Deleting a PubSub subscription
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.google.cloud.operators.pubsub.PubSubDeleteSubscriptionOperator` operator deletes the subscription.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_pubsub.py
    :language: python
    :start-after: [START howto_operator_gcp_pubsub_unsubscribe]
    :end-before: [END howto_operator_gcp_pubsub_unsubscribe]


.. _howto/operator:PubSubDeleteTopicOperator:

Deleting a PubSub topic
^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.google.cloud.operators.pubsub.PubSubDeleteTopicOperator` operator deletes topic.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_pubsub.py
    :language: python
    :start-after: [START howto_operator_gcp_pubsub_delete_topic]
    :end-before: [END howto_operator_gcp_pubsub_delete_topic]


Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.dev/python/pubsub/latest/index.html>`__
* `Product Documentation <https://cloud.google.com/pubsub/docs/>`__
