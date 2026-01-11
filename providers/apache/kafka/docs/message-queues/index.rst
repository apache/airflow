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


Apache Kafka Message Queue
==========================

Apache Kafka Queue Provider
---------------------------

Implemented by :class:`~airflow.providers.apache.kafka.queues.kafka.KafkaMessageQueueProvider`


The Apache Kafka Queue Provider is a :class:`~airflow.providers.common.messaging.providers.base_provider.BaseMessageQueueProvider` that uses
Apache Kafka as the underlying message queue system.
It allows you to send and receive messages using Kafka topics in your Airflow workflows with :class:`~airflow.providers.common.messaging.triggers.msg_queue.MessageQueueTrigger` common message queue interface.


.. include:: /../src/airflow/providers/apache/kafka/queues/kafka.py
    :start-after: [START kafka_message_queue_provider_description]
    :end-before: [END kafka_message_queue_provider_description]


.. _howto/triggers:KafkaMessageQueueTrigger:

Apache Kafka Message Queue Trigger
----------------------------------

Implemented by :class:`~airflow.providers.apache.kafka.triggers.msg_queue.KafkaMessageQueueTrigger`

Inherited from :class:`~airflow.providers.common.messaging.triggers.msg_queue.MessageQueueTrigger`

Wait for a message in a queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Below is an example of how you can configure an Airflow Dag to be triggered by a message in Apache Kafka.

.. exampleinclude:: /../tests/system/apache/kafka/example_dag_kafka_message_queue_trigger.py
    :language: python
    :start-after: [START howto_trigger_message_queue]
    :end-before: [END howto_trigger_message_queue]


How it works
------------

1. **Kafka Message Queue Trigger**: The ``KafkaMessageQueueTrigger`` listens for messages from Apache Kafka Topic(s).

2. **Asset and Watcher**: The ``Asset`` abstracts the external entity, the Kafka queue in this example.
The ``AssetWatcher`` associate a trigger with a name. This name helps you identify which trigger is associated to which
asset.

3. **Event-Driven Dag**: Instead of running on a fixed schedule, the Dag executes when the asset receives an update
(e.g., a new message in the queue).

For how to use the trigger, refer to the documentation of the
:ref:`Messaging Trigger <howto/trigger:MessageQueueTrigger>`
