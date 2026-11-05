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


Apache Kafka Triggers
=====================

.. _howto/triggers:AwaitMessageTrigger:

AwaitMessageTrigger
------------------------

The ``AwaitMessageTrigger`` is a trigger that will consume messages polled from a Kafka topic and process them with a provided callable.
If the callable returns any data, a TriggerEvent is raised.

For parameter definitions take a look at :class:`~airflow.providers.apache.kafka.triggers.await_message.AwaitMessageTrigger`.


KafkaMessageQueueTrigger
----------------------------------

The ``KafkaMessageQueueTrigger`` is a dedicated interface class for Kafka message queues that extends
the common :class:`~airflow.providers.common.messaging.trigger.msg_queue.MessageQueueTrigger`. It is designed to work with the ``KafkaMessageQueueProvider`` and provides
a more specific interface for Kafka message queue operations while leveraging the unified messaging framework.

For parameter definitions take a look at :class:`~airflow.providers.apache.kafka.triggers.msg_queue.KafkaMessageQueueTrigger`

For how to use the trigger, refer to the documentation of the :ref:`Apache Kafka Message Queue Trigger <howto/triggers:KafkaMessageQueueTrigger>`


.. _howto/triggers:KafkaSharedStreamTrigger:

KafkaSharedStreamTrigger
------------------------

The ``KafkaSharedStreamTrigger`` lets an :class:`~airflow.sdk.AssetWatcher` watch Kafka topics. Triggers with the
same ``topics`` and ``kafka_config_id`` share one Kafka consumer in the triggerer instead of each opening their own.
``KafkaSharedStreamTrigger`` needs Airflow 3.3 or later.

The Kafka connection named by ``kafka_config_id`` must set ``enable.auto.commit`` to ``false`` in its extra field, or
the trigger refuses to start. The shared consumer commits the offset of a message only after the trigger events from
that message are saved to the metadata database. With auto-commit on, the Kafka client would commit offsets on a timer,
possibly before the trigger events are saved. A triggerer crash at that point would lose the message, because Kafka
would not deliver it again.

The same message can still reach a trigger more than once, for example when the triggerer restarts after saving the
trigger events but before committing the offset. In that case the watched asset gets more than one asset event for
the message.

.. code-block:: python

    from airflow.providers.apache.kafka.triggers.shared_stream import KafkaSharedStreamTrigger
    from airflow.sdk import DAG, Asset, AssetWatcher

    trigger = KafkaSharedStreamTrigger(topics=["orders"], kafka_config_id="kafka_default")
    asset = Asset("kafka_orders", watchers=[AssetWatcher(name="orders_watcher", trigger=trigger)])

    with DAG(dag_id="process_orders", schedule=[asset]):
        ...

For parameter definitions take a look at :class:`~airflow.providers.apache.kafka.triggers.shared_stream.KafkaSharedStreamTrigger`.
