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


Apache Kafka Hooks
==================

.. _howto/hook:KafkaHook:

KafkaHook
------------------------

A base hook for interacting with Apache Kafka. Use this hook as a base class when creating your own Kafka hooks.
For parameter definitions take a look at :class:`~airflow.providers.apache.kafka.hooks.base.KafkaHook`.


.. _howto/hook:KafkaAdminClientHook:

KafkaAdminClientHook
------------------------

A hook for interacting with an Apache Kafka cluster.
For parameter definitions take a look at :class:`~airflow.providers.apache.kafka.hooks.client.KafkaAdminClientHook`.

Reference
"""""""""

For further information, look at `Apache Kafka Admin config documentation <https://kafka.apache.org/documentation/#adminclientconfigs>`_.


.. _howto/hook:KafkaConsumerHook:

KafkaConsumerHook
------------------------

A hook for creating a Kafka Consumer. This hook is used by the ``ConsumeFromTopicOperator`` and the ``AwaitMessageTrigger``.
For parameter definitions take a look at :class:`~airflow.providers.apache.kafka.hooks.consume.KafkaConsumerHook`.

Reference
"""""""""

For further information, look at `Apache Kafka Consumer documentation <https://kafka.apache.org/documentation/#consumerconfigs>`_.


.. _howto/hook:KafkaProducerHook:

KafkaProducerHook
------------------------

A hook for creating a Kafka Producer. This hook is used by the ``ProduceToTopicOperator``.
For parameter definitions take a look at :class:`~airflow.providers.apache.kafka.hooks.produce.KafkaProducerHook`.

Reference
"""""""""

For further information, look at `Apache Kafka Producer documentation <https://kafka.apache.org/documentation/#producerconfigs>`_.
