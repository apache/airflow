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


Apache Kafka Operators
======================

.. _howto/operator:ConsumeFromTopicOperator:

ConsumeFromTopicOperator
------------------------

An operator that consumes from one or more Kafka topic(s) and processes the messages.
The operator creates a Kafka Consumer that reads a batch of messages from the cluster and processes them using the user-supplied callable ``apply_function``. The consumer will continue to read in batches until it reaches the end of the log or a maximum number of messages read (``max_messages``) is reached.

For parameter definitions take a look at :class:`~airflow.providers.apache.kafka.operators.consume.ConsumeFromTopicOperator`.


Using the operator
""""""""""""""""""

.. exampleinclude:: /../../providers/tests/system/apache/kafka/example_dag_hello_kafka.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_consume_from_topic]
    :end-before: [END howto_operator_consume_from_topic]


Reference
"""""""""

For further information, see the `Apache Kafka Consumer documentation <https://kafka.apache.org/documentation/#consumerconfigs>`_.


.. _howto/operator:ProduceToTopicOperator:

ProduceToTopicOperator
------------------------

An operator that produces messages to a Kafka topic. The operator will produce messages created as key/value pairs by the user-supplied ``producer_function``.

For parameter definitions take a look at :class:`~airflow.providers.apache.kafka.operators.produce.ProduceToTopicOperator`.

Using the operator
""""""""""""""""""

.. exampleinclude:: /../../providers/tests/system/apache/kafka/example_dag_hello_kafka.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_produce_to_topic]
    :end-before: [END howto_operator_produce_to_topic]


Reference
"""""""""

For further information, see the `Apache Kafka Producer documentation <https://kafka.apache.org/documentation/#producerconfigs>`_.
