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

Amazon Managed Streaming for Apache Kafka
=========================================

`Amazon MSK <https://aws.amazon.com/msk/>`_ is a managed Apache Kafka service. The Amazon
provider's :class:`~airflow.providers.amazon.aws.hooks.msk.MskHook` provides a boto3 client for
managing MSK clusters and can generate IAM authentication tokens for Kafka clients. The
Apache Kafka provider handles producing and consuming topic messages.

Trigger a Dag from an MSK topic
-------------------------------

Use :class:`~airflow.providers.apache.kafka.triggers.msg_queue.KafkaMessageQueueTrigger` with
``AssetWatcher`` to trigger a Dag when a message arrives. Set the trigger's ``kafka_config_id``
to a Kafka connection configured for the MSK brokers. There is no separate Amazon MSK message
queue trigger; the standard Kafka trigger works with MSK because MSK uses the Kafka protocol.
See the `Kafka message queue trigger guide
<https://airflow.apache.org/docs/apache-airflow-providers-apache-kafka/stable/message-queues/index.html>`_
for an ``AssetWatcher`` example.

Configure the Kafka connection
------------------------------

The `Apache Kafka connection guide
<https://airflow.apache.org/docs/apache-airflow-providers-apache-kafka/stable/connections/kafka.html>`_
explains both automatic MSK IAM authentication and explicit ``oauth_cb`` configuration using an
AWS connection. The Amazon provider supplies the ``oauth_cb`` token callback; the Kafka provider
uses it when connecting to MSK.
