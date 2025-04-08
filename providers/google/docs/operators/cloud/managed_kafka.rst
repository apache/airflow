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

Google Cloud Managed Service for Apache Kafka Operators
=======================================================

The `Google Cloud Managed Service for Apache Kafka <https://cloud.google.com/managed-service-for-apache-kafka/docs>`__
helps you set up, secure, maintain, and scale Apache Kafka clusters.

Interacting with Apache Kafka Cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create an Apache Kafka cluster you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaCreateClusterOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_create_cluster_operator]
    :end-before: [END how_to_cloud_managed_kafka_create_cluster_operator]

To delete cluster you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaDeleteClusterOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_delete_cluster_operator]
    :end-before: [END how_to_cloud_managed_kafka_delete_cluster_operator]

To get cluster you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaGetClusterOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_get_cluster_operator]
    :end-before: [END how_to_cloud_managed_kafka_get_cluster_operator]

To get a list of clusters you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaListClustersOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_list_cluster_operator]
    :end-before: [END how_to_cloud_managed_kafka_list_cluster_operator]

To update cluster you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaUpdateClusterOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_update_cluster_operator]
    :end-before: [END how_to_cloud_managed_kafka_update_cluster_operator]

Interacting with Apache Kafka Topics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create an Apache Kafka topic you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaCreateTopicOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_topic.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_create_topic_operator]
    :end-before: [END how_to_cloud_managed_kafka_create_topic_operator]

To delete topic you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaDeleteTopicOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_topic.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_delete_topic_operator]
    :end-before: [END how_to_cloud_managed_kafka_delete_topic_operator]

To get topic you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaGetTopicOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_topic.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_get_topic_operator]
    :end-before: [END how_to_cloud_managed_kafka_get_topic_operator]

To get a list of topics you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaListTopicsOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_topic.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_list_topic_operator]
    :end-before: [END how_to_cloud_managed_kafka_list_topic_operator]

To update topic you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaUpdateTopicOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_topic.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_update_topic_operator]
    :end-before: [END how_to_cloud_managed_kafka_update_topic_operator]

Interacting with Apache Kafka Consumer Groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To delete consumer group you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaDeleteConsumerGroupOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_consumer_group.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_delete_consumer_group_operator]
    :end-before: [END how_to_cloud_managed_kafka_delete_consumer_group_operator]

To get consumer group you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaGetConsumerGroupOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_consumer_group.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_get_consumer_group_operator]
    :end-before: [END how_to_cloud_managed_kafka_get_consumer_group_operator]

To get a list of consumer groups you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaListConsumerGroupsOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_consumer_group.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_list_consumer_group_operator]
    :end-before: [END how_to_cloud_managed_kafka_list_consumer_group_operator]

To update consumer group you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaUpdateConsumerGroupOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_consumer_group.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_update_consumer_group_operator]
    :end-before: [END how_to_cloud_managed_kafka_update_consumer_group_operator]

Using Apache Kafka provider with Google Cloud Managed Service for Apache Kafka
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To produce data to topic you can use
:class:`~airflow.providers.apache.kafka.operators.produce.ProduceToTopicOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_consumer_group.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_produce_to_topic_operator]
    :end-before: [END how_to_cloud_managed_kafka_produce_to_topic_operator]

To consume data from topic you can use
:class:`~airflow.providers.apache.kafka.operators.produce.ConsumeFromTopicOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/managed_kafka/example_managed_kafka_consumer_group.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_consume_from_topic_operator]
    :end-before: [END how_to_cloud_managed_kafka_consume_from_topic_operator]

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://cloud.google.com/managed-service-for-apache-kafka/docs/reference/libraries>`__
* `Product Documentation <https://cloud.google.com/managed-service-for-apache-kafka/docs>`__
