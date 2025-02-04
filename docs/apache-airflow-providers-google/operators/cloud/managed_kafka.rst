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

.. exampleinclude:: /../../providers/tests/system/google/cloud/managed_kafka/example_managed_kafka_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_create_cluster_operator]
    :end-before: [END how_to_cloud_managed_kafka_create_cluster_operator]

To delete cluster you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaDeleteClusterOperator`.

.. exampleinclude:: /../../providers/tests/system/google/cloud/managed_kafka/example_managed_kafka_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_delete_cluster_operator]
    :end-before: [END how_to_cloud_managed_kafka_delete_cluster_operator]

To get cluster you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaGetClusterOperator`.

.. exampleinclude:: /../../providers/tests/system/google/cloud/managed_kafka/example_managed_kafka_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_get_cluster_operator]
    :end-before: [END how_to_cloud_managed_kafka_get_cluster_operator]

To get a list of clusters you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaListClustersOperator`.

.. exampleinclude:: /../../providers/tests/system/google/cloud/managed_kafka/example_managed_kafka_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_list_cluster_operator]
    :end-before: [END how_to_cloud_managed_kafka_list_cluster_operator]

To update cluster you can use
:class:`~airflow.providers.google.cloud.operators.managed_kafka.ManagedKafkaUpdateClusterOperator`.

.. exampleinclude:: /../../providers/tests/system/google/cloud/managed_kafka/example_managed_kafka_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_managed_kafka_update_cluster_operator]
    :end-before: [END how_to_cloud_managed_kafka_update_cluster_operator]

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://cloud.google.com/managed-service-for-apache-kafka/docs/reference/libraries>`__
* `Product Documentation <https://cloud.google.com/managed-service-for-apache-kafka/docs>`__
