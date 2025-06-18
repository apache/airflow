# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from airflow.providers.google.cloud.links.base import BaseGoogleLink

MANAGED_KAFKA_BASE_LINK = "/managedkafka"
MANAGED_KAFKA_CLUSTER_LINK = (
    MANAGED_KAFKA_BASE_LINK + "/{location}/clusters/{cluster_id}?project={project_id}"
)
MANAGED_KAFKA_CLUSTER_LIST_LINK = MANAGED_KAFKA_BASE_LINK + "/clusters?project={project_id}"
MANAGED_KAFKA_TOPIC_LINK = (
    MANAGED_KAFKA_BASE_LINK + "/{location}/clusters/{cluster_id}/topics/{topic_id}?project={project_id}"
)
MANAGED_KAFKA_CONSUMER_GROUP_LINK = (
    MANAGED_KAFKA_BASE_LINK
    + "/{location}/clusters/{cluster_id}/consumer_groups/{consumer_group_id}?project={project_id}"
)


class ApacheKafkaClusterLink(BaseGoogleLink):
    """Helper class for constructing Apache Kafka Cluster link."""

    name = "Apache Kafka Cluster"
    key = "cluster_conf"
    format_str = MANAGED_KAFKA_CLUSTER_LINK


class ApacheKafkaClusterListLink(BaseGoogleLink):
    """Helper class for constructing Apache Kafka Clusters link."""

    name = "Apache Kafka Cluster List"
    key = "cluster_list_conf"
    format_str = MANAGED_KAFKA_CLUSTER_LIST_LINK


class ApacheKafkaTopicLink(BaseGoogleLink):
    """Helper class for constructing Apache Kafka Topic link."""

    name = "Apache Kafka Topic"
    key = "topic_conf"
    format_str = MANAGED_KAFKA_TOPIC_LINK


class ApacheKafkaConsumerGroupLink(BaseGoogleLink):
    """Helper class for constructing Apache Kafka Consumer Group link."""

    name = "Apache Kafka Consumer Group"
    key = "consumer_group_conf"
    format_str = MANAGED_KAFKA_CONSUMER_GROUP_LINK
