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

from typing import Any, Sequence

from confluent_kafka import KafkaException
from confluent_kafka.admin import NewTopic

from airflow.providers.apache.kafka.hooks.base import KafkaBaseHook


class KafkaAdminClientHook(KafkaBaseHook):
    """
    A hook for interacting with the Kafka Cluster.

    :param kafka_config_id: The connection object to use, defaults to "kafka_default"
    """

    def __init__(self, kafka_config_id=KafkaBaseHook.default_conn_name) -> None:
        super().__init__(kafka_config_id=kafka_config_id)

    def create_topic(
        self,
        topics: Sequence[Sequence[Any]],
    ) -> None:
        """
        Create a topic.

        :param topics: a list of topics to create including the number of partitions for the topic
          and the replication factor. Format: [ ("topic_name", number of partitions, replication factor)]
        """
        admin_client = self.get_conn

        new_topics = [NewTopic(t[0], num_partitions=t[1], replication_factor=t[2]) for t in topics]

        futures = admin_client.create_topics(new_topics)

        for t, f in futures.items():
            try:
                f.result()
                self.log.info("The topic %s has been created.", t)
            except KafkaException as e:
                if e.args[0].name == "TOPIC_ALREADY_EXISTS":
                    self.log.warning("The topic %s already exists.", t)
                else:
                    raise

    def delete_topic(
        self,
        topics: Sequence[str],
    ) -> None:
        """
        Delete a topic.

        :param topics: a list of topics to delete.
        """
        admin_client = self.get_conn
        futures = admin_client.delete_topics(topics)

        for t, f in futures.items():
            f.result()
            self.log.info("The topic %s has been deleted.", t)
