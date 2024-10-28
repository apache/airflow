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

from typing import Sequence

from confluent_kafka import Consumer

from airflow.providers.apache.kafka.hooks.base import KafkaBaseHook


class KafkaConsumerHook(KafkaBaseHook):
    """
    A hook for creating a Kafka Consumer.

    :param kafka_config_id: The connection object to use, defaults to "kafka_default"
    :param topics: A list of topics to subscribe to.
    """

    def __init__(
        self, topics: Sequence[str], kafka_config_id=KafkaBaseHook.default_conn_name
    ) -> None:
        super().__init__(kafka_config_id=kafka_config_id)
        self.topics = topics

    def _get_client(self, config) -> Consumer:
        return Consumer(config)

    def get_consumer(self) -> Consumer:
        """Return a Consumer that has been subscribed to topics."""
        consumer = self.get_conn
        consumer.subscribe(self.topics)

        return consumer
