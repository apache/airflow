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

from confluent_kafka import Producer

from airflow.providers.apache.kafka.hooks.base import KafkaBaseHook


class KafkaProducerHook(KafkaBaseHook):
    """
    A hook for creating a Kafka Producer

    :param kafka_config_id: The connection object to use, defaults to "kafka_default"
    """

    def __init__(self, kafka_config_id=KafkaBaseHook.default_conn_name) -> None:
        super().__init__(kafka_config_id=kafka_config_id)

    def _get_client(self, config) -> Producer:
        return Producer(config)

    def get_producer(self) -> Producer:
        """Returns a producer object for sending messages to Kafka"""
        producer = self.get_conn

        self.log.info("Producer %s", producer)
        return producer
