#
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

from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata

from airflow.hooks.base_hook import BaseHook


class KafkaProducerHook(BaseHook):
    """
    KafkaProducerHook Class.
    """
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 9092

    def __init__(self, conn_id, topic):
        super(KafkaProducerHook, self).__init__(None)
        self.conn_id = conn_id
        self._conn = None
        self.server = None
        self.consumer = None
        self.producer = None
        self.topic = topic

    def get_conn(self) -> KafkaProducer:
        """
            Returns a Kafka Producer

        :return:
            A Kafka Producer object.
        """
        if not self._conn:
            _conn = self.get_connection(self.conn_id)
            service_options = _conn.extra_dejson
            host = _conn.host or self.DEFAULT_HOST
            port = _conn.port or self.DEFAULT_PORT

            self.server = f"""{host}:{port}"""
            self.consumer = KafkaProducer(
                bootstrap_servers=self.server,
                **service_options
            )
        return self.producer

    def send_message(self, topic, value=None, key=None, partition=None, timestamp_ms=None) -> FutureRecordMetadata:
        """
            Sends a message on the specified topic and partition.  Keyed messages will be sent in order.

        :param topic:
        :param value:
        :param key:
        :param partition:
        :param timestamp_ms:
        :return:
        """
        producer = self.get_conn()
        try:
            future_record_metadata = producer.send(topic, value=value, key=key, partition=partition,
                                                   timestamp_ms=timestamp_ms)
        finally:
            producer.close()
        return future_record_metadata

    def __repr__(self):
        """
            A pretty version of the connection string.

        :return:
            A pretty version of the connection string.
        """
        connected = self.producer is not None
        return '<KafkaProducerHook ' \
               'connected?=%s server=%s topic=%s>' % \
               (connected, self.server, self.topic)
