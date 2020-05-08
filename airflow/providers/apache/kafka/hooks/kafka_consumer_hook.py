# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from kafka import KafkaConsumer

from airflow.hooks.base_hook import BaseHook


class KafkaConsumerHook(BaseHook):
    """
    KafkaConsumerHook Class.
    """
    DEFAULT_HOST = 'kafka1'
    DEFAULT_PORT = 9092

    def __init__(self, topic, host=DEFAULT_HOST, port=DEFAULT_PORT, kafka_conn_id='kafka_default'):
        super(KafkaConsumerHook, self).__init__(None)
        self.conn_id = kafka_conn_id
        self._conn = None
        self.server = None
        self.consumer = None
        self.extra_dejson = {}
        self.topic = topic
        self.host = host
        self.port = port

    def get_conn(self) -> KafkaConsumer:
        """
            A Kafka Consumer object.

        :return:
            A Kafka Consumer object.
        """
        if not self._conn:
            conn = self.get_connection(self.conn_id)
            service_options = conn.extra_dejson
            host = conn.host or self.DEFAULT_HOST
            port = conn.port or self.DEFAULT_PORT

            self.server = f"""{host}:{port}"""
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.server,
                **service_options
            )
        return self.consumer

    def get_messages(self, timeout_ms=5000) -> dict:
        """
        Get all the messages haven't been consumed, it doesn't
        block by default, then commit the offset.

        :param timeout_ms:
        :return:
            A list of messages
        """
        consumer = self.get_conn()
        try:
            messages = consumer.poll(timeout_ms)
            # consumer.commit()
        finally:
            consumer.close()
        return messages

    def __repr__(self):
        """
            A pretty version of the connection string.

        :return:
            A pretty version of the connection string.
        """
        connected = self.consumer is not None
        return '<KafkaConsumerHook ' \
               'connected?=%s server=%s topic=%s>' % \
               (connected, self.server, self.topic)
