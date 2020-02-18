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

from airflow.hooks import base_hook as BaseHook
from kafka import KafkaConsumer


class KafkaConsumerHook(BaseHook):

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 9092

    def __init__(self, conn_id, topic):
        super(KafkaConsumerHook, self).__init__(None)
        self.conn = self.get_connection(conn_id)
        self.server = None
        self.consumer = None
        self.producer = None
        self.topic = topic

    def get_conn(self):
        conf = self.conn.extra_dejson
        host = self.conn.host or self.DEFAULT_HOST
        port = self.conn.port or self.DEFAULT_PORT

        # Disable auto commit as the hook will commit right
        # after polling.
        conf['enable_auto_commit'] = False

        self.server = f"""{host}:{port}"""
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.server, **conf)

        return self.consumer

    def get_messages(self, timeout_ms=50):
        """
        Get all the messages haven't been consumed, it doesn't
        block by default, then commit the offset.
        :return:
            A list of messages
        """
        consumer = self.get_conn()
        try:
            # `poll` returns a dict where keys are the partitions
            # and values are the corresponding messages.
            messages = consumer.poll(timeout_ms)

            consumer.commit()
        finally:
            consumer.close()
        return messages

    def __repr__(self):
        """
        Pretty the hook with the connection info
        """
        connected = self.consumer is not None
        return '<KafkaConsumerHook ' \
               'connected?=%s server=%s topic=%s>' % \
               (connected, self.server, self.topic)
