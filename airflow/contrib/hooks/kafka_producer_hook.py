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
from kafka import KafkaProducer


class KafkaProducerHook(BaseHook):

    default_host = 'localhost'
    default_port = 9092

    def __init__(self, conn_id, topic):
        super(KafkaConsumerHook, self).__init__(None)
        self.conn = self.get_connection(conn_id)
        self.server = None
        self.consumer = None
        self.producer = None
        self.topic = topic

    def get_conn(self):
        conf = self.conn.extra_dejson
        host = self.conn.host or self.default_host
        port = self.conn.port or self.default_port

        conf['enable_auto_commit'] = False
        self.server = '{host}:{port}'.format(**locals())
        self.producer = KafkaProducer(
            bootstrap_servers=self.server, **conf)

        return self.producer

    def send_message(self, topic, value=None, key=None, headers=None, partition=None, timestamp_ms=None):
        producer = self.get_pub()
        future_record_metadata = producer.send(topic, value=value, key=key, headers=headers, partition=partition,
                                               timestamp_ms=timestamp_ms)
        producer.close()
        return future_record_metadata

    def __repr__(self):
        """
        Pretty the hook with the connection info
        """
        connected = self.produer is not None
        return '<KafkaProducerHook ' \
               'connected?=%s server=%s topic=%s>' % \
               (connected, self.server, self.topic)
