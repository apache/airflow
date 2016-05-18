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

import unittest

from airflow import configuration
from airflow.contrib.hooks.kafka_hook import KafkaConsumerHook
from airflow.models import Connection
from airflow.utils.db import resetdb, provide_session
from mock import patch


class KafkaConsumerHookTest(unittest.TestCase):
    def setUp(self):
        configuration.test_mode()
        resetdb()

        self.conn_id = 'test-kafka-conn-id'
        self.broker_host = 'localhost'
        self.broker_port = '6666'

        self._prepare_connection()

    @patch('airflow.contrib.hooks.kafka_hook.KafkaConsumer')
    def test_initialize_consumer(self, ConsumerMock):
        hook = KafkaConsumerHook(self.conn_id, 'test-topic')
        # triggers the consumer initialization
        hook.get_conn()

        ConsumerMock.assert_called_with(
            'test-topic',
            bootstrap_servers='localhost:6666', enable_auto_commit=False)

    def test_get_messages(self):
        message = {'foo': 'baz'}

        with patch('airflow.contrib.hooks.kafka_hook.KafkaConsumer'):
            hook = KafkaConsumerHook(self.conn_id, 'test-topic')

            consumer = hook.get_conn()
            # populate sample message
            consumer.poll.return_value = {'test-partition': [message]}

            self.assertDictEqual(
                {'test-partition': [message]}, hook.get_messages())

    @provide_session
    def _prepare_connection(self, session):
        conn = Connection(
            self.conn_id,
            host=self.broker_host, port=self.broker_port)

        session.add(conn)
        session.commit()
