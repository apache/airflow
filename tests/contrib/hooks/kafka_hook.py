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

    @patch('kafka.KafkaConsumer')
    def test_get_message(self):
        hook = KafkaConsumerHook(self.conn_id)

        # populate sample message
        message = {'foo': 'baz'}
        consumer = hook.get_conn()
        self.assertDictEqual(
            message, consumer.get_message())


    @provide_session
    def _prepare_connection(self, session):
        conn = Connection(
            self.conn_id,
            host=self.broker_host, port=self.broker_port)

        session.add(conn)
        session.commit()
