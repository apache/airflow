from time import time
from airflow.exceptions import AirflowConfigException
from airflow.models.connection import Connection
from airflow.providers.kafka.hooks.kafka import KafkaHook
from sqlalchemy.sql.expression import false
from plugins.entities.entity import ClsEntity
from typing import Optional, Dict
import threading
import os
import json
import time
import pprint
from plugins.utils.logger import generate_logger

_logger = generate_logger(__name__)

FACTORY_CODE = os.getenv('FACTORY_CODE', 'DEFAULT_FACTORY_CODE')


class ClsKafkaConsumer(ClsEntity):
    _instance_lock = threading.Lock()
    _kafka: Optional[KafkaHook] = None

    def __new__(cls, *args, **kwargs):
        if not hasattr(ClsKafkaConsumer, "_instance"):
            with ClsKafkaConsumer._instance_lock:
                if not hasattr(ClsKafkaConsumer, "_instance"):
                    ClsKafkaConsumer._instance = object.__new__(cls)
        return ClsKafkaConsumer._instance

    def __init__(self, **kwargs):
        super(ClsKafkaConsumer, self).__init__(**kwargs)
        self.kafka_conn_id = 'qcos_kafka'
        self._kafka = KafkaHook(self.kafka_conn_id)
        self._handler = kwargs.get('handler', None)

    def update_client_id(self):
        if self._kafka:
            self._kafka.update_consumer_config('client_id', 'qcos_kafka_consumer_{}'.format(FACTORY_CODE))

    def ensure_connected(self):
        if self._kafka:
            end = False
            while not end:
                end = self._kafka.bootstrap_connected()
                time.sleep(1)

    def register_handler(self, handler):
        self._handler = handler

    def read(self):
        self._kafka.ensure_consumer()
        self.ensure_connected()
        try:
            _logger.info('Kafka Reading...')
            for msg in self._kafka.client:
                # msg: headers, value
                if self._handler is not None:
                    self._handler(msg)
                _logger.debug('Kafka New Message: {}'.format(pprint.pformat(msg, indent=4)))
        except Exception as e:
            _logger.error('Read Error', e)
            self._kafka.unsubscribe()
            self._kafka.close()
