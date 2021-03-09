from time import time
from airflow.exceptions import AirflowConfigException
from kafka import KafkaConsumer
from sqlalchemy.sql.expression import false, update
from .entity import ClsEntity
from typing import Optional, Dict
import threading
import os
import json
import time
import pprint
from airflow.utils.logger import generate_logger

_logger = generate_logger(__name__)

FACTORY_CODE = os.getenv('FACTORY_CODE', 'DEFAULT_FACTORY_CODE')


auth_type_options = [
    'PLAIN',
    'OAUTHBEARER',
    'SCRAM-SHA-256',
    'SCRAM-SHA-512',
]


class ClsKafkaConsumer(ClsEntity):
    _instance_lock = threading.Lock()
    _consumer: Optional[KafkaConsumer] = None

    def __new__(cls, *args, **kwargs):
        if not hasattr(ClsKafkaConsumer, "_instance"):
            with ClsKafkaConsumer._instance_lock:
                if not hasattr(ClsKafkaConsumer, "_instance"):
                    ClsKafkaConsumer._instance = object.__new__(
                        cls, *args, **kwargs)
        return ClsKafkaConsumer._instance

    def __init__(self, **kwargs):
        super(ClsKafkaConsumer, self).__init__(**kwargs)
        self._topic = kwargs.get('topic', '')
        self._group_id = kwargs.get('group_id', '')
        self._servers = kwargs.get('bootstrap_servers', '')
        self._auth_type = kwargs.get('auth_type', '')
        self._user = kwargs.get('user', '')
        self._password = kwargs.get('password', '')
        self._security_protocol = kwargs.get('security_protocol', '')
        if not self._auth_type:
            _logger.info(u'Kafka Consumer 认证方式为空')

    def validate_consumer_configuration(self) -> bool:
        ret = false
        if not self.topic:
            raise AirflowConfigException(u'Kafka Consumer Topic为空')
        if not self.group_id:
            raise AirflowConfigException(u'Kafka Consumer 消费组为空')
        if not self.servers:
            raise AirflowConfigException(u'Kafka Consumer需要连接的远程服务器为空')
        return ret

    @property
    def consumer_entity(self):
        return '{}@{}'.format(self.group_id, self.topic)  # group_id@topic

    @property
    def servers(self):
        return self._servers

    @property
    def topic(self):
        return self._topic

    def consumer_add_auth_config(self, config) -> Dict:
        if not config:
            return config
        if self._auth_type == auth_type_options[0]:
            # PLAIN
            if self._security_protocol not in ['SASL_PLAINTEXT', 'SASL_SSL']:
                raise AirflowConfigException(u'认证PLAINT类型, protocol配置错误')
            config.update({
                'security_protocol': self._security_protocol,
                'sasl_mechanism': auth_type_options[0],
                'sasl_plain_username': self._user,
                'sasl_plain_password': self._password,
            })
        if self._auth_type in [auth_type_options[2], auth_type_options[3]]:
            # SCRAM-SHA-256 or SCRAM-SHA-512
            if self._security_protocol not in ['SASL_PLAINTEXT', 'SASL_SSL']:
                raise AirflowConfigException(
                    u'认证{}类型, protocol配置错误'.format(self._auth_type))
            config.update({
                'security_protocol': self._security_protocol,
                'sasl_mechanism': self._auth_type,
                'sasl_plain_username': self._user,
                'sasl_plain_password': self._password,
            })
        return config

    @property
    def group_id(self):
        return self._group_id

    def ensure_connected(self):
        if self._consumer:
            end = False
            while not end:
                end = self._consumer.bootstrap_connected()
                time.sleep(1)

    def read(self):
        if not self._consumer:
            self.ensure_consumer()
        self.ensure_connected()
        try:
            for msg in self._consumer:
                # msg: headers, value
                _logger.debug('Kafka New Message: {}'.format(pprint.pformat(msg, indent=4)))
        except Exception as e:
            _logger.error('Read Error', e)
            self._consumer.unsubscribe()
            self._consumer.close()

    def _create_consumer(self) -> KafkaConsumer:
        _logger.info(u'Create Kafka Consume Server:{}, Topic: {}, Group: {}'.format(
            self.servers, self.topic, self.group_id))
        consumer_config = {
            'group_id': self.group_id,
            'bootstrap_servers': self.servers,
            'client_id': 'qcos_kafka_consumer_{}'.format(FACTORY_CODE),
            'value_deserializer': lambda data: json.loads(data), # 数据只支持json数据包
        }
        if self._auth_type:
            self.consumer_add_auth_config(consumer_config)
        consumer: KafkaConsumer = KafkaConsumer(self.topic, **consumer_config)
        return consumer

    def ensure_consumer(self) -> Optional[KafkaConsumer]:
        try:
            if self._consumer:
                return self._consumer
            self.validate_consumer_configuration()
            self._consumer = self._create_consumer()
        except Exception as e:
            _logger.error(e)
            raise e
