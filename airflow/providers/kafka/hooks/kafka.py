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
"""Hook for Kafka"""
import re
from typing import Any, Optional, Dict

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json

from sqlalchemy.sql.sqltypes import Boolean
from airflow.exceptions import AirflowException, AirflowConfigException
from airflow.hooks.base import BaseHook


auth_type_options = [
    'PLAIN',
    'OAUTHBEARER',
    'SCRAM-SHA-256',
    'SCRAM-SHA-512',
]


class KafkaHook(BaseHook):
    """
    Kafka interaction hook, a Wrapper around Kafka Python SDK.

    :param kafka_conn_id: reference to a pre-defined Kafka Connection
    :type kafka_conn_id: str
    """

    default_conn_name = 'kafka_default'
    conn_type = "kafka"
    conn_name_attr = "kafka_conn_id"
    hook_name = "Kafka"

    def __getattribute__(self, name: str) -> Any:
        try:
            return super(KafkaHook, self).__getattribute__(name)
        except Exception:
            return None

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port'],
            "relabeling": {
                'host': 'bootstrap servers',
                'login': 'user',
                'schema': 'topic',
            },
        }

    def __init__(self, kafka_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.kafka_conn_id = kafka_conn_id
        self.client = None
        # self.get_conn()
        self._create_consumer_config()

    def _get_connection_default_config(self):
        extra_options = {}
        if not self.kafka_conn_id:
            raise AirflowException(
                'Failed to create Kafka client. no kafka_conn_id provided')

        conn = self.get_connection(self.kafka_conn_id)
        if conn.extra is not None:
            extra_options = conn.extra_dejson

            self._topic = extra_options.get('topic', '')
            self._group_id = extra_options.get('group_id', '')
            self._servers = extra_options.get('bootstrap_servers', '')
            self._auth_type = extra_options.get('auth_type', '')
            if not self._auth_type:
                self.log.info(u'Kafka Consumer 认证方式为空')
            self._user = extra_options.get('user', '')
            self._password = extra_options.get('password', '')
            self._security_protocol = extra_options.get(
                'security_protocol', '')

    def _create_consumer_config(self) -> Dict:
        self._get_connection_default_config()
        self.log.info(u'Create Kafka Consume Server:{}, Topic: {}, Group: {}'.format(
            self.servers, self.topic, self.group_id))
        consumer_config = {
            'group_id': self.group_id,
            'bootstrap_servers': self.servers,
            'auto_offset_reset': 'earliest',  #  不会收到之前重复的数据
            # 数据只支持json数据包
            'value_deserializer': lambda data: json.loads(data),
        }
        if self._auth_type:
            self.consumer_add_auth_config(consumer_config)
        self._consumer_config = consumer_config
        return consumer_config

    def update_consumer_config(self, key: str, val: Any):
        if not self._consumer_config:
            return
        self._consumer_config.update({key, val})

    def _create_consumer(self) -> KafkaConsumer:
        consumer_config = self._consumer_config
        if not consumer_config:
            raise AirflowConfigException(u'请先设置Kafka配置信息')
        consumer: KafkaConsumer = KafkaConsumer(self.topic, **consumer_config)
        return consumer

    def ensure_consumer(self) -> Optional[KafkaConsumer]:
        try:
            if self.client:
                return self.client
            self.validate_consumer_configuration()
            self.client = self._create_consumer()
        except Exception as e:
            self.log.error(e)
            raise e

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

    def validate_consumer_configuration(self):
        if not self.topic:
            raise AirflowConfigException(u'Kafka Consumer Topic为空')
        if not self.group_id:
            raise AirflowConfigException(u'Kafka Consumer 消费组为空')
        if not self.servers:
            raise AirflowConfigException(u'Kafka Consumer 需要连接的远程服务器为空')
        return True

    @property
    def servers(self):
        return self._servers

    @property
    def topic(self):
        return self._topic

    @property
    def group_id(self):
        return self._group_id

    def unsubscribe(self):
        client = self.get_conn()
        return client.unsubscribe()

    def close(self):
        client = self.get_conn()
        return client.close()

    def bootstrap_connected(self):
        client = self.get_conn()
        return client.bootstrap_connected()

    def get_conn(self) -> KafkaConsumer:
        return self.ensure_consumer()
