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

from enum import Enum

from kafka import BrokerConnection, KafkaAdminClient, KafkaClient, KafkaConsumer, KafkaProducer

from airflow.hooks.base import BaseHook


class KafkaHookClient:
    """Simple wrapper of Kafka classes"""

    def __init__(self, **kwargs):
        """
        Take and save configs that common for Kafka classes

        'bootstrap_servers'
        'client_id'
        """
        self.configs = kwargs

    def create_broker_connection(self, **kwargs) -> BrokerConnection:
        """Returns BrokerConnection instance"""
        broker_connection_conf = dict(self.configs, **kwargs)
        return BrokerConnection(**broker_connection_conf)

    def create_internal_client(self, **kwargs) -> KafkaClient:
        """Returns KafkaClient instance"""
        internal_client_conf = dict(self.configs, **kwargs)
        return KafkaClient(**internal_client_conf)

    def create_admin_client(self, **kwargs) -> KafkaAdminClient:
        """Returns KafkaAdminClient instance"""
        admin_client_conf = dict(self.configs, **kwargs)
        return KafkaAdminClient(**admin_client_conf)

    def create_producer(self, **kwargs) -> KafkaProducer:
        """
        Returns KafkaProducer instance

        Valid parameters:
            key_serializer: None
            value_serializer: None
            acks: 1
            bootstrap_topics_filter: set()
            compression_type: None
            retries: 0
            batch_size: 16384
            linger_ms: 0
            buffer_memory: 33554432
            max_block_ms: 60000
            max_request_size: 1048576
            partitioner: DefaultPartitioner()
        """
        producer_conf = dict(self.configs, **kwargs)
        return KafkaProducer(**producer_conf)

    def create_consumer(self, **kwargs) -> KafkaConsumer:
        """
        Returns KafkaConsumer instance.

        Valid arguments:
            group_id: None
            key_deserializer: None
            value_deserializer: None
            fetch_max_wait_ms: 500
            fetch_min_bytes: 1
            fetch_max_bytes: 52428800
            max_partition_fetch_bytes: 1 * 1024 * 1024
            max_poll_records: 500
            max_poll_interval_ms: 300000
            auto_offset_reset: 'latest'
            enable_auto_commit: True
            auto_commit_interval_ms: 5000
            default_offset_commit_callback: lambda offsets, response: True
            check_crcs: True
            session_timeout_ms: 10000
            heartbeat_interval_ms: 3000
            consumer_timeout_ms: float('inf')
            legacy_iterator: False # enable to revert to < 1.4.7 iterator
            metric_group_prefix: 'consumer'
            exclude_internal_topics: True
            partition_assignment_strategy: (RangePartitionAssignor, RoundRobinPartitionAssignor)
        """
        consumer_conf = dict(self.configs, **kwargs)
        return KafkaConsumer(configs=consumer_conf)


class SecurityProtocol(Enum):
    PLAINTEXT: str = 'PLAINTEXT'
    SASL_PLAINTEXT: str = 'SASL_PLAINTEXT'
    SASL_SSL: str = 'SASL_SSL'
    SSL: str = 'SSL'


class SaslMechanism(Enum):
    PLAIN: str = 'PLAIN'
    GSSAPI: str = 'GSSAPI'
    OAUTHBEARER: str = 'OAUTHBEARER'
    SCRAM_SHA_256: str = 'SCRAM-SHA-256'
    SCRAM_SHA_512: str = 'SCRAM-SHA-512'


class KafkaHook(BaseHook):
    """
    Interact with Apache Kafka cluster using `python-kafka`.
    Hook attribute `conn` returns the client which contains library classes

    .. seealso::
        - https://github.com/dpkp/kafka-python/

    .. seealso::
        :class:`~airflow.providers.apache.kafka.hooks.kafka.KafkaHook`

    :param kafka_conn_id: The connection id to the Kafka cluster
    """

    conn_name_attr = 'kafka_conn_id'
    default_conn_name = 'kafka_default'
    conn_type = 'kafka'
    hook_name = 'Apache Kafka'

    def __init__(self, kafka_conn_id: str = 'kafka_default') -> None:

        super().__init__()
        self.kafka_conn_id = kafka_conn_id

        configs = self._get_configs()

        self.client = KafkaHookClient(
            bootstrap_servers=self.get_conn_url(),
            client_id='apache-airflow-kafka-hook',
            **configs,
        )

    def get_conn(self) -> KafkaHookClient:
        """Returns a connection object"""
        return self.client

    def get_conn_url(self) -> str:
        """Get Kafka connection url"""
        conn = self.get_connection(self.kafka_conn_id)

        host = 'localhost' if not conn.host else conn.host
        port = 9092 if not conn.port else conn.port

        servers = map(lambda h: h if ':' in h else f'{h}:{port}', host.split(','))

        return ','.join(servers)

    def _get_configs(self) -> dict:
        """Generates configs for Kafka classes"""
        conn = self.get_connection(self.kafka_conn_id)
        configs = conn.extra_dejson.copy()

        configs['security_protocol'] = SecurityProtocol(conn.schema.upper()).value

        if configs['security_protocol'] in ('SASL_PLAINTEXT', 'SASL_SSL'):
            mechanism = configs.get('sasl_mechanism', 'PLAIN').upper()
            configs['sasl_mechanism'] = SaslMechanism(mechanism)
            if configs['sasl_mechanism'] == 'OAUTHBEARER':
                token_provider = object()
                setattr(token_provider, 'token', lambda _: conn.password)
                configs['sasl_oauth_token_provider'] = token_provider
            elif configs['sasl_mechanism'] in ('PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'):
                configs['sasl_plain_username'] = conn.login
                configs['sasl_plain_password'] = conn.password
        elif configs['security_protocol'] == 'SSL':
            configs['ssl_keyfile'] = conn.login
            configs['ssl_password'] = conn.password

        return configs


__all__ = ['KafkaHook']
