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
#

from airflow.providers.apache.kafka.hooks.kafka import KafkaHook, KafkaHookClient
from tests.test_utils.config import env_vars


class TestKafkaHook:
    def test_client_attribute(self):
        conn_uri = 'kafka://1.1.1.1:9092/PLAINTEXT'
        with env_vars({'AIRFLOW_CONN_KAFKA_DEFAULT': conn_uri}):
            hook = KafkaHook('kafka_default')
            assert hasattr(hook, 'client')
            assert isinstance(hook.client, KafkaHookClient)

    def test_get_conn_url(self):
        conn_uri = 'kafka://:XXXXX@1.1.1.1:9092/PLAINTEXT'
        with env_vars({'AIRFLOW_CONN_KAFKA_DEFAULT': conn_uri}):
            hook = KafkaHook('kafka_default')
            assert hook.get_conn_url() == '1.1.1.1:9092'
