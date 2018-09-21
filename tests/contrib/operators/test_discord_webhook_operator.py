# -*- coding: utf-8 -*-
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
import unittest

from airflow import DAG, configuration

from airflow.contrib.operators.discord_webhook_operator import DiscordWebhookOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2018, 1, 1)


class TestDiscordWebhookOperator(unittest.TestCase):
    _config = {
        'http_conn_id': 'discord-webhook-default',
        'webhook_endpoint': 'webhooks/11111/some-discord-token_111',
        'message': 'your message here',
        'username': 'Airflow Webhook',
        'avatar_url': 'https://static-cdn.avatars.com/my-avatar-path',
        'tts': False,
        'proxy': 'https://proxy.proxy.com:8888'
    }

    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG('test_dag_id', default_args=args)

    def test_execute(self):
        operator = DiscordWebhookOperator(
            task_id='discord_webhook_task',
            dag=self.dag,
            **self._config
        )

        self.assertEqual(self._config['http_conn_id'], operator.http_conn_id)
        self.assertEqual(self._config['webhook_endpoint'], operator.webhook_endpoint)
        self.assertEqual(self._config['message'], operator.message)
        self.assertEqual(self._config['username'], operator.username)
        self.assertEqual(self._config['avatar_url'], operator.avatar_url)
        self.assertEqual(self._config['tts'], operator.tts)
        self.assertEqual(self._config['proxy'], operator.proxy)


if __name__ == '__main__':
    unittest.main()
