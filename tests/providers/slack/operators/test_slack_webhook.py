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

from airflow.models.dag import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestSlackWebhookOperator(unittest.TestCase):
    _config = {
        'http_conn_id': 'slack-webhook-default',
        'webhook_token': 'manual_token',
        'message': 'your message here',
        'attachments': [{'fallback': 'Required plain-text summary'}],
        'blocks': [{'type': 'section', 'text': {'type': 'mrkdwn', 'text': '*bold text*'}}],
        'channel': '#general',
        'username': 'SlackMcSlackFace',
        'icon_emoji': ':hankey',
        'icon_url': 'https://airflow.apache.org/_images/pin_large.png',
        'link_names': True,
        'proxy': 'https://my-horrible-proxy.proxyist.com:8080',
    }

    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG('test_dag_id', default_args=args)

    def test_execute(self):
        # Given / When
        operator = SlackWebhookOperator(task_id='slack_webhook_job', dag=self.dag, **self._config)

        assert self._config['http_conn_id'] == operator.http_conn_id
        assert self._config['webhook_token'] == operator.webhook_token
        assert self._config['message'] == operator.message
        assert self._config['attachments'] == operator.attachments
        assert self._config['blocks'] == operator.blocks
        assert self._config['channel'] == operator.channel
        assert self._config['username'] == operator.username
        assert self._config['icon_emoji'] == operator.icon_emoji
        assert self._config['icon_url'] == operator.icon_url
        assert self._config['link_names'] == operator.link_names
        assert self._config['proxy'] == operator.proxy

    def test_assert_templated_fields(self):
        operator = SlackWebhookOperator(task_id='slack_webhook_job', dag=self.dag, **self._config)

        template_fields = [
            'webhook_token',
            'message',
            'attachments',
            'blocks',
            'channel',
            'username',
            'proxy',
        ]

        assert operator.template_fields == template_fields
