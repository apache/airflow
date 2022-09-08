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

from airflow.models.dag import DAG
from airflow.providers.microsoft.teams.operators.teams_webhook import TeamsWebhookOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestTeamsWebhookOperator:
    _config = {
        'http_conn_id': 'teams-webhook-default',
        'webhook_token': 'manual_token',
        'message': 'your message here',
        'subtitle': 'your subtitle here',
        'facts': [
            {'name': 'Assigned to', 'value': 'Airflow team'},
            {'name': 'Due date', 'value': 'Mon May 01 2017 17:07:18 GMT-0700 (Pacific Daylight Time)'},
            {'name': 'status', 'value': 'Completed'},
        ],
        'action_button_name': 'Learn More',
        'action_button_url': 'https://airflow.apache.org',
        'theme_color': 'FF0000',
        'icon_url': 'https://airflow.apache.org/_images/pin_large.png',
        'proxy': 'https://my-horrible-proxy.proxyist.com:8080',
    }

    def test_execute(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG('test_dag_id', default_args=args)
        operator = TeamsWebhookOperator(task_id='teams_webhook_job', dag=dag, **self._config)

        assert self._config['http_conn_id'] == operator.http_conn_id
        assert self._config['webhook_token'] == operator.webhook_token
        assert self._config['message'] == operator.message
        assert self._config['subtitle'] == operator.subtitle
        assert self._config['facts'] == operator.facts
        assert self._config['action_button_name'] == operator.action_button_name
        assert self._config['action_button_url'] == operator.action_button_url
        assert self._config['theme_color'] == operator.theme_color
        assert self._config['icon_url'] == operator.icon_url
        assert self._config['proxy'] == operator.proxy
