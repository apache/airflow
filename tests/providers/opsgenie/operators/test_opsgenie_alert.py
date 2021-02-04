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
from airflow.providers.opsgenie.operators.opsgenie_alert import OpsgenieAlertOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestOpsgenieAlertOperator(unittest.TestCase):
    _config = {
        'message': 'An example alert message',
        'alias': 'Life is too short for no alias',
        'description': 'Every alert needs a description',
        'responders': [
            {'id': '4513b7ea-3b91-438f-b7e4-e3e54af9147c', 'type': 'team'},
            {'name': 'NOC', 'type': 'team'},
            {'id': 'bb4d9938-c3c2-455d-aaab-727aa701c0d8', 'type': 'user'},
            {'username': 'trinity@opsgenie.com', 'type': 'user'},
            {'id': 'aee8a0de-c80f-4515-a232-501c0bc9d715', 'type': 'escalation'},
            {'name': 'Nightwatch Escalation', 'type': 'escalation'},
            {'id': '80564037-1984-4f38-b98e-8a1f662df552', 'type': 'schedule'},
            {'name': 'First Responders Schedule', 'type': 'schedule'},
        ],
        'visible_to': [
            {'id': '4513b7ea-3b91-438f-b7e4-e3e54af9147c', 'type': 'team'},
            {'name': 'rocket_team', 'type': 'team'},
            {'id': 'bb4d9938-c3c2-455d-aaab-727aa701c0d8', 'type': 'user'},
            {'username': 'trinity@opsgenie.com', 'type': 'user'},
        ],
        'actions': ['Restart', 'AnExampleAction'],
        'tags': ['OverwriteQuietHours', 'Critical'],
        'details': {'key1': 'value1', 'key2': 'value2'},
        'entity': 'An example entity',
        'source': 'Airflow',
        'priority': 'P1',
        'user': 'Jesse',
        'note': 'Write this down',
    }

    expected_payload_dict = {
        'message': _config['message'],
        'alias': _config['alias'],
        'description': _config['description'],
        'responders': _config['responders'],
        'visible_to': _config['visible_to'],
        'actions': _config['actions'],
        'tags': _config['tags'],
        'details': _config['details'],
        'entity': _config['entity'],
        'source': _config['source'],
        'priority': _config['priority'],
        'user': _config['user'],
        'note': _config['note'],
    }

    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG('test_dag_id', default_args=args)

    def test_build_opsgenie_payload(self):
        # Given / When
        operator = OpsgenieAlertOperator(task_id='opsgenie_alert_job', dag=self.dag, **self._config)

        payload = operator._build_opsgenie_payload()

        # Then
        assert self.expected_payload_dict == payload

    def test_properties(self):
        # Given / When
        operator = OpsgenieAlertOperator(task_id='opsgenie_alert_job', dag=self.dag, **self._config)

        assert 'opsgenie_default' == operator.opsgenie_conn_id
        assert self._config['message'] == operator.message
        assert self._config['alias'] == operator.alias
        assert self._config['description'] == operator.description
        assert self._config['responders'] == operator.responders
        assert self._config['visible_to'] == operator.visible_to
        assert self._config['actions'] == operator.actions
        assert self._config['tags'] == operator.tags
        assert self._config['details'] == operator.details
        assert self._config['entity'] == operator.entity
        assert self._config['source'] == operator.source
        assert self._config['priority'] == operator.priority
        assert self._config['user'] == operator.user
        assert self._config['note'] == operator.note
