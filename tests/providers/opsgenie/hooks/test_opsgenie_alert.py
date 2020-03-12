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
import json
import unittest

import requests_mock

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.opsgenie.hooks.opsgenie_alert import OpsgenieAlertHook
from airflow.utils import db


class TestOpsgenieAlertHook(unittest.TestCase):
    conn_id = 'opsgenie_conn_id_test'
    opsgenie_alert_endpoint = 'https://api.opsgenie.com/v2/alerts'
    _payload = {
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
            {'name': 'First Responders Schedule', 'type': 'schedule'}
        ],
        'visibleTo': [
            {'id': '4513b7ea-3b91-438f-b7e4-e3e54af9147c', 'type': 'team'},
            {'name': 'rocket_team', 'type': 'team'},
            {'id': 'bb4d9938-c3c2-455d-aaab-727aa701c0d8', 'type': 'user'},
            {'username': 'trinity@opsgenie.com', 'type': 'user'}
        ],
        'actions': ['Restart', 'AnExampleAction'],
        'tags': ['OverwriteQuietHours', 'Critical'],
        'details': {'key1': 'value1', 'key2': 'value2'},
        'entity': 'An example entity',
        'source': 'Airflow',
        'priority': 'P1',
        'user': 'Jesse',
        'note': 'Write this down'
    }
    _mock_success_response_body = {
        "result": "Request will be processed",
        "took": 0.302,
        "requestId": "43a29c5c-3dbf-4fa4-9c26-f4f71023e120"
    }

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id=self.conn_id,
                host='https://api.opsgenie.com/',
                password='eb243592-faa2-4ba2-a551q-1afdf565c889'
            )
        )

    def test_get_api_key(self):
        hook = OpsgenieAlertHook(opsgenie_conn_id=self.conn_id)
        api_key = hook._get_api_key()
        self.assertEqual('eb243592-faa2-4ba2-a551q-1afdf565c889', api_key)

    def test_get_conn_defaults_host(self):
        hook = OpsgenieAlertHook()
        hook.get_conn()
        self.assertEqual('https://api.opsgenie.com', hook.base_url)

    @requests_mock.mock()
    def test_call_with_success(self, m):
        hook = OpsgenieAlertHook(opsgenie_conn_id=self.conn_id)
        m.post(
            self.opsgenie_alert_endpoint,
            status_code=202,
            json=self._mock_success_response_body
        )
        resp = hook.execute(payload=self._payload)
        self.assertEqual(resp.status_code, 202)
        self.assertEqual(resp.json(), self._mock_success_response_body)

    @requests_mock.mock()
    def test_api_key_set(self, m):
        hook = OpsgenieAlertHook(opsgenie_conn_id=self.conn_id)
        m.post(
            self.opsgenie_alert_endpoint,
            status_code=202,
            json=self._mock_success_response_body
        )
        resp = hook.execute(payload=self._payload)
        self.assertEqual(resp.request.headers.get('Authorization'),
                         'GenieKey eb243592-faa2-4ba2-a551q-1afdf565c889')

    @requests_mock.mock()
    def test_api_key_not_set(self, m):
        hook = OpsgenieAlertHook()
        m.post(
            self.opsgenie_alert_endpoint,
            status_code=202,
            json=self._mock_success_response_body
        )
        with self.assertRaises(AirflowException):
            hook.execute(payload=self._payload)

    @requests_mock.mock()
    def test_payload_set(self, m):
        hook = OpsgenieAlertHook(opsgenie_conn_id=self.conn_id)
        m.post(
            self.opsgenie_alert_endpoint,
            status_code=202,
            json=self._mock_success_response_body
        )
        resp = hook.execute(payload=self._payload)
        self.assertEqual(json.loads(resp.request.body), self._payload)


if __name__ == '__main__':
    unittest.main()
