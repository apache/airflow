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


import unittest
from unittest import mock

from airflow.models import Connection
from airflow.providers.pagerduty.hooks.pagerduty import PagerdutyHook
from airflow.utils.session import provide_session

DEFAULT_CONN_ID = "pagerduty_default"


class TestPagerdutyHook(unittest.TestCase):
    @provide_session
    def setUp(self, session=None):
        session.add(Connection(
            conn_id=DEFAULT_CONN_ID,
            password="pagerduty_token",
            extra='{"routing_key": "route"}',
        ))
        session.commit()

    @provide_session
    def test_without_routing_key_extra(self, session):
        session.add(Connection(
            conn_id="pagerduty_no_extra",
            password="pagerduty_token_without_extra",
        ))
        session.commit()
        hook = PagerdutyHook(pagerduty_conn_id="pagerduty_no_extra")
        self.assertEqual(hook.token, 'pagerduty_token_without_extra', 'token initialised.')
        self.assertEqual(hook.routing_key, None, 'default routing key skipped.')

    def test_get_token_from_password(self):
        hook = PagerdutyHook(pagerduty_conn_id=DEFAULT_CONN_ID)
        self.assertEqual(hook.token, 'pagerduty_token', 'token initialised.')

    def test_token_parameter_override(self):
        hook = PagerdutyHook(token="pagerduty_param_token", pagerduty_conn_id=DEFAULT_CONN_ID)
        self.assertEqual(hook.token, 'pagerduty_param_token', 'token initialised.')

    @mock.patch('airflow.providers.pagerduty.hooks.pagerduty.pypd.EventV2.create')
    def test_create_event(self, mock_event_create):
        hook = PagerdutyHook(pagerduty_conn_id=DEFAULT_CONN_ID)
        mock_event_create.return_value = {
            "status": "success",
            "message": "Event processed",
            "dedup_key": "samplekeyhere",
        }
        resp = hook.create_event(
            routing_key="key",
            summary="test",
            source="airflow_test",
            severity="error",
        )
        self.assertEqual(resp["status"], "success")
        mock_event_create.assert_called_once_with(
            api_key="pagerduty_token",
            data={
                "routing_key": "key",
                "event_action": "trigger",
                "payload": {
                    "severity": "error",
                    "source": "airflow_test",
                    "summary": "test",
                },
            })

    @mock.patch('airflow.providers.pagerduty.hooks.pagerduty.pypd.EventV2.create')
    def test_create_event_with_default_routing_key(self, mock_event_create):
        hook = PagerdutyHook(pagerduty_conn_id=DEFAULT_CONN_ID)
        mock_event_create.return_value = {
            "status": "success",
            "message": "Event processed",
            "dedup_key": "samplekeyhere",
        }
        resp = hook.create_event(
            summary="test",
            source="airflow_test",
            severity="error",
            custom_details='{"foo": "bar"}',
        )
        self.assertEqual(resp["status"], "success")
        mock_event_create.assert_called_once_with(
            api_key="pagerduty_token",
            data={
                "routing_key": "route",
                "event_action": "trigger",
                "payload": {
                    "severity": "error",
                    "source": "airflow_test",
                    "summary": "test",
                    "custom_details": '{"foo": "bar"}',
                },
            })
