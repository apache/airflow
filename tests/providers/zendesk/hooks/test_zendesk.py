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
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.zendesk.hooks.zendesk import ZendeskHook
from airflow.utils import db


class TestZendeskHook:
    conn_id = 'zendesk_conn_id_test'

    @pytest.fixture(autouse=True)
    def init_connection(self):
        db.merge_conn(
            Connection(
                conn_id=self.conn_id,
                conn_type='zendesk',
                host='yoursubdomain.zendesk.com',
                login='user@gmail.com',
                password='eb243592-faa2-4ba2-a551q-1afdf565c889',
            )
        )

    def test_hook_init_and_get_conn(self):
        hook = ZendeskHook(zendesk_conn_id=self.conn_id)
        zenpy_client = hook.get_conn()
        # Verify config of zenpy APIs
        assert zenpy_client.users.subdomain == 'yoursubdomain'
        assert zenpy_client.users.domain == 'zendesk.com'
        assert zenpy_client.users.session.auth == ('user@gmail.com', 'eb243592-faa2-4ba2-a551q-1afdf565c889')
        assert not zenpy_client.cache.disabled
        assert hook._ZendeskHook__url == 'https://yoursubdomain.zendesk.com'

    @pytest.mark.parametrize(
        "call_params,expected_params",
        [
            (
                {
                    "path": 'api/v2/users/1/tickets.json',
                    "query": {'query_param1': 'value1', "query_param2": "value2"},
                },
                {
                    "url": "https://yoursubdomain.zendesk.com/api/v2/users/1/tickets" ".json",
                    "params": {'query_param1': 'value1', "query_param2": "value2"},
                },
            ),
            (
                {
                    "path": 'api/v2/users/1/tickets.json',
                    "query": {'query_param1': 'value1', "query_param2": "value2"},
                    "extra_param1": 'extra_value1',
                    "extra_param2": 'extra_value2',
                },
                {
                    "url": "https://yoursubdomain.zendesk.com/api/v2/users/1/tickets" ".json",
                    "params": {'query_param1': 'value1', "query_param2": "value2"},
                    "extra_param1": 'extra_value1',
                    "extra_param2": 'extra_value2',
                },
            ),
            (
                {
                    "path": '/api/v2/users/1/tickets.json',
                    "query": {'query_param1': 'value1', "query_param2": "value2"},
                },
                {
                    "url": "https://yoursubdomain.zendesk.com/api/v2/users/1/tickets" ".json",
                    "params": {'query_param1': 'value1', "query_param2": "value2"},
                },
            ),
            (
                {
                    "path": '/api/v2/users/1/tickets.json',
                },
                {"url": "https://yoursubdomain.zendesk.com/api/v2/users/1/tickets" ".json", "params": None},
            ),
        ],
    )
    def test_hook_custom_get_request(self, call_params, expected_params):
        hook = ZendeskHook(zendesk_conn_id=self.conn_id)
        mock_get = mock.Mock()
        hook._get = mock_get
        hook.call(**call_params)
        mock_get.assert_called_once_with(**expected_params)
