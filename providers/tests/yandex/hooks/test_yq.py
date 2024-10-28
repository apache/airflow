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
from __future__ import annotations

import json
from datetime import timedelta
from unittest import mock

import pytest

yandexcloud = pytest.importorskip("yandexcloud")

import responses
from responses import matchers

from airflow.models import Connection
from airflow.providers.yandex.hooks.yq import YQHook

OAUTH_TOKEN = "my_oauth_token"
IAM_TOKEN = "my_iam_token"
SERVICE_ACCOUNT_AUTH_KEY_JSON = (
    """{"id":"my_id", "service_account_id":"my_sa1", "private_key":"my_pk"}"""
)


class TestYandexCloudYqHook:
    def _init_hook(self):
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection"
        ) as mock_get_connection:
            mock_get_connection.return_value = self.connection
            self.hook = YQHook(default_folder_id="my_folder_id")

    def setup_method(self):
        self.connection = Connection(
            extra={"service_account_json": SERVICE_ACCOUNT_AUTH_KEY_JSON}
        )

    @responses.activate()
    def test_oauth_token_usage(self):
        responses.post(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries",
            match=[
                matchers.header_matcher(
                    {
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {OAUTH_TOKEN}",
                    }
                ),
                matchers.query_param_matcher({"project": "my_folder_id"}),
            ],
            json={"id": "query1"},
            status=200,
        )

        self.connection = Connection(extra={"oauth": OAUTH_TOKEN})
        self._init_hook()
        query_id = self.hook.create_query(query_text="select 777", name="my query")
        assert query_id == "query1"

        with mock.patch("yandex_query_client.YQHttpClient.compose_query_web_link") as m:
            m.return_value = "http://gg.zz"
            assert self.hook.compose_query_web_link("query1") == "http://gg.zz"
            m.assert_called_once_with("query1")

    @responses.activate()
    @mock.patch("yandexcloud.auth.get_auth_token", return_value=IAM_TOKEN)
    def test_metadata_token_usage(self, mock_get_auth_token):
        responses.post(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries",
            match=[
                matchers.header_matcher(
                    {
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {IAM_TOKEN}",
                    }
                ),
                matchers.query_param_matcher({"project": "my_folder_id"}),
            ],
            json={"id": "query1"},
            status=200,
        )

        self.connection = Connection(extra={})
        self._init_hook()
        query_id = self.hook.create_query(query_text="select 777", name="my query")
        assert query_id == "query1"

    @mock.patch("yandexcloud._auth_fabric.__validate_service_account_key")
    @mock.patch("yandexcloud.auth.get_auth_token", return_value=IAM_TOKEN)
    def test_select_results(self, mock_get_auth_token, mock_validate):
        with mock.patch.multiple(
            "yandex_query_client.YQHttpClient",
            create_query=mock.DEFAULT,
            wait_query_to_succeed=mock.DEFAULT,
            get_query_all_result_sets=mock.DEFAULT,
            get_query_status=mock.DEFAULT,
            get_query=mock.DEFAULT,
            stop_query=mock.DEFAULT,
        ) as mocks:
            self._init_hook()
            mock_validate.assert_called()
            mock_get_auth_token.assert_called_once_with(
                service_account_key=json.loads(SERVICE_ACCOUNT_AUTH_KEY_JSON)
            )

            mocks["create_query"].return_value = "query1"
            mocks["wait_query_to_succeed"].return_value = 2
            mocks["get_query_all_result_sets"].return_value = {"x": 765}
            mocks["get_query_status"].return_value = "COMPLETED"
            mocks["get_query"].return_value = {"id": "my_q"}

            query_id = self.hook.create_query(query_text="select 777", name="my query")
            assert query_id == "query1"
            mocks["create_query"].assert_called_once_with(
                query_text="select 777", name="my query"
            )

            results = self.hook.wait_results(
                query_id, execution_timeout=timedelta(minutes=10)
            )
            assert results == {"x": 765}
            mocks["wait_query_to_succeed"].assert_called_once_with(
                query_id, execution_timeout=timedelta(minutes=10), stop_on_timeout=True
            )
            mocks["get_query_all_result_sets"].assert_called_once_with(
                query_id=query_id, result_set_count=2
            )

            assert self.hook.get_query_status(query_id) == "COMPLETED"
            mocks["get_query_status"].assert_called_once_with(query_id)

            assert self.hook.get_query(query_id) == {"id": "my_q"}
            mocks["get_query"].assert_called_once_with(query_id)

            self.hook.stop_query(query_id)
            mocks["stop_query"].assert_called_once_with(query_id)
