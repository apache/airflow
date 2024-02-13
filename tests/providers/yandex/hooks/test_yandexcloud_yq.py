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
import responses
from decimal import Decimal
from responses import matchers
from unittest import mock

from airflow.models import Connection
from airflow.providers.yandex.hooks.yandexcloud_yq import YQHook

OAUTH_TOKEN = "my_oauth_token"
SERVICE_ACCOUNT_AUTH_KEY_JSON = """{"id":"my_id", "service_account_id":"my_sa1", "private_key":"-----BEGIN PRIVATE KEY----- my_pk"}"""

class TestYandexCloudYqHook:
    def _init_hook(self):
        with mock.patch("airflow.hooks.base.BaseHook.get_connection") as mock_get_connection:
            mock_get_connection.return_value = self.connection
            self.hook = YQHook(default_folder_id="my_folder_id")

    def setup_method(self):
        #self.connection = Connection(extra=json.dumps({"oauth": OAUTH_TOKEN}))
        self.connection = Connection(extra=json.dumps({"service_account_json": SERVICE_ACCOUNT_AUTH_KEY_JSON}))

    @responses.activate()
    @mock.patch("jwt.encode")
    def test_simple_select_via_iam(self, mock_jwt):
        responses.post(
            "https://iam.api.cloud.yandex.net/iam/v1/tokens",
            json={"iamToken": "super_token"},
            status=200,
        )
        mock_jwt.return_value = "zzzz"
        
        responses.post(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries",
            match=[
                matchers.header_matcher({"Content-Type": "application/json", "Authorization": "Bearer super_token"}),
                matchers.query_param_matcher({"project": "my_folder_id"})
            ],
            json={"id": "query1"},
            status=200,
        )

        responses.get(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1/status",
            json={"status": "COMPLETED"},
            status=200,
        )

        responses.get(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1",
            json={"id": "query1", "result_sets": [{"rows_count": 1, "truncated": False}]},
            status=200,
        )

        responses.get(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1/results/0",
            json={"rows": [[777]], "columns": [{"name": "column0", "type": "Int32"}]},
            status=200,
        )

        self._init_hook()
        query_id = self.hook.create_query(query_text="select 777", name="my query", description="my desc")
        assert query_id == "query1"

        results = self.hook.wait_results(query_id)
        assert results == {"rows": [[777]], "columns": [
            {"name": "column0", "type": "Int32"}]}
        
    @responses.activate()
    @mock.patch("jwt.encode")
    def test_integral_results(self, mock_jwt):
        responses.post(
            "https://iam.api.cloud.yandex.net/iam/v1/tokens",
            json={"iamToken": "super_token"},
            status=200,
        )
        mock_jwt.return_value = "zzzz"
        
        responses.post(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries",
            match=[
                matchers.header_matcher({"Content-Type": "application/json", "Authorization": "Bearer super_token"}),
                matchers.query_param_matcher({"project": "my_folder_id"})
            ],
            json={"id": "query1"},
            status=200,
        )

        responses.get(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1/status",
            json={"status": "COMPLETED"},
            status=200,
        )

        responses.get(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1",
            json={"id": "query1", "result_sets": [{"rows_count": 1, "truncated": False}]},
            status=200,
        )

        responses.get(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1/results/0",
            json={
                "rows":[[100,-100,200,200,10000000000,-20000000000,"18014398509481984","-18014398509481984",123.5,-789.125,"inf",True,False,"aGVsbG8=","hello","1.23","he\"llo_again","Я Привет",1,2,3,4]],
                "columns":[{"name":"column0","type":"Int32"},{"name":"column1","type":"Int32"},{"name":"column2","type":"Int64"},{"name":"column3","type":"Uint64"},{"name":"column4","type":"Uint64"},{"name":"column5","type":"Int64"},{"name":"column6","type":"Int64"},{"name":"column7","type":"Int64"},{"name":"column8","type":"Float"},{"name":"column9","type":"Double"},{"name":"column10","type":"Double"},{"name":"column11","type":"Bool"},{"name":"column12","type":"Bool"},{"name":"column13","type":"String"},{"name":"column14","type":"Utf8"},{"name":"column15","type":"Decimal(6,3)"},{"name":"column16","type":"Utf8"},{"name":"column17","type":"Utf8"},{"name":"column18","type":"Int8"},{"name":"column19","type":"Int16"},{"name":"column20","type":"Uint8"},{"name":"column21","type":"Uint16"}]
            },
            status=200,
        )

        self._init_hook()
        query_id = self.hook.create_query(query_text="complex_query1", name="my query", description="my desc")
        assert query_id == "query1"

        results = self.hook.wait_results(query_id)
        assert results == {
            "rows": [
                [
                    100, -100,
                    200, 200,
                    10000000000, -20000000000,
                    "18014398509481984", "-18014398509481984",
                    123.5, -789.125,
                    float("inf"), True,
                    False, "hello",
                    "hello", Decimal("1.23"),
                    "he\"llo_again", "Я Привет",
                    1, 2, 3, 4
                ]
            ],
            "columns":[{"name":"column0","type":"Int32"},{"name":"column1","type":"Int32"},{"name":"column2","type":"Int64"},{"name":"column3","type":"Uint64"},{"name":"column4","type":"Uint64"},{"name":"column5","type":"Int64"},{"name":"column6","type":"Int64"},{"name":"column7","type":"Int64"},{"name":"column8","type":"Float"},{"name":"column9","type":"Double"},{"name":"column10","type":"Double"},{"name":"column11","type":"Bool"},{"name":"column12","type":"Bool"},{"name":"column13","type":"String"},{"name":"column14","type":"Utf8"},{"name":"column15","type":"Decimal(6,3)"},{"name":"column16","type":"Utf8"},{"name":"column17","type":"Utf8"},{"name":"column18","type":"Int8"},{"name":"column19","type":"Int16"},{"name":"column20","type":"Uint8"},{"name":"column21","type":"Uint16"}]
        }