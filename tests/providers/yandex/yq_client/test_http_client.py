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

import responses
from responses import matchers

from airflow.providers.yandex.yq_client.http_client import YQHttpClient, YQHttpClientConfig

IAM_TOKEN = "my_iam_token"
PROJECT = "my_project"


class TestYQHttpClient:
    def setup_method(self):
        config = YQHttpClientConfig(IAM_TOKEN, PROJECT)
        self.client = YQHttpClient(config)

    def setup_mocks_for_query_execution(self, query_results_json_list):
        responses.post(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries",
            match=[
                matchers.header_matcher(
                    {"Content-Type": "application/json", "Authorization": f"Bearer {IAM_TOKEN}"}
                ),
                matchers.query_param_matcher({"project": PROJECT}),
                matchers.json_params_matcher({"name": "my query", "text": "select 777"}),
            ],
            json={"id": "query1"},
            status=200,
        )

        responses.get(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1/status",
            json={"status": "RUNNING"},
            status=200,
        )

        responses.get(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1/status",
            json={"status": "COMPLETED"},
            status=200,
        )

        result_set_count = len(query_results_json_list)
        responses.get(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1",
            json={
                "id": "query1",
                "result_sets": [{"rows_count": 1, "truncated": False} for _ in range(result_set_count)],
            },
            status=200,
        )

        for i in range(result_set_count):
            responses.get(
                f"https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1/results/{i}",
                json=query_results_json_list[i],
                status=200,
            )

    def _create_test_query(self):
        query_id = self.client.create_query(query_text="select 777", name="my query")
        assert query_id == "query1"
        return query_id

    @responses.activate()
    def test_select_results(self):
        self.setup_mocks_for_query_execution(
            [{"rows": [[777]], "columns": [{"name": "column0", "type": "Int32"}]}]
        )

        query_id = self._create_test_query()

        result_set_count = self.client.wait_query_to_succeed(query_id)
        assert result_set_count == 1
        results = self.client.get_query_all_result_sets(query_id, result_set_count=result_set_count)
        assert results == {"rows": [[777]], "columns": [{"name": "column0", "type": "Int32"}]}

        responses.post(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1/stop",
            match=[
                matchers.header_matcher({"Authorization": f"Bearer {IAM_TOKEN}"}),
                matchers.query_param_matcher({"project": PROJECT}),
            ],
            status=204,
        )

        assert self.client.get_query_status(query_id) == "COMPLETED"
        assert self.client.get_query(query_id) == {
            "id": "query1",
            "result_sets": [{"rows_count": 1, "truncated": False}],
        }
        self.client.stop_query(query_id)

        assert (
            self.client.compose_query_web_link(query_id)
            == f"https://yq.cloud.yandex.ru/folders/{PROJECT}/ide/queries/query1"
        )

    @responses.activate()
    def test_select_two_record_sets(self):
        self.setup_mocks_for_query_execution(
            [
                {"rows": [[777]], "columns": [{"name": "column0", "type": "Int32"}]},
                {"rows": [["zzz"]], "columns": [{"name": "aaaa", "type": "Utf8"}]},
            ]
        )

        query_id = self._create_test_query()

        result_set_count = self.client.wait_query_to_succeed(query_id)
        assert result_set_count == 2
        results = self.client.get_query_all_result_sets(query_id, result_set_count=result_set_count)
        assert results[0] == {"rows": [[777]], "columns": [{"name": "column0", "type": "Int32"}]}
        assert results[1] == {"rows": [["zzz"]], "columns": [{"name": "aaaa", "type": "Utf8"}]}
