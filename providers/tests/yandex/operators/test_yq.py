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

import re
from datetime import datetime, timedelta
from unittest.mock import MagicMock, call, patch

import pytest

from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS

yandexcloud = pytest.importorskip("yandexcloud")

import responses
from responses import matchers

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.yandex.operators.yq import YQExecuteQueryOperator

OAUTH_TOKEN = "my_oauth_token"
FOLDER_ID = "my_folder_id"


class TestYQExecuteQueryOperator:
    def setup_method(self):
        dag_id = "test_dag"
        self.dag = DAG(
            dag_id,
            default_args={
                "owner": "airflow",
                "start_date": datetime.today(),
                "end_date": datetime.today() + timedelta(days=1),
            },
            schedule="@once",
        )

    @responses.activate()
    @patch("airflow.hooks.base.BaseHook.get_connection")
    def test_execute_query(self, mock_get_connection):
        mock_get_connection.return_value = Connection(extra={"oauth": OAUTH_TOKEN})
        operator = YQExecuteQueryOperator(
            task_id="simple_sql", sql="select 987", folder_id="my_folder_id"
        )
        context = {"ti": MagicMock()}

        responses.post(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries",
            match=[
                matchers.header_matcher(
                    {
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {OAUTH_TOKEN}",
                    }
                ),
                matchers.query_param_matcher({"project": FOLDER_ID}),
                matchers.json_params_matcher({"text": "select 987"}),
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

        results = operator.execute(context)
        assert results == {
            "rows": [[777]],
            "columns": [{"name": "column0", "type": "Int32"}],
        }

        if AIRFLOW_V_3_0_PLUS:
            context["ti"].xcom_push.assert_has_calls(
                [
                    call(
                        key="web_link",
                        value=f"https://yq.cloud.yandex.ru/folders/{FOLDER_ID}/ide/queries/query1",
                    ),
                ]
            )
        else:
            context["ti"].xcom_push.assert_has_calls(
                [
                    call(
                        key="web_link",
                        value=f"https://yq.cloud.yandex.ru/folders/{FOLDER_ID}/ide/queries/query1",
                        execution_date=None,
                    ),
                ]
            )

        responses.get(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1/status",
            json={"status": "ERROR"},
            status=200,
        )

        responses.get(
            "https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/query1",
            json={"id": "query1", "issues": ["some error"]},
            status=200,
        )

        with pytest.raises(
            RuntimeError,
            match=re.escape("""Query query1 failed with issues=['some error']"""),
        ):
            operator.execute(context)
