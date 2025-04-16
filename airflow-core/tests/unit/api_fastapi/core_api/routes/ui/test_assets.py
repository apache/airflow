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

from unittest import mock

import pytest

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset

from tests_common.test_utils.db import clear_db_dags, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def cleanup():
    clear_db_dags()
    clear_db_serialized_dags()


class TestNextRunAssets:
    def test_should_response_200(self, test_client, dag_maker):
        with dag_maker(
            dag_id="upstream",
            schedule=[Asset(uri="s3://bucket/next-run-asset/1", name="asset1")],
            serialized=True,
        ):
            EmptyOperator(task_id="task1")

        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        response = test_client.get("/next_run_assets/upstream")

        assert response.status_code == 200
        assert response.json() == {
            "asset_expression": {
                "all": [
                    {
                        "asset": {
                            "uri": "s3://bucket/next-run-asset/1",
                            "name": "asset1",
                            "group": "asset",
                            "id": mock.ANY,
                        }
                    }
                ]
            },
            "events": [
                {"id": mock.ANY, "uri": "s3://bucket/next-run-asset/1", "name": "asset1", "lastUpdate": None}
            ],
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/next_run_assets/upstream")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/next_run_assets/upstream")
        assert response.status_code == 403
