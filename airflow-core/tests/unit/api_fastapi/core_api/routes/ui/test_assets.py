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
from fastapi import status
from sqlalchemy import select, update

from airflow.models.dag import DAG
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from airflow.utils.session import create_session
from airflow.utils.timezone import datetime

from tests_common.test_utils.db import clear_db_dags, clear_db_serialized_dags

pytestmark = pytest.mark.db_test

TEST_DAG_ID = "test_dag_missing_dag_id_in_db"
TEST_DAG_RUN_ID = "test_dag_run_missing_dag_id_in_db"
TEST_DAG_ID = "test_dag_missing_dag_id"
TEST_TASK_ID = "test_task"
TEST_RUN_ID = "test_run"
TEST_XCOM_KEY = "test_xcom_key"
TEST_XCOM_VALUE = {"foo": "bar"}


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

    def test_next_run_assets_missing_dag_id_in_serialized_data(self, test_client):
        """
        Test /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries endpoint
        when serialized DAG is missing dag_id (should return 400 error).
        """

        test_dag = DAG(dag_id=TEST_DAG_ID, start_date=datetime(2025, 4, 15), schedule="@once")
        EmptyOperator(task_id=TEST_TASK_ID, dag=test_dag)
        test_dag.sync_to_db()
        SerializedDagModel.write_dag(test_dag, bundle_name=TEST_DAG_ID)
        with create_session() as session:
            dag_model = session.scalar(
                select(SerializedDagModel).where(SerializedDagModel.dag_id == TEST_DAG_ID)
            )
            if not dag_model:
                pytest.fail("Failed to find serialized DAG in database")
            data = dag_model.data
            del data["dag"]["dag_id"]
            session.execute(
                update(SerializedDagModel).where(SerializedDagModel.dag_id == TEST_DAG_ID).values(_data=data)
            )
            session.commit()
        response = test_client.get(
            f"/next_run_assets/{TEST_DAG_ID}",
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert f"An unexpected error occurred while trying to deserialize DAG '{TEST_DAG_ID}'." == response.json()["detail"]
