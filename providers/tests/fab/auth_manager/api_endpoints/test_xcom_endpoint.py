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

from datetime import timedelta

import pytest

from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import BaseXCom, XCom
from airflow.operators.empty import EmptyOperator
from airflow.security import permissions
from airflow.utils.dates import parse_execution_date
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType

from providers.tests.fab.auth_manager.api_endpoints.api_connexion_utils import (
    create_user,
    delete_user,
)
from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_xcom

pytestmark = [
    pytest.mark.db_test,
    pytest.mark.skip_if_database_isolation_mode,
    pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+"),
]


class CustomXCom(BaseXCom):
    @classmethod
    def deserialize_value(cls, xcom: XCom):
        return f"real deserialized {super().deserialize_value(xcom)}"

    def orm_deserialize_value(self):
        return f"orm deserialized {super().orm_deserialize_value()}"


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_auth_api):
    app = minimal_app_for_auth_api

    create_user(
        app,
        username="test_granular_permissions",
        role_name="TestGranularDag",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
        ],
    )
    app.appbuilder.sm.sync_perm_for_dag(
        "test-dag-id-1",
        access_control={
            "TestGranularDag": [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]
        },
    )

    yield app

    delete_user(app, username="test_granular_permissions")


def _compare_xcom_collections(collection1: dict, collection_2: dict):
    assert collection1.get("total_entries") == collection_2.get("total_entries")

    def sort_key(record):
        return (
            record.get("dag_id"),
            record.get("task_id"),
            record.get("execution_date"),
            record.get("map_index"),
            record.get("key"),
        )

    assert sorted(collection1.get("xcom_entries", []), key=sort_key) == sorted(
        collection_2.get("xcom_entries", []), key=sort_key
    )


class TestXComEndpoint:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_runs()
        clear_db_xcom()

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        """
        Setup For XCom endpoint TC
        """
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        # clear existing xcoms
        self.clean_db()

    def teardown_method(self) -> None:
        """
        Clear Hanging XComs
        """
        self.clean_db()


class TestGetXComEntries(TestXComEndpoint):
    def test_should_respond_200_with_tilde_and_granular_dag_access(self):
        dag_id_1 = "test-dag-id-1"
        task_id_1 = "test-task-id-1"
        execution_date = "2005-04-02T00:00:00+00:00"
        execution_date_parsed = parse_execution_date(execution_date)
        dag_run_id_1 = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entries(
            dag_id_1, dag_run_id_1, execution_date_parsed, task_id_1
        )

        dag_id_2 = "test-dag-id-2"
        task_id_2 = "test-task-id-2"
        run_id_2 = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entries(dag_id_2, run_id_2, execution_date_parsed, task_id_2)
        self._create_invalid_xcom_entries(execution_date_parsed)
        response = self.client.get(
            "/api/v1/dags/~/dagRuns/~/taskInstances/~/xcomEntries",
            environ_overrides={"REMOTE_USER": "test_granular_permissions"},
        )

        assert 200 == response.status_code
        response_data = response.json
        for xcom_entry in response_data["xcom_entries"]:
            xcom_entry["timestamp"] = "TIMESTAMP"
        _compare_xcom_collections(
            response_data,
            {
                "xcom_entries": [
                    {
                        "dag_id": dag_id_1,
                        "execution_date": execution_date,
                        "key": "test-xcom-key-1",
                        "task_id": task_id_1,
                        "timestamp": "TIMESTAMP",
                        "map_index": -1,
                    },
                    {
                        "dag_id": dag_id_1,
                        "execution_date": execution_date,
                        "key": "test-xcom-key-2",
                        "task_id": task_id_1,
                        "timestamp": "TIMESTAMP",
                        "map_index": -1,
                    },
                ],
                "total_entries": 2,
            },
        )

    def _create_xcom_entries(
        self, dag_id, run_id, execution_date, task_id, mapped_ti=False
    ):
        with create_session() as session:
            dag = DagModel(dag_id=dag_id)
            session.add(dag)
            dagrun = DagRun(
                dag_id=dag_id,
                run_id=run_id,
                execution_date=execution_date,
                start_date=execution_date,
                run_type=DagRunType.MANUAL,
            )
            session.add(dagrun)
            if mapped_ti:
                for i in [0, 1]:
                    ti = TaskInstance(
                        EmptyOperator(task_id=task_id), run_id=run_id, map_index=i
                    )
                    ti.dag_id = dag_id
                    session.add(ti)
            else:
                ti = TaskInstance(EmptyOperator(task_id=task_id), run_id=run_id)
                ti.dag_id = dag_id
                session.add(ti)

        for i in [1, 2]:
            if mapped_ti:
                key = "test-xcom-key"
                map_index = i - 1
            else:
                key = f"test-xcom-key-{i}"
                map_index = -1

            XCom.set(
                key=key,
                value="TEST",
                run_id=run_id,
                task_id=task_id,
                dag_id=dag_id,
                map_index=map_index,
            )

    def _create_invalid_xcom_entries(self, execution_date):
        """
        Invalid XCom entries to test join query
        """
        with create_session() as session:
            dag = DagModel(dag_id="invalid_dag")
            session.add(dag)
            dagrun = DagRun(
                dag_id="invalid_dag",
                run_id="invalid_run_id",
                execution_date=execution_date + timedelta(days=1),
                start_date=execution_date,
                run_type=DagRunType.MANUAL,
            )
            session.add(dagrun)
            dagrun1 = DagRun(
                dag_id="invalid_dag",
                run_id="not_this_run_id",
                execution_date=execution_date,
                start_date=execution_date,
                run_type=DagRunType.MANUAL,
            )
            session.add(dagrun1)
            ti = TaskInstance(
                EmptyOperator(task_id="invalid_task"), run_id="not_this_run_id"
            )
            ti.dag_id = "invalid_dag"
            session.add(ti)
        for i in [1, 2]:
            XCom.set(
                key=f"invalid-xcom-key-{i}",
                value="TEST",
                run_id="not_this_run_id",
                task_id="invalid_task",
                dag_id="invalid_dag",
            )
