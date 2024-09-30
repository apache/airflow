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

from airflow.models.dag import DAG, DagModel
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from tests.test_utils.compat import AIRFLOW_V_3_0_PLUS, ignore_provider_compatibility_error

with ignore_provider_compatibility_error("3.0.0+", __file__):
    from airflow.utils.types import DagRunTriggeredByType, DagRunType
from tests.providers.fab.auth_manager.api_endpoints.api_connexion_utils import (
    create_user,
    delete_roles,
    delete_user,
)
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = [
    pytest.mark.db_test,
    pytest.mark.skip_if_database_isolation_mode,
    pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+"),
]


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_auth_api):
    app = minimal_app_for_auth_api

    create_user(
        app,
        username="test_no_dag_run_create_permission",
        role_name="TestNoDagRunCreatePermission",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_ASSET),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_CLUSTER_ACTIVITY),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG_RUN),
        ],
    )
    create_user(
        app,
        username="test_dag_view_only",
        role_name="TestViewDags",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG_RUN),
        ],
    )
    create_user(
        app,
        username="test_view_dags",
        role_name="TestViewDags",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN),
        ],
    )
    create_user(
        app,
        username="test_granular_permissions",
        role_name="TestGranularDag",
        permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN)],
    )
    app.appbuilder.sm.sync_perm_for_dag(
        "TEST_DAG_ID",
        access_control={
            "TestGranularDag": {permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ},
            "TestNoDagRunCreatePermission": {permissions.RESOURCE_DAG_RUN: {permissions.ACTION_CAN_CREATE}},
        },
    )

    yield app

    delete_user(app, username="test_dag_view_only")
    delete_user(app, username="test_view_dags")
    delete_user(app, username="test_granular_permissions")
    delete_user(app, username="test_no_dag_run_create_permission")
    delete_roles(app)


class TestDagRunEndpoint:
    default_time = "2020-06-11T18:00:00+00:00"
    default_time_2 = "2020-06-12T18:00:00+00:00"
    default_time_3 = "2020-06-13T18:00:00+00:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        clear_db_runs()
        clear_db_serialized_dags()
        clear_db_dags()

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    def _create_dag(self, dag_id):
        dag_instance = DagModel(dag_id=dag_id)
        dag_instance.is_active = True
        with create_session() as session:
            session.add(dag_instance)
        dag = DAG(dag_id=dag_id, schedule=None, params={"validated_number": Param(1, minimum=1, maximum=10)})
        self.app.dag_bag.bag_dag(dag)
        return dag_instance

    def _create_test_dag_run(self, state=DagRunState.RUNNING, extra_dag=False, commit=True, idx_start=1):
        dag_runs = []
        dags = []
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        for i in range(idx_start, idx_start + 2):
            if i == 1:
                dags.append(DagModel(dag_id="TEST_DAG_ID", is_active=True))
            dagrun_model = DagRun(
                dag_id="TEST_DAG_ID",
                run_id=f"TEST_DAG_RUN_ID_{i}",
                run_type=DagRunType.MANUAL,
                execution_date=timezone.parse(self.default_time) + timedelta(days=i - 1),
                start_date=timezone.parse(self.default_time),
                external_trigger=True,
                state=state,
                **triggered_by_kwargs,
            )
            dagrun_model.updated_at = timezone.parse(self.default_time)
            dag_runs.append(dagrun_model)

        if extra_dag:
            for i in range(idx_start + 2, idx_start + 4):
                dags.append(DagModel(dag_id=f"TEST_DAG_ID_{i}"))
                dag_runs.append(
                    DagRun(
                        dag_id=f"TEST_DAG_ID_{i}",
                        run_id=f"TEST_DAG_RUN_ID_{i}",
                        run_type=DagRunType.MANUAL,
                        execution_date=timezone.parse(self.default_time_2),
                        start_date=timezone.parse(self.default_time),
                        external_trigger=True,
                        state=state,
                    )
                )
        if commit:
            with create_session() as session:
                session.add_all(dag_runs)
                session.add_all(dags)
        return dag_runs


class TestGetDagRuns(TestDagRunEndpoint):
    def test_should_return_accessible_with_tilde_as_dag_id_and_dag_level_permissions(self):
        self._create_test_dag_run(extra_dag=True)
        expected_dag_run_ids = ["TEST_DAG_ID", "TEST_DAG_ID"]
        response = self.client.get(
            "api/v1/dags/~/dagRuns", environ_overrides={"REMOTE_USER": "test_granular_permissions"}
        )
        assert response.status_code == 200
        dag_run_ids = [dag_run["dag_id"] for dag_run in response.json["dag_runs"]]
        assert dag_run_ids == expected_dag_run_ids


class TestGetDagRunBatch(TestDagRunEndpoint):
    def test_should_return_accessible_with_tilde_as_dag_id_and_dag_level_permissions(self):
        self._create_test_dag_run(extra_dag=True)
        expected_response_json_1 = {
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": "TEST_DAG_RUN_ID_1",
            "end_date": None,
            "state": "running",
            "execution_date": self.default_time,
            "logical_date": self.default_time,
            "external_trigger": True,
            "start_date": self.default_time,
            "conf": {},
            "data_interval_end": None,
            "data_interval_start": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": None,
        }
        expected_response_json_1.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})
        expected_response_json_2 = {
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": "TEST_DAG_RUN_ID_2",
            "end_date": None,
            "state": "running",
            "execution_date": self.default_time_2,
            "logical_date": self.default_time_2,
            "external_trigger": True,
            "start_date": self.default_time,
            "conf": {},
            "data_interval_end": None,
            "data_interval_start": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": None,
        }
        expected_response_json_2.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})

        response = self.client.post(
            "api/v1/dags/~/dagRuns/list",
            json={"dag_ids": []},
            environ_overrides={"REMOTE_USER": "test_granular_permissions"},
        )
        assert response.status_code == 200
        assert response.json == {
            "dag_runs": [
                expected_response_json_1,
                expected_response_json_2,
            ],
            "total_entries": 2,
        }


class TestPostDagRun(TestDagRunEndpoint):
    def test_dagrun_trigger_with_dag_level_permissions(self):
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={"conf": {"validated_number": 1}},
            environ_overrides={"REMOTE_USER": "test_no_dag_run_create_permission"},
        )
        assert response.status_code == 200

    @pytest.mark.parametrize(
        "username",
        ["test_dag_view_only", "test_view_dags", "test_granular_permissions"],
    )
    def test_should_raises_403_unauthorized(self, username):
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={
                "dag_run_id": "TEST_DAG_RUN_ID_1",
                "execution_date": self.default_time,
            },
            environ_overrides={"REMOTE_USER": username},
        )
        assert response.status_code == 403
