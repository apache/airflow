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

import os
from datetime import datetime

import pendulum
import pytest

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models import DagBag, DagModel
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.security import permissions
from airflow.utils.session import provide_session
from tests.providers.fab.auth_manager.api_endpoints.api_connexion_utils import create_user, delete_user
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags
from tests.test_utils.www import _check_last_log

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


@pytest.fixture
def current_file_token(url_safe_serializer) -> str:
    return url_safe_serializer.dumps(__file__)


DAG_ID = "test_dag"
TASK_ID = "op1"
DAG2_ID = "test_dag2"
DAG3_ID = "test_dag3"
UTC_JSON_REPR = "UTC" if pendulum.__version__.startswith("3") else "Timezone('UTC')"


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_auth_api):
    app = minimal_app_for_auth_api

    create_user(app, username="test_granular_permissions", role_name="TestGranularDag")
    app.appbuilder.sm.sync_perm_for_dag(
        "TEST_DAG_1",
        access_control={
            "TestGranularDag": {
                permissions.RESOURCE_DAG: {permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ}
            },
        },
    )
    app.appbuilder.sm.sync_perm_for_dag(
        "TEST_DAG_1",
        access_control={
            "TestGranularDag": {
                permissions.RESOURCE_DAG: {permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ}
            },
        },
    )

    with DAG(
        DAG_ID,
        schedule=None,
        start_date=datetime(2020, 6, 15),
        doc_md="details",
        params={"foo": 1},
        tags=["example"],
    ) as dag:
        EmptyOperator(task_id=TASK_ID)

    with DAG(DAG2_ID, schedule=None, start_date=datetime(2020, 6, 15)) as dag2:  # no doc_md
        EmptyOperator(task_id=TASK_ID)

    with DAG(DAG3_ID, schedule=None) as dag3:  # DAG start_date set to None
        EmptyOperator(task_id=TASK_ID, start_date=datetime(2019, 6, 12))

    dag_bag = DagBag(os.devnull, include_examples=False)
    dag_bag.dags = {dag.dag_id: dag, dag2.dag_id: dag2, dag3.dag_id: dag3}

    app.dag_bag = dag_bag

    yield app

    delete_user(app, username="test_granular_permissions")


class TestDagEndpoint:
    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.clean_db()
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        self.dag_id = DAG_ID
        self.dag2_id = DAG2_ID
        self.dag3_id = DAG3_ID

    def teardown_method(self) -> None:
        self.clean_db()

    @provide_session
    def _create_dag_models(self, count, dag_id_prefix="TEST_DAG", is_paused=False, session=None):
        for num in range(1, count + 1):
            dag_model = DagModel(
                dag_id=f"{dag_id_prefix}_{num}",
                fileloc=f"/tmp/dag_{num}.py",
                timetable_summary="2 2 * * *",
                is_active=True,
                is_paused=is_paused,
            )
            session.add(dag_model)

    @provide_session
    def _create_dag_model_for_details_endpoint(self, dag_id, session=None):
        dag_model = DagModel(
            dag_id=dag_id,
            fileloc="/tmp/dag.py",
            timetable_summary="2 2 * * *",
            is_active=True,
            is_paused=False,
        )
        session.add(dag_model)

    @provide_session
    def _create_dag_model_for_details_endpoint_with_dataset_expression(self, dag_id, session=None):
        dag_model = DagModel(
            dag_id=dag_id,
            fileloc="/tmp/dag.py",
            timetable_summary="2 2 * * *",
            is_active=True,
            is_paused=False,
            dataset_expression={
                "any": [
                    "s3://dag1/output_1.txt",
                    {"all": ["s3://dag2/output_1.txt", "s3://dag3/output_3.txt"]},
                ]
            },
        )
        session.add(dag_model)

    @provide_session
    def _create_deactivated_dag(self, session=None):
        dag_model = DagModel(
            dag_id="TEST_DAG_DELETED_1",
            fileloc="/tmp/dag_del_1.py",
            timetable_summary="2 2 * * *",
            is_active=False,
        )
        session.add(dag_model)


class TestGetDag(TestDagEndpoint):
    def test_should_respond_200_with_granular_dag_access(self):
        self._create_dag_models(1)
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_1", environ_overrides={"REMOTE_USER": "test_granular_permissions"}
        )
        assert response.status_code == 200

    def test_should_respond_403_with_granular_access_for_different_dag(self):
        self._create_dag_models(3)
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_2", environ_overrides={"REMOTE_USER": "test_granular_permissions"}
        )
        assert response.status_code == 403


class TestGetDags(TestDagEndpoint):
    def test_should_respond_200_with_granular_dag_access(self):
        self._create_dag_models(3)
        response = self.client.get(
            "/api/v1/dags", environ_overrides={"REMOTE_USER": "test_granular_permissions"}
        )
        assert response.status_code == 200
        assert len(response.json["dags"]) == 1
        assert response.json["dags"][0]["dag_id"] == "TEST_DAG_1"


class TestPatchDag(TestDagEndpoint):
    @provide_session
    def _create_dag_model(self, session=None):
        dag_model = DagModel(
            dag_id="TEST_DAG_1", fileloc="/tmp/dag_1.py", timetable_summary="2 2 * * *", is_paused=True
        )
        session.add(dag_model)
        return dag_model

    def test_should_respond_200_on_patch_with_granular_dag_access(self, session):
        self._create_dag_models(1)
        response = self.client.patch(
            "/api/v1/dags/TEST_DAG_1",
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test_granular_permissions"},
        )
        assert response.status_code == 200
        _check_last_log(session, dag_id="TEST_DAG_1", event="api.patch_dag", execution_date=None)

    def test_validation_error_raises_400(self):
        patch_body = {
            "ispaused": True,
        }
        dag_model = self._create_dag_model()
        response = self.client.patch(
            f"/api/v1/dags/{dag_model.dag_id}",
            json=patch_body,
            environ_overrides={"REMOTE_USER": "test_granular_permissions"},
        )
        assert response.status_code == 400
        assert response.json == {
            "detail": "{'ispaused': ['Unknown field.']}",
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }


class TestPatchDags(TestDagEndpoint):
    def test_should_respond_200_with_granular_dag_access(self):
        self._create_dag_models(3)
        response = self.client.patch(
            "api/v1/dags?dag_id_pattern=~",
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test_granular_permissions"},
        )
        assert response.status_code == 200
        assert len(response.json["dags"]) == 1
        assert response.json["dags"][0]["dag_id"] == "TEST_DAG_1"
