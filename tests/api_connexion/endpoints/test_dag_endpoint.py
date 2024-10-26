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
import unittest.mock
from datetime import datetime

import pendulum
import pytest

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models import DagBag, DagModel
from airflow.models.dag import DAG
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags
from tests_common.test_utils.www import _check_last_log

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
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api

    create_user(
        app,
        username="test",
        role_name="admin",
    )
    create_user(app, username="test_no_permissions", role_name=None)

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

    delete_user(app, username="test")
    delete_user(app, username="test_no_permissions")


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
    def _create_dag_model_for_details_endpoint_with_asset_expression(self, dag_id, session=None):
        dag_model = DagModel(
            dag_id=dag_id,
            fileloc="/tmp/dag.py",
            timetable_summary="2 2 * * *",
            is_active=True,
            is_paused=False,
            asset_expression={
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
    @conf_vars({("webserver", "secret_key"): "mysecret"})
    def test_should_respond_200(self):
        self._create_dag_models(1)
        response = self.client.get("/api/v1/dags/TEST_DAG_1", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert {
            "dag_id": "TEST_DAG_1",
            "dag_display_name": "TEST_DAG_1",
            "description": None,
            "fileloc": "/tmp/dag_1.py",
            "file_token": "Ii90bXAvZGFnXzEucHki.EnmIdPaUPo26lHQClbWMbDFD1Pk",
            "is_paused": False,
            "is_active": True,
            "owners": [],
            "timetable_summary": "2 2 * * *",
            "tags": [],
            "next_dagrun": None,
            "has_task_concurrency_limits": True,
            "next_dagrun_data_interval_start": None,
            "next_dagrun_data_interval_end": None,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "next_dagrun_create_after": None,
            "last_expired": None,
            "max_active_tasks": 16,
            "last_pickled": None,
            "default_view": None,
            "last_parsed_time": None,
            "scheduler_lock": None,
            "timetable_description": None,
            "has_import_errors": False,
            "pickle_id": None,
        } == response.json

    @conf_vars({("webserver", "secret_key"): "mysecret"})
    def test_should_respond_200_with_schedule_none(self, session):
        dag_model = DagModel(
            dag_id="TEST_DAG_1",
            fileloc="/tmp/dag_1.py",
            timetable_summary=None,
            is_paused=False,
        )
        session.add(dag_model)
        session.commit()
        response = self.client.get("/api/v1/dags/TEST_DAG_1", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert {
            "dag_id": "TEST_DAG_1",
            "dag_display_name": "TEST_DAG_1",
            "description": None,
            "fileloc": "/tmp/dag_1.py",
            "file_token": "Ii90bXAvZGFnXzEucHki.EnmIdPaUPo26lHQClbWMbDFD1Pk",
            "is_paused": False,
            "is_active": False,
            "owners": [],
            "timetable_summary": None,
            "tags": [],
            "next_dagrun": None,
            "has_task_concurrency_limits": True,
            "next_dagrun_data_interval_start": None,
            "next_dagrun_data_interval_end": None,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "next_dagrun_create_after": None,
            "last_expired": None,
            "max_active_tasks": 16,
            "last_pickled": None,
            "default_view": None,
            "last_parsed_time": None,
            "scheduler_lock": None,
            "timetable_description": None,
            "has_import_errors": False,
            "pickle_id": None,
        } == response.json

    def test_should_respond_404(self):
        response = self.client.get("/api/v1/dags/INVALID_DAG", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 404

    def test_should_raises_401_unauthenticated(self):
        self._create_dag_models(1)

        response = self.client.get("/api/v1/dags/TEST_DAG_1")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/details", environ_overrides={"REMOTE_USER": "test_no_permissions"}
        )
        assert response.status_code == 403

    @pytest.mark.parametrize(
        "fields",
        [
            ["dag_id"],  # only one
            ["fileloc", "file_token", "owners"],  # auto_field and fields.Method
            ["tags"],  # fields.List
        ],
    )
    def test_should_return_specified_fields(self, fields):
        self._create_dag_models(1)
        response = self.client.get(
            f"/api/v1/dags/TEST_DAG_1?fields={','.join(fields)}", environ_overrides={"REMOTE_USER": "test"}
        )
        res_json = response.json
        assert len(res_json.keys()) == len(fields)
        for field in fields:
            assert field in res_json

    @pytest.mark.parametrize(
        "fields",
        [
            [],  # empty test
            ["#caw&c"],  # field which not exists
            ["dag_id", "#caw&c"],  # field which not exists
        ],
    )
    def test_should_respond_400_with_not_exists_fields(self, fields):
        self._create_dag_models(1)
        response = self.client.get(
            f"/api/v1/dags/TEST_DAG_1?fields={','.join(fields)}", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 400, f"Current code: {response.status_code}"


class TestGetDagDetails(TestDagEndpoint):
    def test_should_respond_200(self, url_safe_serializer):
        self._create_dag_model_for_details_endpoint(self.dag_id)
        current_file_token = url_safe_serializer.dumps("/tmp/dag.py")
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/details", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        last_parsed = response.json["last_parsed"]
        expected = {
            "catchup": True,
            "dag_id": "test_dag",
            "dag_display_name": "test_dag",
            "dag_run_timeout": None,
            "asset_expression": None,
            "default_view": None,
            "description": None,
            "doc_md": "details",
            "end_date": None,
            "fileloc": "/tmp/dag.py",
            "file_token": current_file_token,
            "has_import_errors": False,
            "has_task_concurrency_limits": True,
            "is_active": True,
            "is_paused": False,
            "is_paused_upon_creation": None,
            "last_expired": None,
            "last_parsed": last_parsed,
            "last_parsed_time": None,
            "last_pickled": None,
            "max_active_runs": 16,
            "max_active_tasks": 16,
            "max_consecutive_failed_dag_runs": 0,
            "next_dagrun": None,
            "next_dagrun_create_after": None,
            "next_dagrun_data_interval_end": None,
            "next_dagrun_data_interval_start": None,
            "orientation": "LR",
            "owners": [],
            "params": {
                "foo": {
                    "__class": "airflow.models.param.Param",
                    "description": None,
                    "schema": {},
                    "value": 1,
                }
            },
            "pickle_id": None,
            "render_template_as_native_obj": False,
            "timetable_summary": "2 2 * * *",
            "scheduler_lock": None,
            "start_date": "2020-06-15T00:00:00+00:00",
            "tags": [],
            "template_searchpath": None,
            "timetable_description": None,
            "timezone": UTC_JSON_REPR,
        }
        assert response.json == expected

    def test_should_respond_200_with_asset_expression(self, url_safe_serializer):
        self._create_dag_model_for_details_endpoint_with_asset_expression(self.dag_id)
        current_file_token = url_safe_serializer.dumps("/tmp/dag.py")
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/details", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        last_parsed = response.json["last_parsed"]
        expected = {
            "catchup": True,
            "dag_id": "test_dag",
            "dag_display_name": "test_dag",
            "dag_run_timeout": None,
            "asset_expression": {
                "any": [
                    "s3://dag1/output_1.txt",
                    {"all": ["s3://dag2/output_1.txt", "s3://dag3/output_3.txt"]},
                ]
            },
            "default_view": None,
            "description": None,
            "doc_md": "details",
            "end_date": None,
            "fileloc": "/tmp/dag.py",
            "file_token": current_file_token,
            "has_import_errors": False,
            "has_task_concurrency_limits": True,
            "is_active": True,
            "is_paused": False,
            "is_paused_upon_creation": None,
            "last_expired": None,
            "last_parsed": last_parsed,
            "last_parsed_time": None,
            "last_pickled": None,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "max_active_tasks": 16,
            "next_dagrun": None,
            "next_dagrun_create_after": None,
            "next_dagrun_data_interval_end": None,
            "next_dagrun_data_interval_start": None,
            "orientation": "LR",
            "owners": [],
            "params": {
                "foo": {
                    "__class": "airflow.models.param.Param",
                    "description": None,
                    "schema": {},
                    "value": 1,
                }
            },
            "pickle_id": None,
            "render_template_as_native_obj": False,
            "timetable_summary": "2 2 * * *",
            "scheduler_lock": None,
            "start_date": "2020-06-15T00:00:00+00:00",
            "tags": [],
            "template_searchpath": None,
            "timetable_description": None,
            "timezone": UTC_JSON_REPR,
        }
        assert response.json == expected

    def test_should_response_200_with_doc_md_none(self, url_safe_serializer):
        current_file_token = url_safe_serializer.dumps("/tmp/dag.py")
        self._create_dag_model_for_details_endpoint(self.dag2_id)
        response = self.client.get(
            f"/api/v1/dags/{self.dag2_id}/details", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        last_parsed = response.json["last_parsed"]
        expected = {
            "catchup": True,
            "dag_id": "test_dag2",
            "dag_display_name": "test_dag2",
            "dag_run_timeout": None,
            "asset_expression": None,
            "default_view": None,
            "description": None,
            "doc_md": None,
            "end_date": None,
            "fileloc": "/tmp/dag.py",
            "file_token": current_file_token,
            "has_import_errors": False,
            "has_task_concurrency_limits": True,
            "is_active": True,
            "is_paused": False,
            "is_paused_upon_creation": None,
            "last_expired": None,
            "last_parsed": last_parsed,
            "last_parsed_time": None,
            "last_pickled": None,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "max_active_tasks": 16,
            "next_dagrun": None,
            "next_dagrun_create_after": None,
            "next_dagrun_data_interval_end": None,
            "next_dagrun_data_interval_start": None,
            "orientation": "LR",
            "owners": [],
            "params": {},
            "pickle_id": None,
            "render_template_as_native_obj": False,
            "timetable_summary": "2 2 * * *",
            "scheduler_lock": None,
            "start_date": "2020-06-15T00:00:00+00:00",
            "tags": [],
            "template_searchpath": None,
            "timetable_description": None,
            "timezone": UTC_JSON_REPR,
        }
        assert response.json == expected

    def test_should_response_200_for_null_start_date(self, url_safe_serializer):
        current_file_token = url_safe_serializer.dumps("/tmp/dag.py")
        self._create_dag_model_for_details_endpoint(self.dag3_id)
        response = self.client.get(
            f"/api/v1/dags/{self.dag3_id}/details", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        last_parsed = response.json["last_parsed"]
        expected = {
            "catchup": True,
            "dag_id": "test_dag3",
            "dag_display_name": "test_dag3",
            "dag_run_timeout": None,
            "asset_expression": None,
            "default_view": None,
            "description": None,
            "doc_md": None,
            "end_date": None,
            "fileloc": "/tmp/dag.py",
            "file_token": current_file_token,
            "has_import_errors": False,
            "has_task_concurrency_limits": True,
            "is_active": True,
            "is_paused": False,
            "is_paused_upon_creation": None,
            "last_expired": None,
            "last_parsed": last_parsed,
            "last_parsed_time": None,
            "last_pickled": None,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "max_active_tasks": 16,
            "next_dagrun": None,
            "next_dagrun_create_after": None,
            "next_dagrun_data_interval_end": None,
            "next_dagrun_data_interval_start": None,
            "orientation": "LR",
            "owners": [],
            "params": {},
            "pickle_id": None,
            "render_template_as_native_obj": False,
            "timetable_summary": "2 2 * * *",
            "scheduler_lock": None,
            "start_date": None,
            "tags": [],
            "template_searchpath": None,
            "timetable_description": None,
            "timezone": UTC_JSON_REPR,
        }
        assert response.json == expected

    def test_should_respond_200_serialized(self, url_safe_serializer):
        current_file_token = url_safe_serializer.dumps("/tmp/dag.py")
        self._create_dag_model_for_details_endpoint(self.dag_id)
        # Get the dag out of the dagbag before we patch it to an empty one
        SerializedDagModel.write_dag(self.app.dag_bag.get_dag(self.dag_id))

        # Create empty app with empty dagbag to check if DAG is read from db
        dag_bag = DagBag(os.devnull, include_examples=False, read_dags_from_db=True)
        patcher = unittest.mock.patch.object(self.app, "dag_bag", dag_bag)
        patcher.start()

        expected = {
            "catchup": True,
            "dag_id": "test_dag",
            "dag_display_name": "test_dag",
            "dag_run_timeout": None,
            "asset_expression": None,
            "default_view": None,
            "description": None,
            "doc_md": "details",
            "end_date": None,
            "file_token": current_file_token,
            "fileloc": "/tmp/dag.py",
            "has_import_errors": False,
            "has_task_concurrency_limits": True,
            "is_active": True,
            "is_paused": False,
            "is_paused_upon_creation": None,
            "last_expired": None,
            "last_parsed_time": None,
            "last_pickled": None,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "max_active_tasks": 16,
            "next_dagrun": None,
            "next_dagrun_create_after": None,
            "next_dagrun_data_interval_end": None,
            "next_dagrun_data_interval_start": None,
            "orientation": "LR",
            "owners": [],
            "params": {
                "foo": {
                    "__class": "airflow.models.param.Param",
                    "description": None,
                    "schema": {},
                    "value": 1,
                }
            },
            "pickle_id": None,
            "render_template_as_native_obj": False,
            "timetable_summary": "2 2 * * *",
            "scheduler_lock": None,
            "start_date": "2020-06-15T00:00:00+00:00",
            "tags": [],
            "template_searchpath": None,
            "timetable_description": None,
            "timezone": UTC_JSON_REPR,
        }
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/details", environ_overrides={"REMOTE_USER": "test"}
        )

        assert response.status_code == 200
        expected.update({"last_parsed": response.json["last_parsed"]})
        assert response.json == expected

        patcher.stop()

        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/details", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        expected = {
            "catchup": True,
            "dag_id": "test_dag",
            "dag_display_name": "test_dag",
            "dag_run_timeout": None,
            "asset_expression": None,
            "default_view": None,
            "description": None,
            "doc_md": "details",
            "end_date": None,
            "file_token": current_file_token,
            "fileloc": "/tmp/dag.py",
            "has_import_errors": False,
            "has_task_concurrency_limits": True,
            "is_active": True,
            "is_paused": False,
            "is_paused_upon_creation": None,
            "last_expired": None,
            "last_parsed_time": None,
            "last_pickled": None,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "max_active_tasks": 16,
            "next_dagrun": None,
            "next_dagrun_create_after": None,
            "next_dagrun_data_interval_end": None,
            "next_dagrun_data_interval_start": None,
            "orientation": "LR",
            "owners": [],
            "params": {
                "foo": {
                    "__class": "airflow.models.param.Param",
                    "description": None,
                    "schema": {},
                    "value": 1,
                }
            },
            "pickle_id": None,
            "render_template_as_native_obj": False,
            "timetable_summary": "2 2 * * *",
            "scheduler_lock": None,
            "start_date": "2020-06-15T00:00:00+00:00",
            "tags": [],
            "template_searchpath": None,
            "timetable_description": None,
            "timezone": UTC_JSON_REPR,
        }
        expected.update({"last_parsed": response.json["last_parsed"]})
        assert response.json == expected

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get(f"/api/v1/dags/{self.dag_id}/details")

        assert_401(response)

    def test_should_raise_404_when_dag_is_not_found(self):
        response = self.client.get(
            "/api/v1/dags/non_existing_dag_id/details", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 404
        assert response.json == {
            "detail": "The DAG with dag_id: non_existing_dag_id was not found",
            "status": 404,
            "title": "DAG not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        }

    @pytest.mark.parametrize(
        "fields",
        [
            ["dag_id"],  # only one
            ["doc_md", "file_token", "owners"],  # fields.String and fields.Method
            ["tags"],  # fields.List
        ],
    )
    def test_should_return_specified_fields(self, fields):
        self._create_dag_model_for_details_endpoint(self.dag2_id)
        response = self.client.get(
            f"/api/v1/dags/{self.dag2_id}/details?fields={','.join(fields)}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        res_json = response.json
        assert len(res_json.keys()) == len(fields)
        for field in fields:
            assert field in res_json

    def test_should_respond_400_with_not_exists_fields(self):
        fields = ["#caw&c"]
        self._create_dag_model_for_details_endpoint(self.dag2_id)
        response = self.client.get(
            f"/api/v1/dags/{self.dag2_id}/details?fields={','.join(fields)}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400, f"Current code: {response.status_code}"


class TestGetDags(TestDagEndpoint):
    @provide_session
    def test_should_respond_200(self, session, url_safe_serializer):
        self._create_dag_models(2)
        self._create_deactivated_dag()

        dags_query = session.query(DagModel)
        assert len(dags_query.all()) == 3

        response = self.client.get("api/v1/dags", environ_overrides={"REMOTE_USER": "test"})
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        file_token2 = url_safe_serializer.dumps("/tmp/dag_2.py")

        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_1",
                    "dag_display_name": "TEST_DAG_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
                {
                    "dag_id": "TEST_DAG_2",
                    "dag_display_name": "TEST_DAG_2",
                    "description": None,
                    "fileloc": "/tmp/dag_2.py",
                    "file_token": file_token2,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
            ],
            "total_entries": 2,
        } == response.json

    def test_only_active_true_returns_active_dags(self, url_safe_serializer):
        self._create_dag_models(1)
        self._create_deactivated_dag()
        response = self.client.get("api/v1/dags?only_active=True", environ_overrides={"REMOTE_USER": "test"})
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_1",
                    "dag_display_name": "TEST_DAG_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                }
            ],
            "total_entries": 1,
        } == response.json

    def test_only_active_false_returns_all_dags(self, url_safe_serializer):
        self._create_dag_models(1)
        self._create_deactivated_dag()
        response = self.client.get("api/v1/dags?only_active=False", environ_overrides={"REMOTE_USER": "test"})
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        file_token_2 = url_safe_serializer.dumps("/tmp/dag_del_1.py")
        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_1",
                    "dag_display_name": "TEST_DAG_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
                {
                    "dag_id": "TEST_DAG_DELETED_1",
                    "dag_display_name": "TEST_DAG_DELETED_1",
                    "description": None,
                    "fileloc": "/tmp/dag_del_1.py",
                    "file_token": file_token_2,
                    "is_paused": False,
                    "is_active": False,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
            ],
            "total_entries": 2,
        } == response.json

    @pytest.mark.parametrize(
        "url, expected_dag_ids",
        [
            ("api/v1/dags?tags=t1", ["TEST_DAG_1", "TEST_DAG_3"]),
            ("api/v1/dags?tags=t2", ["TEST_DAG_2", "TEST_DAG_3"]),
            ("api/v1/dags?tags=t1,t2", ["TEST_DAG_1", "TEST_DAG_2", "TEST_DAG_3"]),
            ("api/v1/dags", ["TEST_DAG_1", "TEST_DAG_2", "TEST_DAG_3", "TEST_DAG_4"]),
        ],
    )
    def test_filter_dags_by_tags_works(self, url, expected_dag_ids):
        # test filter by tags
        dag1 = DAG(dag_id="TEST_DAG_1", schedule=None, tags=["t1"])
        dag2 = DAG(dag_id="TEST_DAG_2", schedule=None, tags=["t2"])
        dag3 = DAG(dag_id="TEST_DAG_3", schedule=None, tags=["t1", "t2"])
        dag4 = DAG(dag_id="TEST_DAG_4", schedule=None)
        dag1.sync_to_db()
        dag2.sync_to_db()
        dag3.sync_to_db()
        dag4.sync_to_db()

        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        dag_ids = [dag["dag_id"] for dag in response.json["dags"]]

        assert expected_dag_ids == dag_ids

    @pytest.mark.parametrize(
        "url, expected_dag_ids",
        [
            ("api/v1/dags?dag_id_pattern=DAG_1", {"TEST_DAG_1", "SAMPLE_DAG_1"}),
            ("api/v1/dags?dag_id_pattern=SAMPLE_DAG", {"SAMPLE_DAG_1", "SAMPLE_DAG_2"}),
            (
                "api/v1/dags?dag_id_pattern=_DAG_",
                {"TEST_DAG_1", "TEST_DAG_2", "SAMPLE_DAG_1", "SAMPLE_DAG_2"},
            ),
        ],
    )
    def test_filter_dags_by_dag_id_works(self, url, expected_dag_ids):
        # test filter by tags
        dag1 = DAG(dag_id="TEST_DAG_1", schedule=None)
        dag2 = DAG(dag_id="TEST_DAG_2", schedule=None)
        dag3 = DAG(dag_id="SAMPLE_DAG_1", schedule=None)
        dag4 = DAG(dag_id="SAMPLE_DAG_2", schedule=None)
        dag1.sync_to_db()
        dag2.sync_to_db()
        dag3.sync_to_db()
        dag4.sync_to_db()

        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        dag_ids = {dag["dag_id"] for dag in response.json["dags"]}

        assert expected_dag_ids == dag_ids

    @pytest.mark.parametrize(
        "url, expected_dag_ids",
        [
            ("api/v1/dags?limit=1", ["TEST_DAG_1"]),
            ("api/v1/dags?limit=2", ["TEST_DAG_1", "TEST_DAG_10"]),
            (
                "api/v1/dags?offset=5",
                ["TEST_DAG_5", "TEST_DAG_6", "TEST_DAG_7", "TEST_DAG_8", "TEST_DAG_9"],
            ),
            (
                "api/v1/dags?offset=0",
                [
                    "TEST_DAG_1",
                    "TEST_DAG_10",
                    "TEST_DAG_2",
                    "TEST_DAG_3",
                    "TEST_DAG_4",
                    "TEST_DAG_5",
                    "TEST_DAG_6",
                    "TEST_DAG_7",
                    "TEST_DAG_8",
                    "TEST_DAG_9",
                ],
            ),
            ("api/v1/dags?limit=1&offset=5", ["TEST_DAG_5"]),
            ("api/v1/dags?limit=1&offset=1", ["TEST_DAG_10"]),
            ("api/v1/dags?limit=2&offset=2", ["TEST_DAG_2", "TEST_DAG_3"]),
        ],
    )
    def test_should_respond_200_and_handle_pagination(self, url, expected_dag_ids):
        self._create_dag_models(10)

        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})

        assert response.status_code == 200

        dag_ids = [dag["dag_id"] for dag in response.json["dags"]]

        assert expected_dag_ids == dag_ids
        assert 10 == response.json["total_entries"]

    def test_should_respond_200_default_limit(self):
        self._create_dag_models(101)

        response = self.client.get("api/v1/dags", environ_overrides={"REMOTE_USER": "test"})

        assert response.status_code == 200

        assert 100 == len(response.json["dags"])
        assert 101 == response.json["total_entries"]

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("api/v1/dags")

        assert_401(response)

    def test_should_respond_403_unauthorized(self):
        self._create_dag_models(1)

        response = self.client.get("api/v1/dags", environ_overrides={"REMOTE_USER": "test_no_permissions"})

        assert response.status_code == 403

    def test_paused_true_returns_paused_dags(self, url_safe_serializer):
        self._create_dag_models(1, dag_id_prefix="TEST_DAG_PAUSED", is_paused=True)
        self._create_dag_models(1, dag_id_prefix="TEST_DAG_UNPAUSED", is_paused=False)
        response = self.client.get("api/v1/dags?paused=True", environ_overrides={"REMOTE_USER": "test"})
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_PAUSED_1",
                    "dag_display_name": "TEST_DAG_PAUSED_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": True,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                }
            ],
            "total_entries": 1,
        } == response.json

    def test_paused_false_returns_unpaused_dags(self, url_safe_serializer):
        self._create_dag_models(1, dag_id_prefix="TEST_DAG_PAUSED", is_paused=True)
        self._create_dag_models(1, dag_id_prefix="TEST_DAG_UNPAUSED", is_paused=False)
        response = self.client.get("api/v1/dags?paused=False", environ_overrides={"REMOTE_USER": "test"})
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_UNPAUSED_1",
                    "dag_display_name": "TEST_DAG_UNPAUSED_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                }
            ],
            "total_entries": 1,
        } == response.json

    def test_paused_none_returns_all_dags(self, url_safe_serializer):
        self._create_dag_models(1, dag_id_prefix="TEST_DAG_PAUSED", is_paused=True)
        self._create_dag_models(1, dag_id_prefix="TEST_DAG_UNPAUSED", is_paused=False)
        response = self.client.get("api/v1/dags", environ_overrides={"REMOTE_USER": "test"})
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_PAUSED_1",
                    "dag_display_name": "TEST_DAG_PAUSED_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": True,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
                {
                    "dag_id": "TEST_DAG_UNPAUSED_1",
                    "dag_display_name": "TEST_DAG_UNPAUSED_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
            ],
            "total_entries": 2,
        } == response.json

    def test_should_return_specified_fields(self):
        self._create_dag_models(2)
        self._create_deactivated_dag()

        fields = ["dag_id", "file_token", "owners"]
        response = self.client.get(
            f"api/v1/dags?fields={','.join(fields)}", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200

        res_json = response.json
        for dag in res_json["dags"]:
            assert len(dag.keys()) == len(fields)
            for field in fields:
                assert field in dag

    def test_should_respond_400_with_not_exists_fields(self):
        self._create_dag_models(1)
        self._create_deactivated_dag()
        fields = ["#caw&c"]
        response = self.client.get(
            f"api/v1/dags?fields={','.join(fields)}", environ_overrides={"REMOTE_USER": "test"}
        )

        assert response.status_code == 400, f"Current code: {response.status_code}"


class TestPatchDag(TestDagEndpoint):
    def test_should_respond_200_on_patch_is_paused(self, url_safe_serializer, session):
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        dag_model = self._create_dag_model()
        payload = {"is_paused": False}
        response = self.client.patch(
            f"/api/v1/dags/{dag_model.dag_id}",
            json=payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        expected_response = {
            "dag_id": "TEST_DAG_1",
            "dag_display_name": "TEST_DAG_1",
            "description": None,
            "fileloc": "/tmp/dag_1.py",
            "file_token": file_token,
            "is_paused": False,
            "is_active": False,
            "owners": [],
            "timetable_summary": "2 2 * * *",
            "tags": [],
            "next_dagrun": None,
            "has_task_concurrency_limits": True,
            "next_dagrun_data_interval_start": None,
            "next_dagrun_data_interval_end": None,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "next_dagrun_create_after": None,
            "last_expired": None,
            "max_active_tasks": 16,
            "last_pickled": None,
            "default_view": None,
            "last_parsed_time": None,
            "scheduler_lock": None,
            "timetable_description": None,
            "has_import_errors": False,
            "pickle_id": None,
        }
        assert response.json == expected_response
        _check_last_log(
            session, dag_id="TEST_DAG_1", event="api.patch_dag", execution_date=None, expected_extra=payload
        )

    def test_should_respond_400_on_invalid_request(self):
        patch_body = {
            "is_paused": True,
            "timetable_summary": "1 1 * * *",
        }
        dag_model = self._create_dag_model()
        response = self.client.patch(f"/api/v1/dags/{dag_model.dag_id}", json=patch_body)
        assert response.status_code == 400
        assert response.json == {
            "detail": "Property is read-only - 'timetable_summary'",
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }

    def test_non_existing_dag_raises_not_found(self):
        patch_body = {
            "is_paused": True,
        }
        response = self.client.patch(
            "/api/v1/dags/non_existing_dag", json=patch_body, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 404
        assert response.json == {
            "detail": None,
            "status": 404,
            "title": "Dag with id: 'non_existing_dag' not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        }

    def test_should_respond_404(self):
        response = self.client.get("/api/v1/dags/INVALID_DAG", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 404

    @provide_session
    def _create_dag_model(self, session=None):
        dag_model = DagModel(
            dag_id="TEST_DAG_1", fileloc="/tmp/dag_1.py", timetable_summary="2 2 * * *", is_paused=True
        )
        session.add(dag_model)
        return dag_model

    def test_should_raises_401_unauthenticated(self):
        dag_model = self._create_dag_model()
        response = self.client.patch(
            f"/api/v1/dags/{dag_model.dag_id}",
            json={
                "is_paused": False,
            },
        )

        assert_401(response)

    def test_should_respond_200_with_update_mask(self, url_safe_serializer):
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        dag_model = self._create_dag_model()
        payload = {
            "is_paused": False,
        }
        response = self.client.patch(
            f"/api/v1/dags/{dag_model.dag_id}?update_mask=is_paused",
            json=payload,
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 200
        expected_response = {
            "dag_id": "TEST_DAG_1",
            "dag_display_name": "TEST_DAG_1",
            "description": None,
            "fileloc": "/tmp/dag_1.py",
            "file_token": file_token,
            "is_paused": False,
            "is_active": False,
            "owners": [],
            "timetable_summary": "2 2 * * *",
            "tags": [],
            "next_dagrun": None,
            "has_task_concurrency_limits": True,
            "next_dagrun_data_interval_start": None,
            "next_dagrun_data_interval_end": None,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "next_dagrun_create_after": None,
            "last_expired": None,
            "max_active_tasks": 16,
            "last_pickled": None,
            "default_view": None,
            "last_parsed_time": None,
            "scheduler_lock": None,
            "timetable_description": None,
            "has_import_errors": False,
            "pickle_id": None,
        }
        assert response.json == expected_response

    @pytest.mark.parametrize(
        "payload, update_mask, error_message",
        [
            (
                {
                    "is_paused": True,
                },
                "update_mask=description",
                "Only `is_paused` field can be updated through the REST API",
            ),
            (
                {
                    "is_paused": True,
                },
                "update_mask=timetable_summary, description",
                "Only `is_paused` field can be updated through the REST API",
            ),
        ],
    )
    def test_should_respond_400_for_invalid_fields_in_update_mask(self, payload, update_mask, error_message):
        dag_model = self._create_dag_model()

        response = self.client.patch(
            f"/api/v1/dags/{dag_model.dag_id}?{update_mask}",
            json=payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert response.json["detail"] == error_message

    def test_should_respond_403_unauthorized(self):
        dag_model = self._create_dag_model()
        response = self.client.patch(
            f"/api/v1/dags/{dag_model.dag_id}",
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )

        assert response.status_code == 403


class TestPatchDags(TestDagEndpoint):
    @provide_session
    def test_should_respond_200_on_patch_is_paused(self, session, url_safe_serializer):
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        file_token2 = url_safe_serializer.dumps("/tmp/dag_2.py")
        self._create_dag_models(2)
        self._create_deactivated_dag()

        dags_query = session.query(DagModel)
        assert len(dags_query.all()) == 3

        response = self.client.patch(
            "/api/v1/dags?dag_id_pattern=~",
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_1",
                    "dag_display_name": "TEST_DAG_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
                {
                    "dag_id": "TEST_DAG_2",
                    "dag_display_name": "TEST_DAG_2",
                    "description": None,
                    "fileloc": "/tmp/dag_2.py",
                    "file_token": file_token2,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
            ],
            "total_entries": 2,
        } == response.json
        _check_last_log(session, dag_id=None, event="api.patch_dags", execution_date=None)

    def test_should_respond_200_on_patch_is_paused_using_update_mask(self, session, url_safe_serializer):
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        file_token2 = url_safe_serializer.dumps("/tmp/dag_2.py")
        self._create_dag_models(2)
        self._create_deactivated_dag()

        dags_query = session.query(DagModel)
        assert len(dags_query.all()) == 3

        response = self.client.patch(
            "/api/v1/dags?dag_id_pattern=~&update_mask=is_paused",
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_1",
                    "dag_display_name": "TEST_DAG_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
                {
                    "dag_id": "TEST_DAG_2",
                    "dag_display_name": "TEST_DAG_2",
                    "description": None,
                    "fileloc": "/tmp/dag_2.py",
                    "file_token": file_token2,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
            ],
            "total_entries": 2,
        } == response.json
        _check_last_log(session, dag_id=None, event="api.patch_dags", execution_date=None)

    def test_wrong_value_as_update_mask_rasise(self, session):
        self._create_dag_models(2)
        self._create_deactivated_dag()

        dags_query = session.query(DagModel)
        assert len(dags_query.all()) == 3

        response = self.client.patch(
            "/api/v1/dags?dag_id_pattern=~&update_mask=ispaused",
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 400
        assert response.json == {
            "detail": "Only `is_paused` field can be updated through the REST API",
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }

    def test_invalid_request_body_raises_badrequest(self, session):
        self._create_dag_models(2)
        self._create_deactivated_dag()

        dags_query = session.query(DagModel)
        assert len(dags_query.all()) == 3

        response = self.client.patch(
            "/api/v1/dags?dag_id_pattern=~&update_mask=is_paused",
            json={
                "ispaused": False,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 400
        assert response.json == {
            "detail": "{'ispaused': ['Unknown field.']}",
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }

    def test_only_active_true_returns_active_dags(self, url_safe_serializer, session):
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        self._create_dag_models(1)
        self._create_deactivated_dag()
        response = self.client.patch(
            "/api/v1/dags?only_active=True&dag_id_pattern=~",
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_1",
                    "dag_display_name": "TEST_DAG_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                }
            ],
            "total_entries": 1,
        } == response.json
        _check_last_log(session, dag_id=None, event="api.patch_dags", execution_date=None)

    def test_only_active_false_returns_all_dags(self, url_safe_serializer, session):
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        self._create_dag_models(1)
        self._create_deactivated_dag()
        response = self.client.patch(
            "/api/v1/dags?only_active=False&dag_id_pattern=~",
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )

        file_token_2 = url_safe_serializer.dumps("/tmp/dag_del_1.py")
        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_1",
                    "dag_display_name": "TEST_DAG_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
                {
                    "dag_id": "TEST_DAG_DELETED_1",
                    "dag_display_name": "TEST_DAG_DELETED_1",
                    "description": None,
                    "fileloc": "/tmp/dag_del_1.py",
                    "file_token": file_token_2,
                    "is_paused": False,
                    "is_active": False,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
            ],
            "total_entries": 2,
        } == response.json
        _check_last_log(session, dag_id=None, event="api.patch_dags", execution_date=None)

    @pytest.mark.parametrize(
        "url, expected_dag_ids",
        [
            ("api/v1/dags?tags=t1&dag_id_pattern=~", ["TEST_DAG_1", "TEST_DAG_3"]),
            ("api/v1/dags?tags=t2&dag_id_pattern=~", ["TEST_DAG_2", "TEST_DAG_3"]),
            ("api/v1/dags?tags=t1,t2&dag_id_pattern=~", ["TEST_DAG_1", "TEST_DAG_2", "TEST_DAG_3"]),
            ("api/v1/dags?dag_id_pattern=~", ["TEST_DAG_1", "TEST_DAG_2", "TEST_DAG_3", "TEST_DAG_4"]),
        ],
    )
    def test_filter_dags_by_tags_works(self, url, expected_dag_ids):
        # test filter by tags
        dag1 = DAG(dag_id="TEST_DAG_1", schedule=None, tags=["t1"])
        dag2 = DAG(dag_id="TEST_DAG_2", schedule=None, tags=["t2"])
        dag3 = DAG(dag_id="TEST_DAG_3", schedule=None, tags=["t1", "t2"])
        dag4 = DAG(dag_id="TEST_DAG_4", schedule=None)
        dag1.sync_to_db()
        dag2.sync_to_db()
        dag3.sync_to_db()
        dag4.sync_to_db()
        response = self.client.patch(
            url,
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        dag_ids = [dag["dag_id"] for dag in response.json["dags"]]

        assert expected_dag_ids == dag_ids

    @pytest.mark.parametrize(
        "url, expected_dag_ids",
        [
            ("api/v1/dags?dag_id_pattern=DAG_1", {"TEST_DAG_1", "SAMPLE_DAG_1"}),
            ("api/v1/dags?dag_id_pattern=SAMPLE_DAG", {"SAMPLE_DAG_1", "SAMPLE_DAG_2"}),
            (
                "api/v1/dags?dag_id_pattern=_DAG_",
                {"TEST_DAG_1", "TEST_DAG_2", "SAMPLE_DAG_1", "SAMPLE_DAG_2"},
            ),
        ],
    )
    def test_filter_dags_by_dag_id_works(self, url, expected_dag_ids):
        # test filter by tags
        dag1 = DAG(dag_id="TEST_DAG_1", schedule=None)
        dag2 = DAG(dag_id="TEST_DAG_2", schedule=None)
        dag3 = DAG(dag_id="SAMPLE_DAG_1", schedule=None)
        dag4 = DAG(dag_id="SAMPLE_DAG_2", schedule=None)
        dag1.sync_to_db()
        dag2.sync_to_db()
        dag3.sync_to_db()
        dag4.sync_to_db()

        response = self.client.patch(
            url,
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        dag_ids = {dag["dag_id"] for dag in response.json["dags"]}

        assert expected_dag_ids == dag_ids

    @pytest.mark.parametrize(
        "url, expected_dag_ids",
        [
            ("api/v1/dags?limit=1&dag_id_pattern=~", ["TEST_DAG_1"]),
            ("api/v1/dags?limit=2&dag_id_pattern=~", ["TEST_DAG_1", "TEST_DAG_10"]),
            (
                "api/v1/dags?offset=5&dag_id_pattern=~",
                ["TEST_DAG_5", "TEST_DAG_6", "TEST_DAG_7", "TEST_DAG_8", "TEST_DAG_9"],
            ),
            (
                "api/v1/dags?offset=0&dag_id_pattern=~",
                [
                    "TEST_DAG_1",
                    "TEST_DAG_10",
                    "TEST_DAG_2",
                    "TEST_DAG_3",
                    "TEST_DAG_4",
                    "TEST_DAG_5",
                    "TEST_DAG_6",
                    "TEST_DAG_7",
                    "TEST_DAG_8",
                    "TEST_DAG_9",
                ],
            ),
            ("api/v1/dags?limit=1&offset=5&dag_id_pattern=~", ["TEST_DAG_5"]),
            ("api/v1/dags?limit=1&offset=1&dag_id_pattern=~", ["TEST_DAG_10"]),
            ("api/v1/dags?limit=2&offset=2&dag_id_pattern=~", ["TEST_DAG_2", "TEST_DAG_3"]),
        ],
    )
    def test_should_respond_200_and_handle_pagination(self, url, expected_dag_ids):
        self._create_dag_models(10)

        response = self.client.patch(
            url,
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 200

        dag_ids = [dag["dag_id"] for dag in response.json["dags"]]

        assert expected_dag_ids == dag_ids
        assert 10 == response.json["total_entries"]

    def test_should_respond_200_default_limit(self):
        self._create_dag_models(101)

        response = self.client.patch(
            "api/v1/dags?dag_id_pattern=~",
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 200

        assert 100 == len(response.json["dags"])
        assert 101 == response.json["total_entries"]

    def test_should_raises_401_unauthenticated(self):
        response = self.client.patch(
            "api/v1/dags?dag_id_pattern=~",
            json={
                "is_paused": False,
            },
        )

        assert_401(response)

    def test_should_respond_403_unauthorized(self):
        self._create_dag_models(1)
        response = self.client.patch(
            "api/v1/dags?dag_id_pattern=~",
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )

        assert response.status_code == 403

    def test_should_respond_200_and_pause_dags(self, url_safe_serializer):
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        file_token2 = url_safe_serializer.dumps("/tmp/dag_2.py")
        self._create_dag_models(2)

        response = self.client.patch(
            "/api/v1/dags?dag_id_pattern=~",
            json={
                "is_paused": True,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_1",
                    "dag_display_name": "TEST_DAG_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": True,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
                {
                    "dag_id": "TEST_DAG_2",
                    "dag_display_name": "TEST_DAG_2",
                    "description": None,
                    "fileloc": "/tmp/dag_2.py",
                    "file_token": file_token2,
                    "is_paused": True,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
            ],
            "total_entries": 2,
        } == response.json

    @provide_session
    def test_should_respond_200_and_pause_dag_pattern(self, session, url_safe_serializer):
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        self._create_dag_models(10)
        file_token10 = url_safe_serializer.dumps("/tmp/dag_10.py")

        response = self.client.patch(
            "/api/v1/dags?dag_id_pattern=TEST_DAG_1",
            json={
                "is_paused": True,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_1",
                    "dag_display_name": "TEST_DAG_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": True,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
                {
                    "dag_id": "TEST_DAG_10",
                    "dag_display_name": "TEST_DAG_10",
                    "description": None,
                    "fileloc": "/tmp/dag_10.py",
                    "file_token": file_token10,
                    "is_paused": True,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
            ],
            "total_entries": 2,
        } == response.json

        dags_not_updated = session.query(DagModel).filter(~DagModel.is_paused)
        assert len(dags_not_updated.all()) == 8
        dags_updated = session.query(DagModel).filter(DagModel.is_paused)
        assert len(dags_updated.all()) == 2

    @provide_session
    def test_should_respond_200_and_reverse_ordering(self, session, url_safe_serializer):
        file_token = url_safe_serializer.dumps("/tmp/dag_1.py")
        self._create_dag_models(2)
        file_token10 = url_safe_serializer.dumps("/tmp/dag_2.py")

        response = self.client.get(
            "/api/v1/dags?order_by=-dag_id",
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 200
        assert {
            "dags": [
                {
                    "dag_id": "TEST_DAG_2",
                    "dag_display_name": "TEST_DAG_2",
                    "description": None,
                    "fileloc": "/tmp/dag_2.py",
                    "file_token": file_token10,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
                {
                    "dag_id": "TEST_DAG_1",
                    "dag_display_name": "TEST_DAG_1",
                    "description": None,
                    "fileloc": "/tmp/dag_1.py",
                    "file_token": file_token,
                    "is_paused": False,
                    "is_active": True,
                    "owners": [],
                    "timetable_summary": "2 2 * * *",
                    "tags": [],
                    "next_dagrun": None,
                    "has_task_concurrency_limits": True,
                    "next_dagrun_data_interval_start": None,
                    "next_dagrun_data_interval_end": None,
                    "max_active_runs": 16,
                    "max_consecutive_failed_dag_runs": 0,
                    "next_dagrun_create_after": None,
                    "last_expired": None,
                    "max_active_tasks": 16,
                    "last_pickled": None,
                    "default_view": None,
                    "last_parsed_time": None,
                    "scheduler_lock": None,
                    "timetable_description": None,
                    "has_import_errors": False,
                    "pickle_id": None,
                },
            ],
            "total_entries": 2,
        } == response.json

    def test_should_respons_400_dag_id_pattern_missing(self):
        self._create_dag_models(1)
        response = self.client.patch(
            "/api/v1/dags?only_active=True",
            json={
                "is_paused": False,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400


class TestDeleteDagEndpoint(TestDagEndpoint):
    def test_that_dag_can_be_deleted(self, session):
        self._create_dag_models(1)

        response = self.client.delete(
            "/api/v1/dags/TEST_DAG_1",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 204
        _check_last_log(session, dag_id="TEST_DAG_1", event="api.delete_dag", execution_date=None)

    def test_raise_when_dag_is_not_found(self):
        response = self.client.delete(
            "/api/v1/dags/TEST_DAG_1",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        assert response.json == {
            "detail": None,
            "status": 404,
            "title": "Dag with id: 'TEST_DAG_1' not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        }

    def test_raises_when_task_instances_of_dag_is_still_running(self, dag_maker, session):
        with dag_maker("TEST_DAG_1"):
            EmptyOperator(task_id="dummy")
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instances()[0]
        ti.set_state(TaskInstanceState.RUNNING)
        session.flush()
        response = self.client.delete(
            "/api/v1/dags/TEST_DAG_1",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 409
        assert response.json == {
            "detail": "Task instances of dag with id: 'TEST_DAG_1' are still running",
            "status": 409,
            "title": "Conflict",
            "type": EXCEPTIONS_LINK_MAP[409],
        }

    def test_users_without_delete_permission_cannot_delete_dag(self):
        self._create_dag_models(1)
        response = self.client.delete(
            "/api/v1/dags/TEST_DAG_1",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403
