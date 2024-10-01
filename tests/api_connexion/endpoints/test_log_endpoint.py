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

import copy
import logging.config
import sys
from unittest import mock
from unittest.mock import PropertyMock

import pytest
from itsdangerous.url_safe import URLSafeSerializer

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.types import DagRunType
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.db import clear_db_runs

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api

    create_user(
        app,
        username="test",
        role_name="admin",
    )
    create_user(app, username="test_no_permissions", role_name=None)

    yield app

    delete_user(app, username="test")
    delete_user(app, username="test_no_permissions")


class TestGetLog:
    DAG_ID = "dag_for_testing_log_endpoint"
    RUN_ID = "dag_run_id_for_testing_log_endpoint"
    TASK_ID = "task_for_testing_log_endpoint"
    MAPPED_TASK_ID = "mapped_task_for_testing_log_endpoint"
    TRY_NUMBER = 1

    default_time = "2020-06-10T20:00:00+00:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app, configure_loggers, dag_maker, session) -> None:
        self.app = configured_app
        self.client = self.app.test_client()
        # Make sure that the configure_logging is not cached
        self.old_modules = dict(sys.modules)

        with dag_maker(self.DAG_ID, start_date=timezone.parse(self.default_time), session=session) as dag:
            EmptyOperator(task_id=self.TASK_ID)

            @task(task_id=self.MAPPED_TASK_ID)
            def add_one(x: int):
                return x + 1

            add_one.expand(x=[1, 2, 3])

        dr = dag_maker.create_dagrun(
            run_id=self.RUN_ID,
            run_type=DagRunType.SCHEDULED,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
        )

        configured_app.dag_bag.bag_dag(dag)

        # Add dummy dag for checking picking correct log with same task_id and different dag_id case.
        with dag_maker(
            f"{self.DAG_ID}_copy", start_date=timezone.parse(self.default_time), session=session
        ) as dummy_dag:
            EmptyOperator(task_id=self.TASK_ID)
        dr2 = dag_maker.create_dagrun(
            run_id=self.RUN_ID,
            run_type=DagRunType.SCHEDULED,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
        )
        configured_app.dag_bag.bag_dag(dummy_dag)

        for ti in dr.task_instances:
            ti.try_number = 1
            ti.hostname = "localhost"
            session.merge(ti)
        for ti in dr2.task_instances:
            ti.try_number = 1
            ti.hostname = "localhost"
            session.merge(ti)
        session.flush()
        dag.clear()
        dummy_dag.clear()
        for ti in dr.task_instances:
            ti.try_number = 2
            ti.hostname = "localhost"
            session.merge(ti)
        for ti in dr2.task_instances:
            ti.try_number = 2
            ti.hostname = "localhost"
            session.merge(ti)
        session.flush()

    @pytest.fixture
    def configure_loggers(self, tmp_path, create_log_template):
        self.log_dir = tmp_path

        # TASK_ID
        dir_path = tmp_path / f"dag_id={self.DAG_ID}" / f"run_id={self.RUN_ID}" / f"task_id={self.TASK_ID}"
        dir_path.mkdir(parents=True)

        log = dir_path / "attempt=1.log"
        log.write_text("Log for testing.")

        # try number 2
        log = dir_path / "attempt=2.log"
        log.write_text("Log for testing 2.")

        # MAPPED_TASK_ID
        for map_index in range(3):
            dir_path = (
                tmp_path
                / f"dag_id={self.DAG_ID}"
                / f"run_id={self.RUN_ID}"
                / f"task_id={self.MAPPED_TASK_ID}"
                / f"map_index={map_index}"
            )

            dir_path.mkdir(parents=True)

            log = dir_path / "attempt=1.log"
            log.write_text("Log for testing.")

            # try number 2
            log = dir_path / "attempt=2.log"
            log.write_text("Log for testing 2.")

        # Create a custom logging configuration
        logging_config = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
        logging_config["handlers"]["task"]["base_log_folder"] = self.log_dir

        logging.config.dictConfig(logging_config)

        yield

        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)

    def teardown_method(self):
        clear_db_runs()

    @pytest.mark.parametrize("try_number", [1, 2])
    def test_should_respond_200_json(self, try_number):
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": False})
        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/{try_number}",
            query_string={"token": token},
            headers={"Accept": "application/json"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        expected_filename = f"{self.log_dir}/dag_id={self.DAG_ID}/run_id={self.RUN_ID}/task_id={self.TASK_ID}/attempt={try_number}.log"
        log_content = "Log for testing." if try_number == 1 else "Log for testing 2."
        assert (
            response.json["content"]
            == f"[('localhost', '*** Found local files:\\n***   * {expected_filename}\\n{log_content}')]"
        )
        info = serializer.loads(response.json["continuation_token"])
        assert info == {"end_of_log": True, "log_pos": 16 if try_number == 1 else 18}
        assert 200 == response.status_code

    @pytest.mark.parametrize(
        "request_url, expected_filename, extra_query_string, try_number",
        [
            (
                f"api/v1/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}/logs/1",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={TASK_ID}/attempt=1.log",
                {},
                1,
            ),
            (
                f"api/v1/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{MAPPED_TASK_ID}/logs/1",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={MAPPED_TASK_ID}/map_index=0/attempt=1.log",
                {"map_index": 0},
                1,
            ),
            # try_number 2
            (
                f"api/v1/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}/logs/2",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={TASK_ID}/attempt=2.log",
                {},
                2,
            ),
            (
                f"api/v1/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{MAPPED_TASK_ID}/logs/2",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={MAPPED_TASK_ID}/map_index=0/attempt=2.log",
                {"map_index": 0},
                2,
            ),
        ],
    )
    def test_should_respond_200_text_plain(
        self, request_url, expected_filename, extra_query_string, try_number
    ):
        expected_filename = expected_filename.replace("LOG_DIR", str(self.log_dir))

        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            request_url,
            query_string={"token": token, **extra_query_string},
            headers={"Accept": "text/plain"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert 200 == response.status_code

        log_content = "Log for testing." if try_number == 1 else "Log for testing 2."

        assert (
            response.data.decode("utf-8")
            == f"localhost\n*** Found local files:\n***   * {expected_filename}\n{log_content}\n"
        )

    @pytest.mark.parametrize(
        "request_url, expected_filename, extra_query_string, try_number",
        [
            (
                f"api/v1/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}/logs/1",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={TASK_ID}/attempt=1.log",
                {},
                1,
            ),
            (
                f"api/v1/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{MAPPED_TASK_ID}/logs/1",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={MAPPED_TASK_ID}/map_index=0/attempt=1.log",
                {"map_index": 0},
                1,
            ),
            (
                f"api/v1/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}/logs/2",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={TASK_ID}/attempt=2.log",
                {},
                2,
            ),
            (
                f"api/v1/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{MAPPED_TASK_ID}/logs/2",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={MAPPED_TASK_ID}/map_index=0/attempt=2.log",
                {"map_index": 0},
                2,
            ),
        ],
    )
    def test_get_logs_of_removed_task(self, request_url, expected_filename, extra_query_string, try_number):
        expected_filename = expected_filename.replace("LOG_DIR", str(self.log_dir))

        # Recreate DAG without tasks
        dagbag = self.app.dag_bag
        dag = DAG(self.DAG_ID, schedule=None, start_date=timezone.parse(self.default_time))
        del dagbag.dags[self.DAG_ID]
        dagbag.bag_dag(dag=dag)

        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            request_url,
            query_string={"token": token, **extra_query_string},
            headers={"Accept": "text/plain"},
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert 200 == response.status_code

        log_content = "Log for testing." if try_number == 1 else "Log for testing 2."
        assert (
            response.data.decode("utf-8")
            == f"localhost\n*** Found local files:\n***   * {expected_filename}\n{log_content}\n"
        )

    @pytest.mark.parametrize("try_number", [1, 2])
    def test_get_logs_response_with_ti_equal_to_none(self, try_number):
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/Invalid-Task-ID/logs/{try_number}",
            query_string={"token": token},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        assert response.json == {
            "detail": None,
            "status": 404,
            "title": "TaskInstance not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        }

    @pytest.mark.parametrize("try_number", [1, 2])
    def test_get_logs_with_metadata_as_download_large_file(self, try_number):
        with mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read") as read_mock:
            first_return = ([[("", "1st line")]], [{}])
            second_return = ([[("", "2nd line")]], [{"end_of_log": False}])
            third_return = ([[("", "3rd line")]], [{"end_of_log": True}])
            fourth_return = ([[("", "should never be read")]], [{"end_of_log": True}])
            read_mock.side_effect = [first_return, second_return, third_return, fourth_return]

            response = self.client.get(
                f"api/v1/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/"
                f"taskInstances/{self.TASK_ID}/logs/{try_number}?full_content=True",
                headers={"Accept": "text/plain"},
                environ_overrides={"REMOTE_USER": "test"},
            )

            assert "1st line" in response.data.decode("utf-8")
            assert "2nd line" in response.data.decode("utf-8")
            assert "3rd line" in response.data.decode("utf-8")
            assert "should never be read" not in response.data.decode("utf-8")

    @pytest.mark.parametrize("try_number", [1, 2])
    @mock.patch("airflow.api_connexion.endpoints.log_endpoint.TaskLogReader")
    def test_get_logs_for_handler_without_read_method(self, mock_log_reader, try_number):
        type(mock_log_reader.return_value).supports_read = PropertyMock(return_value=False)

        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": False})

        # check guessing
        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/{try_number}",
            query_string={"token": token},
            headers={"Content-Type": "application/jso"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert 400 == response.status_code
        assert "Task log handler does not support read logs." in response.data.decode("utf-8")

    def test_bad_signature_raises(self):
        token = {"download_logs": False}

        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/1",
            query_string={"token": token},
            headers={"Accept": "application/json"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.json == {
            "detail": None,
            "status": 400,
            "title": "Bad Signature. Please use only the tokens provided by the API.",
            "type": EXCEPTIONS_LINK_MAP[400],
        }

    def test_raises_404_for_invalid_dag_run_id(self):
        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/NO_DAG_RUN/"  # invalid run_id
            f"taskInstances/{self.TASK_ID}/logs/1?",
            headers={"Accept": "application/json"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        assert response.json == {
            "detail": None,
            "status": 404,
            "title": "TaskInstance not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        }

    def test_should_raises_401_unauthenticated(self):
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": False})

        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/1",
            query_string={"token": token},
            headers={"Accept": "application/json"},
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/1",
            query_string={"token": token},
            headers={"Accept": "text/plain"},
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403

    def test_should_raise_404_when_missing_map_index_param_for_mapped_task(self):
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.MAPPED_TASK_ID}/logs/1",
            query_string={"token": token},
            headers={"Accept": "text/plain"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        assert response.json["title"] == "TaskInstance not found"

    def test_should_raise_404_when_filtering_on_map_index_for_unmapped_task(self):
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/1",
            query_string={"token": token, "map_index": 0},
            headers={"Accept": "text/plain"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        assert response.json["title"] == "TaskInstance not found"
