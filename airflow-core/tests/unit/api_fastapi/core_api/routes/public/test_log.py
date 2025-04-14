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
from uuid6 import uuid7

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models.dag import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task
from airflow.utils import timezone
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


class TestTaskInstancesLog:
    DAG_ID = "dag_for_testing_log_endpoint"
    RUN_ID = "dag_run_id_for_testing_log_endpoint"
    TASK_ID = "task_for_testing_log_endpoint"
    MAPPED_TASK_ID = "mapped_task_for_testing_log_endpoint"
    TRY_NUMBER = 1

    default_time = "2020-06-10T20:00:00+00:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, test_client, configure_loggers, dag_maker, session) -> None:
        self.app = test_client.app
        self.client = test_client
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
            logical_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
        )

        self.app.state.dag_bag.bag_dag(dag)

        for ti in dr.task_instances:
            ti.try_number = 1
            ti.hostname = "localhost"
            session.merge(ti)
        dag.clear()
        for ti in dr.task_instances:
            ti.try_number = 2
            ti.id = str(uuid7())
            ti.hostname = "localhost"
            session.merge(ti)
            session.flush()

        # Add dummy dag for checking picking correct log with same task_id and different dag_id case.
        with dag_maker(
            f"{self.DAG_ID}_copy", start_date=timezone.parse(self.default_time), session=session
        ) as dummy_dag:
            EmptyOperator(task_id=self.TASK_ID)
        dr2 = dag_maker.create_dagrun(
            run_id=self.RUN_ID,
            run_type=DagRunType.SCHEDULED,
            logical_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
        )
        self.app.state.dag_bag.bag_dag(dummy_dag)

        for ti in dr2.task_instances:
            ti.try_number = 1
            ti.hostname = "localhost"
            session.merge(ti)
        dummy_dag.clear()
        for ti in dr2.task_instances:
            ti.try_number = 2
            ti.id = str(uuid7())
            ti.hostname = "localhost"
            session.merge(ti)
            session.flush()
        session.flush()

        ...

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
        key = self.app.state.secret_key
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": False})
        response = self.client.get(
            f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/{try_number}",
            params={"token": token},
            headers={"Accept": "application/json"},
        )
        expected_filename = f"{self.log_dir}/dag_id={self.DAG_ID}/run_id={self.RUN_ID}/task_id={self.TASK_ID}/attempt={try_number}.log"
        log_content = "Log for testing." if try_number == 1 else "Log for testing 2."
        assert response.status_code == 200, response.json()
        resp_contnt = response.json()["content"]
        assert expected_filename in resp_contnt[0]["sources"]
        assert log_content in resp_contnt[2]["event"]

        assert response.json()["continuation_token"] is None
        assert response.status_code == 200

    @pytest.mark.parametrize(
        "request_url, expected_filename, extra_query_string, try_number",
        [
            (
                f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}/logs/1",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={TASK_ID}/attempt=1.log",
                {},
                1,
            ),
            (
                f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{MAPPED_TASK_ID}/logs/1",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={MAPPED_TASK_ID}/map_index=0/attempt=1.log",
                {"map_index": 0},
                1,
            ),
            # try_number 2
            (
                f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}/logs/2",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={TASK_ID}/attempt=2.log",
                {},
                2,
            ),
            (
                f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{MAPPED_TASK_ID}/logs/2",
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

        key = self.app.state.secret_key
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            request_url,
            params={"token": token, **extra_query_string},
            headers={"Accept": "text/plain"},
        )
        assert response.status_code == 200

        log_content = "Log for testing." if try_number == 1 else "Log for testing 2."
        resp_content = response.content.decode("utf-8")

        assert expected_filename in resp_content
        assert log_content in resp_content

    @pytest.mark.parametrize(
        "request_url, expected_filename, extra_query_string, try_number",
        [
            (
                f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}/logs/1",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={TASK_ID}/attempt=1.log",
                {},
                1,
            ),
            (
                f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{MAPPED_TASK_ID}/logs/1",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={MAPPED_TASK_ID}/map_index=0/attempt=1.log",
                {"map_index": 0},
                1,
            ),
            (
                f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}/logs/2",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={TASK_ID}/attempt=2.log",
                {},
                2,
            ),
            (
                f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{MAPPED_TASK_ID}/logs/2",
                f"LOG_DIR/dag_id={DAG_ID}/run_id={RUN_ID}/task_id={MAPPED_TASK_ID}/map_index=0/attempt=2.log",
                {"map_index": 0},
                2,
            ),
        ],
    )
    def test_get_logs_of_removed_task(self, request_url, expected_filename, extra_query_string, try_number):
        expected_filename = expected_filename.replace("LOG_DIR", str(self.log_dir))

        # Recreate DAG without tasks
        dagbag = self.app.state.dag_bag
        dag = DAG(self.DAG_ID, schedule=None, start_date=timezone.parse(self.default_time))
        del dagbag.dags[self.DAG_ID]
        dagbag.bag_dag(dag=dag)

        key = self.app.state.secret_key
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            request_url,
            params={"token": token, **extra_query_string},
            headers={"Accept": "text/plain"},
        )

        assert response.status_code == 200

        log_content = "Log for testing." if try_number == 1 else "Log for testing 2."
        resp_content = response.content.decode("utf-8")
        assert expected_filename in resp_content
        assert log_content in resp_content

    @pytest.mark.parametrize("try_number", [1, 2])
    def test_get_logs_response_with_ti_equal_to_none(self, try_number):
        key = self.app.state.secret_key
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/Invalid-Task-ID/logs/{try_number}",
            params={"token": token},
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "TaskInstance not found"}

    @pytest.mark.parametrize("try_number", [1, 2])
    def test_get_logs_with_metadata_as_download_large_file(self, try_number):
        with mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read") as read_mock:
            first_return = (["", "1st line"], {})
            second_return = (["", "2nd line"], {"end_of_log": False})
            third_return = (["", "3rd line"], {"end_of_log": True})
            fourth_return = (["", "should never be read"], {"end_of_log": True})
            read_mock.side_effect = [first_return, second_return, third_return, fourth_return]

            response = self.client.get(
                f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/"
                f"taskInstances/{self.TASK_ID}/logs/{try_number}?full_content=True",
                headers={"Accept": "text/plain"},
            )

            assert "1st line" in response.content.decode("utf-8")
            assert "2nd line" in response.content.decode("utf-8")
            assert "3rd line" in response.content.decode("utf-8")
            assert "should never be read" not in response.content.decode("utf-8")

    @pytest.mark.parametrize("try_number", [1, 2])
    @mock.patch("airflow.api_fastapi.core_api.routes.public.log.TaskLogReader")
    def test_get_logs_for_handler_without_read_method(self, mock_log_reader, try_number):
        type(mock_log_reader.return_value).supports_read = PropertyMock(return_value=False)

        key = self.app.state.secret_key
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": False})

        # check guessing
        response = self.client.get(
            f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/{try_number}",
            params={"token": token},
            headers={"Content-Type": "application/jso"},
        )
        assert response.status_code == 400
        assert "Task log handler does not support read logs." in response.content.decode("utf-8")

    def test_bad_signature_raises(self):
        token = {"download_logs": False}

        response = self.client.get(
            f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/1",
            params={"token": token},
            headers={"Accept": "application/json"},
        )
        # assert response.status_code == 400
        assert response.json() == {"detail": "Bad Signature. Please use only the tokens provided by the API."}

    def test_should_raises_401_unauthenticated(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/1",
            headers={"Accept": "application/json"},
        )
        assert response.status_code == 401

    def test_should_raises_403_unauthorized(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/1",
            headers={"Accept": "application/json"},
        )
        assert response.status_code == 403

    def test_raises_404_for_invalid_dag_run_id(self):
        response = self.client.get(
            f"/dags/{self.DAG_ID}/dagRuns/NO_DAG_RUN/"  # invalid run_id
            f"taskInstances/{self.TASK_ID}/logs/1?",
            headers={"Accept": "application/json"},
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "TaskInstance not found"}

    def test_should_raise_404_when_missing_map_index_param_for_mapped_task(self):
        key = self.app.state.secret_key
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.MAPPED_TASK_ID}/logs/1",
            params={"token": token},
            headers={"Accept": "text/plain"},
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "TaskInstance not found"

    def test_should_raise_404_when_filtering_on_map_index_for_unmapped_task(self):
        key = self.app.state.secret_key
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/1",
            params={"token": token, "map_index": 0},
            headers={"Accept": "text/plain"},
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "TaskInstance not found"
