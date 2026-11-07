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

import json
import sys
from unittest import mock
from unittest.mock import PropertyMock
from uuid import uuid4

import pytest
from itsdangerous.url_safe import URLSafeSerializer
from sqlalchemy import select, update

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.dagbag import create_dag_bag, dag_bag_from_app
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.dynamic_region import DynamicRegion
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task
from airflow.utils.log.logging_mixin import ExternalLoggingMixin
from airflow.utils.state import TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_runs
from tests_common.test_utils.file_task_handler import convert_list_to_stream

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


class TestTaskInstancesLog:
    DAG_ID = "dag_for_testing_log_endpoint"
    RUN_ID = "dag_run_id_for_testing_log_endpoint"
    TASK_ID = "task_for_testing_log_endpoint"
    MAPPED_TASK_ID = "mapped_task_for_testing_log_endpoint"
    TRY_NUMBER = 1

    default_time = "2020-06-10T20:00:00+00:00"

    def _place_tries_in_sibling_regions(self, session):
        history = session.scalar(
            select(TaskInstance)
            .where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id == self.TASK_ID,
                TaskInstance.try_number == 1,
            )
            .execution_options(include_all_attempts=True)
        )
        current = session.scalar(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id == self.TASK_ID,
                TaskInstance.working_set.is_(True),
            )
        )
        previous_region, current_region = uuid4(), uuid4()
        for region_id, source in ((previous_region, None), (current_region, previous_region)):
            session.add(
                DynamicRegion(
                    id=region_id,
                    dag_id=self.DAG_ID,
                    run_id=self.RUN_ID,
                    node_id="loop",
                    forked_from_region_id=source,
                )
            )
        history.region_id, history.region_index = previous_region, 2
        current.region_id, current.region_index, current.try_number = current_region, 2, 2
        session.commit()
        return history, current, previous_region, current_region

    def test_external_link_uses_the_selected_archived_try(self, session):
        history, current, previous_region, _ = self._place_tries_in_sibling_regions(session)
        with mock.patch(
            "airflow.api_fastapi.core_api.routes.public.log.TaskLogReader", autospec=True
        ) as reader:
            reader.return_value.supports_external_link = True
            reader.return_value.log_handler = mock.create_autospec(ExternalLoggingMixin, instance=True)
            reader.return_value.log_handler.get_external_log_url.return_value = "https://logs.example/old"
            response = self.client.get(
                f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/externalLogUrl/1",
                params={"region_id": str(previous_region), "region_index": 2},
            )
            assert response.status_code == 200
            selected, try_number = reader.return_value.log_handler.get_external_log_url.call_args.args
            assert selected.id == history.id
            assert selected.id != current.id
            assert try_number == 1

    @pytest.mark.parametrize(
        ("requested_try", "expected_try"), [(1, "history"), (2, "current")], ids=["history", "current"]
    )
    def test_external_link_selects_the_requested_try_within_a_region(
        self, session, requested_try, expected_try
    ):
        history, current, previous_region, _ = self._place_tries_in_sibling_regions(session)
        current.region_id = previous_region
        session.commit()
        expected = history if expected_try == "history" else current
        with mock.patch(
            "airflow.api_fastapi.core_api.routes.public.log.TaskLogReader", autospec=True
        ) as reader:
            reader.return_value.supports_external_link = True
            reader.return_value.log_handler = mock.create_autospec(ExternalLoggingMixin, instance=True)
            reader.return_value.log_handler.get_external_log_url.return_value = "https://logs.example/try"
            response = self.client.get(
                f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/externalLogUrl/{requested_try}",
                params={"region_id": str(previous_region), "region_index": 2},
            )
            assert response.status_code == 200, response.text
            selected, try_number = reader.return_value.log_handler.get_external_log_url.call_args.args
            assert selected.id == expected.id
            assert try_number == requested_try

    @pytest.mark.parametrize("selected_try", ["history", "current"])
    def test_log_reads_the_execution_in_the_selected_region(self, session, selected_try):
        history, current, previous_region, current_region = self._place_tries_in_sibling_regions(session)
        current.try_number = 1
        session.commit()
        expected, region = (
            (history, previous_region) if selected_try == "history" else (current, current_region)
        )
        with mock.patch(
            "airflow.api_fastapi.core_api.routes.public.log.TaskLogReader", autospec=True
        ) as reader:
            reader.return_value.supports_read = True
            reader.return_value.read_log_chunks.return_value = ([], {"end_of_log": True})
            response = self.client.get(
                f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/1",
                params={"region_id": str(region), "region_index": 2},
                headers={"Accept": "application/json"},
            )
            assert response.status_code == 200, response.text
            selected = reader.return_value.read_log_chunks.call_args.args[0]
            assert selected.id == expected.id

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

        for ti in dr.task_instances:
            ti.try_number = 1
            ti.state = TaskInstanceState.FAILED
            ti.hostname = "localhost"
            session.merge(ti)
        dag.clear()
        # Commit changes to avoid locks
        session.commit()

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

        for ti in dr2.task_instances:
            ti.try_number = 1
            ti.state = TaskInstanceState.FAILED
            ti.hostname = "localhost"
            session.merge(ti)
        dummy_dag.clear()

        # Final commit to ensure all changes are persisted
        session.commit()

        dagbag = create_dag_bag()
        test_client.app.dependency_overrides[dag_bag_from_app] = lambda: dagbag

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

        with mock.patch(
            "airflow.utils.log.file_task_handler.FileTaskHandler.local_base",
            new_callable=mock.PropertyMock,
            create=True,
        ) as local_base:
            local_base.return_value = self.log_dir
            yield

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
        resp_content = response.json()["content"]
        assert expected_filename in resp_content[1]["event"]
        assert log_content in resp_content[3]["event"]

        assert response.json()["continuation_token"] is None
        assert response.status_code == 200

    @pytest.mark.parametrize(
        ("request_url", "expected_filename", "extra_query_string", "try_number"),
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
    def test_should_respond_200_ndjson(self, request_url, expected_filename, extra_query_string, try_number):
        expected_filename = expected_filename.replace("LOG_DIR", str(self.log_dir))

        key = self.app.state.secret_key
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            request_url,
            params={"token": token, **extra_query_string},
            headers={"Accept": "application/x-ndjson"},
        )
        assert response.status_code == 200

        log_content = "Log for testing." if try_number == 1 else "Log for testing 2."
        resp_content = response.content.decode("utf-8")

        assert expected_filename in resp_content
        assert log_content in resp_content

        # check content is in ndjson format
        for line in resp_content.splitlines():
            log = json.loads(line)
            assert "event" in log
            assert "timestamp" in log

    @pytest.mark.parametrize(
        ("request_url", "expected_filename", "extra_query_string", "try_number"),
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
        dagbag = create_dag_bag()
        dag = DAG(self.DAG_ID, schedule=None, start_date=timezone.parse(self.default_time))
        sync_dag_to_db(dag)

        self.app.dependency_overrides[dag_bag_from_app] = lambda: dagbag

        key = self.app.state.secret_key
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            request_url,
            params={"token": token, **extra_query_string},
            headers={"Accept": "application/x-ndjson"},
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
        from airflow.utils.log.file_task_handler import StructuredLogMessage

        with mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read") as read_mock:
            first_return = (convert_list_to_stream([StructuredLogMessage(event="", message="1st line")]), {})
            second_return = (
                convert_list_to_stream([StructuredLogMessage(event="", message="2nd line")]),
                {"end_of_log": False},
            )
            third_return = (
                convert_list_to_stream([StructuredLogMessage(event="", message="3rd line")]),
                {"end_of_log": True},
            )
            fourth_return = (
                convert_list_to_stream([StructuredLogMessage(event="", message="should never be read")]),
                {"end_of_log": True},
            )
            read_mock.side_effect = [first_return, second_return, third_return, fourth_return]

            response = self.client.get(
                f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/"
                f"taskInstances/{self.TASK_ID}/logs/{try_number}?full_content=True",
                headers={"Accept": "application/x-ndjson"},
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

    @mock.patch("airflow.api_fastapi.core_api.routes.public.log.TaskLogReader", autospec=True)
    def test_get_logs_resolves_task_for_unversioned_run(self, mock_log_reader, session):
        reader = mock_log_reader.return_value
        reader.supports_read = True
        reader.read_log_chunks.return_value = (iter([]), {"end_of_log": True})
        session.execute(
            update(TaskInstance).where(TaskInstance.dag_id == self.DAG_ID).values(dag_version_id=None)
        )
        session.execute(
            update(DagRun).where(DagRun.dag_id == self.DAG_ID).values(created_dag_version_id=None)
        )
        session.commit()

        response = self.client.get(
            f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/1",
            headers={"Accept": "application/json"},
        )

        assert response.status_code == 200, response.json()
        assert reader.read_log_chunks.call_args.args[0].task.task_id == self.TASK_ID

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
            headers={"Accept": "application/x-ndjson"},
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "Task instance not found for selected coordinates"

    def test_should_raise_404_when_filtering_on_map_index_for_unmapped_task(self):
        key = self.app.state.secret_key
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{self.TASK_ID}/logs/1",
            params={"token": token, "map_index": 0},
            headers={"Accept": "application/x-ndjson"},
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "TaskInstance not found"

    @pytest.mark.parametrize(
        ("supports_external_link", "task_id", "expected_status", "expected_response", "mock_external_url"),
        [
            (
                True,
                "task_for_testing_log_endpoint",
                200,
                {"url": "https://external-logs.example.com/log/123"},
                True,
            ),
            (
                False,
                "task_for_testing_log_endpoint",
                400,
                {"detail": "Task log handler does not support external logs."},
                False,
            ),
            (True, "INVALID_TASK", 404, {"detail": "TaskInstance not found"}, False),
        ],
        ids=[
            "external_links_supported_task_exists",
            "external_links_not_supported",
            "external_links_supported_task_not_found",
        ],
    )
    def test_get_external_log_url(
        self, supports_external_link, task_id, expected_status, expected_response, mock_external_url
    ):
        with (
            mock.patch(
                "airflow.utils.log.log_reader.TaskLogReader.supports_external_link",
                new_callable=mock.PropertyMock,
                return_value=supports_external_link,
            ),
            mock.patch("airflow.utils.log.log_reader.TaskLogReader.log_handler") as mock_log_handler,
        ):
            url = f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskInstances/{task_id}/externalLogUrl/{self.TRY_NUMBER}"
            if mock_external_url:
                mock_log_handler.get_external_log_url.return_value = (
                    "https://external-logs.example.com/log/123"
                )

            response = self.client.get(url)

            if expected_status == 200:
                mock_log_handler.get_external_log_url.assert_called_once()
            else:
                mock_log_handler.get_external_log_url.assert_not_called()

            assert response.status_code == expected_status
            assert response.json() == expected_response
