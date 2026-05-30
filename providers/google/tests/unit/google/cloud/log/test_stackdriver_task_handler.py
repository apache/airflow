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

import logging
from contextlib import nullcontext
from pathlib import Path
from unittest import mock
from unittest.mock import PropertyMock
from urllib.parse import parse_qs, urlsplit

import pytest
from google.cloud.logging import Resource
from google.cloud.logging_v2.types import ListLogEntriesRequest, ListLogEntriesResponse, LogEntry

from airflow.providers.google.cloud.log.stackdriver_task_handler import (
    StackdriverRemoteLogIO,
    StackdriverTaskHandler,
)
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS


def _create_list_log_entries_response_mock(messages, token):
    return ListLogEntriesResponse(
        entries=[LogEntry(json_payload={"message": message}) for message in messages], next_page_token=token
    )


@pytest.fixture
def clean_stackdriver_handlers():
    yield
    for handler_ref in reversed(logging._handlerList[:]):
        handler = handler_ref()
        if isinstance(handler, StackdriverTaskHandler):
            logging._removeHandlerRef(handler_ref)
            del handler


class TestStackdriverRemoteLogIO:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        self.local_log_location = str(tmp_path / "local/stackdriver/logs")
        self.io = StackdriverRemoteLogIO(
            base_log_folder=self.local_log_location,
            gcp_key_path="KEY_PATH",
            gcp_log_name="airflow",
            delete_local_copy=True,
        )

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_read_logs(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock(["MSG1", "MSG2"], None)]
        )
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        ti = mock.MagicMock()
        ti.task_id = "test_task"
        ti.dag_id = "test_dag"
        ti.try_number = 1
        if AIRFLOW_V_3_0_PLUS:
            ti.logical_date = timezone.datetime(2016, 1, 1)
        else:
            ti.execution_date = timezone.datetime(2016, 1, 1)

        messages, logs = self.io.read("dag_id=test_dag/run_id=run1/task_id=test_task/attempt=1.log", ti)

        assert len(messages) == 1
        assert "Stackdriver" in messages[0]
        assert logs == ["MSG1\nMSG2"]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_read_logs_empty(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock([], None)]
        )
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        ti = mock.MagicMock()
        ti.task_id = "test_task"
        ti.dag_id = "test_dag"
        ti.try_number = 1
        if AIRFLOW_V_3_0_PLUS:
            ti.logical_date = timezone.datetime(2016, 1, 1)
        else:
            ti.execution_date = timezone.datetime(2016, 1, 1)

        messages, logs = self.io.read("test/path", ti)

        assert len(messages) == 1
        assert logs == []

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
    def test_credentials(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        _ = self.io._client

        mock_get_creds_and_project_id.assert_called_once_with(
            disable_logging=True,
            key_path="KEY_PATH",
            scopes=frozenset(
                {
                    "https://www.googleapis.com/auth/logging.write",
                    "https://www.googleapis.com/auth/logging.read",
                }
            ),
        )
        mock_client.assert_called_once_with(credentials="creds", client_info=mock.ANY, project="project_id")

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
    def test_transport_init(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        transport_type = mock.MagicMock()
        io = StackdriverRemoteLogIO(
            base_log_folder=self.local_log_location,
            gcp_log_name="test-log",
            transport_type=transport_type,
        )
        _ = io.transport
        transport_type.assert_called_once_with(mock_client.return_value, "test-log")

    @mock.patch("shutil.rmtree")
    @mock.patch(
        "airflow.providers.google.cloud.log.stackdriver_task_handler.StackdriverRemoteLogIO.transport",
        new_callable=PropertyMock,
    )
    def test_upload_flushes_transport_and_deletes_local(self, mock_transport_prop, mock_rmtree):
        io = StackdriverRemoteLogIO(
            base_log_folder=self.local_log_location,
            gcp_log_name="airflow",
            delete_local_copy=True,
        )
        mock_transport = mock.MagicMock()
        mock_transport_prop.return_value = mock_transport

        base = Path(self.local_log_location)
        base.mkdir(parents=True, exist_ok=True)
        log_dir = base / "subdir"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "test.log"
        log_file.write_text("log content")

        ti = mock.MagicMock()
        io.upload(str(log_file), ti)

        mock_transport.flush.assert_called_once()
        mock_rmtree.assert_called_once_with(log_dir.resolve(), ignore_errors=True)

    @mock.patch(
        "airflow.providers.google.cloud.log.stackdriver_task_handler.StackdriverRemoteLogIO.transport",
        new_callable=PropertyMock,
    )
    def test_upload_no_delete(self, mock_transport_prop):
        io = StackdriverRemoteLogIO(
            base_log_folder=self.local_log_location,
            gcp_log_name="airflow",
            delete_local_copy=False,
        )
        mock_transport = mock.MagicMock()
        mock_transport_prop.return_value = mock_transport

        ti = mock.MagicMock()
        io.upload("some/path.log", ti)

        mock_transport.flush.assert_called_once()

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    def test_prepare_log_filter(self, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        ti_labels = {
            "task_id": "test_task",
            "dag_id": "test_dag",
            "try_number": "1",
        }
        log_filter = self.io.prepare_log_filter(ti_labels)

        assert 'resource.type="global"' in log_filter
        assert 'logName="projects/project_id/logs/airflow"' in log_filter
        assert 'labels.task_id="test_task"' in log_filter
        assert 'labels.dag_id="test_dag"' in log_filter

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    def test_prepare_log_filter_with_custom_resource(self, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        io = StackdriverRemoteLogIO(
            base_log_folder=self.local_log_location,
            gcp_log_name="airflow",
            resource=Resource(
                type="cloud_composer_environment",
                labels={
                    "environment.name": "test-instance",
                    "location": "europe-west-3",
                },
            ),
        )
        log_filter = io.prepare_log_filter({"task_id": "test"})

        assert 'resource.type="cloud_composer_environment"' in log_filter
        assert 'resource.labels."environment.name"="test-instance"' in log_filter
        assert 'resource.labels.location="europe-west-3"' in log_filter

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="airflow.sdk.log only exists in Airflow 3+")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
    def test_processors_sends_to_transport(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        mock_transport_type = mock.MagicMock()
        with mock.patch("airflow.sdk.log.relative_path_from_logger", return_value="dag/task/1.log"):
            io = StackdriverRemoteLogIO(
                base_log_folder=self.local_log_location,
                gcp_log_name="airflow",
                labels={"env": "test"},
                transport_type=mock_transport_type,
            )
            processors = io.processors
            assert len(processors) == 1

            proc = processors[0]
            mock_logger = mock.MagicMock()

            event = {
                "event": "hello world",
                "logger_name": "airflow.task",
                "timestamp": "2026-01-15T10:30:00+00:00",
            }
            result = proc(mock_logger, "info", event)

        assert result is event
        mock_transport = mock_transport_type.return_value
        mock_transport.send.assert_called_once()
        record = mock_transport.send.call_args[0][0]
        assert record.levelno == logging.INFO

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="airflow.sdk.log only exists in Airflow 3+")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
    def test_processors_skips_non_task_logger(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        mock_transport_type = mock.MagicMock()
        with mock.patch("airflow.sdk.log.relative_path_from_logger", return_value=None):
            io = StackdriverRemoteLogIO(
                base_log_folder=self.local_log_location,
                gcp_log_name="airflow",
                transport_type=mock_transport_type,
            )
            proc = io.processors[0]

            event = {"event": "should not be sent"}
            result = proc(mock.MagicMock(), "info", event)

        assert result is event
        mock_transport_type.return_value.send.assert_not_called()


@pytest.mark.usefixtures("clean_stackdriver_handlers")
@mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
@mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
def test_should_pass_message_to_client(mock_client, mock_get_creds_and_project_id):
    mock_get_creds_and_project_id.return_value = ("creds", "project_id")

    transport_type = mock.MagicMock()
    stackdriver_task_handler = StackdriverTaskHandler(transport=transport_type, labels={"key": "value"})
    logger = logging.getLogger("logger")
    logger.setLevel(logging.INFO)
    logger.addHandler(stackdriver_task_handler)

    logger.info("test-message")
    stackdriver_task_handler.flush()

    transport_type.assert_called_once_with(mock_client.return_value, "airflow")
    transport_type.return_value.send.assert_called_once_with(
        mock.ANY, "test-message", labels={"key": "value"}, resource=Resource(type="global", labels={})
    )
    mock_client.assert_called_once_with(credentials="creds", client_info=mock.ANY, project="project_id")


@pytest.mark.usefixtures("clean_stackdriver_handlers")
@mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
@mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
def test_should_use_configured_log_name(mock_client, mock_get_creds_and_project_id):
    import importlib

    from airflow import settings
    from airflow.config_templates import airflow_local_settings

    mock_get_creds_and_project_id.return_value = ("creds", "project_id")

    try:
        context_manager = nullcontext()
        with context_manager:
            with conf_vars(
                {
                    ("logging", "remote_logging"): "True",
                    ("logging", "remote_base_log_folder"): "stackdriver://host/path",
                }
            ):
                importlib.reload(airflow_local_settings)
                settings.configure_logging()

                task_log = getattr(airflow_local_settings, "REMOTE_TASK_LOG", None)
                if task_log is not None:
                    # Airflow 3+ uses REMOTE_TASK_LOG instead of handler-based config
                    assert isinstance(task_log, StackdriverRemoteLogIO)
                    assert task_log.gcp_log_name == "path"
                    return

                # Older Airflow: stackdriver is wired as a logging handler
                logger = logging.getLogger("airflow.task")
                handler = logger.handlers[0]
                assert isinstance(handler, StackdriverTaskHandler)
    finally:
        importlib.reload(airflow_local_settings)
        settings.configure_logging()


@pytest.mark.db_test
class TestStackdriverLoggingHandlerTask:
    DAG_ID = "dag_for_testing_stackdriver_file_task_handler"
    TASK_ID = "task_for_testing_stackdriver_task_handler"

    @pytest.fixture(autouse=True)
    def task_instance(self, create_task_instance, clean_stackdriver_handlers):
        self.ti = create_task_instance(
            dag_id=self.DAG_ID,
            task_id=self.TASK_ID,
            logical_date=timezone.datetime(2016, 1, 1),
            state=TaskInstanceState.RUNNING,
        )
        self.ti.try_number = 1
        self.ti.raw = False
        yield
        clear_db_runs()
        clear_db_dags()

    def _setup_handler(self, **handler_kwargs):
        self.transport_mock = mock.MagicMock()
        handler_kwargs = {"transport": self.transport_mock, **handler_kwargs}
        stackdriver_task_handler = StackdriverTaskHandler(**handler_kwargs)
        self.logger = logging.getLogger("logger")
        self.logger.addHandler(stackdriver_task_handler)
        return stackdriver_task_handler

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
    def test_should_set_labels(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        stackdriver_task_handler = self._setup_handler()
        stackdriver_task_handler.set_context(self.ti)

        self.logger.info("test-message")
        stackdriver_task_handler.flush()

        date_key = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        labels = {
            "task_id": self.TASK_ID,
            "dag_id": self.DAG_ID,
            date_key: "2016-01-01T00:00:00+00:00",
            "try_number": "1",
        }
        resource = Resource(type="global", labels={})
        self.transport_mock.return_value.send.assert_called_once_with(
            mock.ANY, "test-message", labels=labels, resource=resource
        )

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
    def test_should_append_labels(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        stackdriver_task_handler = self._setup_handler(
            labels={"product.googleapis.com/task_id": "test-value"},
        )
        stackdriver_task_handler.set_context(self.ti)

        self.logger.info("test-message")
        stackdriver_task_handler.flush()

        date_key = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        labels = {
            "task_id": self.TASK_ID,
            "dag_id": self.DAG_ID,
            date_key: "2016-01-01T00:00:00+00:00",
            "try_number": "1",
            "product.googleapis.com/task_id": "test-value",
        }
        resource = Resource(type="global", labels={})
        self.transport_mock.return_value.send.assert_called_once_with(
            mock.ANY, "test-message", labels=labels, resource=resource
        )

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_read_logs_for_all_try(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock(["MSG1", "MSG2"], None)]
        )
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        stackdriver_task_handler = self._setup_handler()
        logs, metadata = stackdriver_task_handler.read(self.ti)

        date_label = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"

        filter_str = (
            'resource.type="global"\n'
            'logName="projects/project_id/logs/airflow"\n'
            'labels.task_id="task_for_testing_stackdriver_task_handler"\n'
            'labels.dag_id="dag_for_testing_stackdriver_file_task_handler"\n'
            f'labels.{date_label}="2016-01-01T00:00:00+00:00"'
        )
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=filter_str,
                order_by="timestamp asc",
                page_size=1000,
                page_token=None,
            )
        )
        assert logs == [(("default-hostname", "MSG1\nMSG2"),)]
        assert metadata == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_read_logs_for_task_with_quote(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock(["MSG1", "MSG2"], None)]
        )
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        self.ti.task_id = 'K"OT'
        stackdriver_task_handler = self._setup_handler()

        logs, metadata = stackdriver_task_handler.read(self.ti)
        date_label = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        filter_str = (
            'resource.type="global"\n'
            'logName="projects/project_id/logs/airflow"\n'
            'labels.task_id="K\\"OT"\n'
            'labels.dag_id="dag_for_testing_stackdriver_file_task_handler"\n'
            f'labels.{date_label}="2016-01-01T00:00:00+00:00"'
        )
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=filter_str,
                order_by="timestamp asc",
                page_size=1000,
                page_token=None,
            )
        )
        assert logs == [(("default-hostname", "MSG1\nMSG2"),)]
        assert metadata == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_read_logs_for_single_try(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock(["MSG1", "MSG2"], None)]
        )
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")
        stackdriver_task_handler = self._setup_handler()

        logs, metadata = stackdriver_task_handler.read(self.ti, 3)
        date_label = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        filter_str = (
            'resource.type="global"\n'
            'logName="projects/project_id/logs/airflow"\n'
            'labels.task_id="task_for_testing_stackdriver_task_handler"\n'
            'labels.dag_id="dag_for_testing_stackdriver_file_task_handler"\n'
            f'labels.{date_label}="2016-01-01T00:00:00+00:00"\n'
            'labels.try_number="3"'
        )
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=filter_str,
                order_by="timestamp asc",
                page_size=1000,
                page_token=None,
            )
        )
        assert logs == [(("default-hostname", "MSG1\nMSG2"),)]
        assert metadata == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_read_logs_with_pagination(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.side_effect = [
            mock.MagicMock(pages=iter([_create_list_log_entries_response_mock(["MSG1", "MSG2"], "TOKEN1")])),
            mock.MagicMock(pages=iter([_create_list_log_entries_response_mock(["MSG3", "MSG4"], None)])),
        ]
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")
        stackdriver_task_handler = self._setup_handler()

        logs, metadata1 = stackdriver_task_handler.read(self.ti, 3)
        date_label = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        filter_str = (
            'resource.type="global"\n'
            'logName="projects/project_id/logs/airflow"\n'
            'labels.task_id="task_for_testing_stackdriver_task_handler"\n'
            'labels.dag_id="dag_for_testing_stackdriver_file_task_handler"\n'
            f'labels.{date_label}="2016-01-01T00:00:00+00:00"\n'
            'labels.try_number="3"'
        )
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=filter_str,
                order_by="timestamp asc",
                page_size=1000,
                page_token=None,
            )
        )
        assert logs == [(("default-hostname", "MSG1\nMSG2"),)]
        assert metadata1 == [{"end_of_log": False, "next_page_token": "TOKEN1"}]

        mock_client.return_value.list_log_entries.return_value.next_page_token = None
        logs, metadata2 = stackdriver_task_handler.read(self.ti, 3, metadata1[0])

        mock_client.return_value.list_log_entries.assert_called_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=(
                    'resource.type="global"\n'
                    'logName="projects/project_id/logs/airflow"\n'
                    'labels.task_id="task_for_testing_stackdriver_task_handler"\n'
                    'labels.dag_id="dag_for_testing_stackdriver_file_task_handler"\n'
                    f'labels.{date_label}="2016-01-01T00:00:00+00:00"\n'
                    'labels.try_number="3"'
                ),
                order_by="timestamp asc",
                page_size=1000,
                page_token="TOKEN1",
            )
        )
        assert logs == [(("default-hostname", "MSG3\nMSG4"),)]
        assert metadata2 == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_read_logs_with_download(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.side_effect = [
            mock.MagicMock(pages=iter([_create_list_log_entries_response_mock(["MSG1", "MSG2"], "TOKEN1")])),
            mock.MagicMock(pages=iter([_create_list_log_entries_response_mock(["MSG3", "MSG4"], None)])),
        ]
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        stackdriver_task_handler = self._setup_handler()
        logs, metadata1 = stackdriver_task_handler.read(self.ti, 3, {"download_logs": True})

        assert logs == [(("default-hostname", "MSG1\nMSG2\nMSG3\nMSG4"),)]
        assert metadata1 == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_read_logs_with_custom_resources(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")
        resource = Resource(
            type="cloud_composer_environment",
            labels={
                "environment.name": "test-instance",
                "location": "europe-west-3",
                "project_id": "project_id",
            },
        )
        stackdriver_task_handler = self._setup_handler(resource=resource)

        entry = mock.MagicMock(json_payload={"message": "TEXT"})
        page = mock.MagicMock(entries=[entry, entry], next_page_token=None)
        mock_client.return_value.list_log_entries.return_value.pages = iter([page])

        logs, metadata = stackdriver_task_handler.read(self.ti)
        date_label = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        filter_str = (
            'resource.type="cloud_composer_environment"\n'
            'logName="projects/project_id/logs/airflow"\n'
            'resource.labels."environment.name"="test-instance"\n'
            'resource.labels.location="europe-west-3"\n'
            'resource.labels.project_id="project_id"\n'
            'labels.task_id="task_for_testing_stackdriver_task_handler"\n'
            'labels.dag_id="dag_for_testing_stackdriver_file_task_handler"\n'
            f'labels.{date_label}="2016-01-01T00:00:00+00:00"'
        )
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=filter_str,
                order_by="timestamp asc",
                page_size=1000,
                page_token=None,
            )
        )
        assert logs == [(("default-hostname", "TEXT\nTEXT"),)]
        assert metadata == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
    def test_should_use_credentials(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        stackdriver_task_handler = StackdriverTaskHandler(gcp_key_path="KEY_PATH")
        client = stackdriver_task_handler.io._client

        mock_get_creds_and_project_id.assert_called_once_with(
            disable_logging=True,
            key_path="KEY_PATH",
            scopes=frozenset(
                {
                    "https://www.googleapis.com/auth/logging.write",
                    "https://www.googleapis.com/auth/logging.read",
                }
            ),
        )
        mock_client.assert_called_once_with(credentials="creds", client_info=mock.ANY, project="project_id")
        assert mock_client.return_value == client

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_return_valid_external_url(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        stackdriver_task_handler = StackdriverTaskHandler(gcp_key_path="KEY_PATH")
        url = stackdriver_task_handler.get_external_log_url(self.ti, self.ti.try_number)

        parsed_url = urlsplit(url)
        parsed_qs = parse_qs(parsed_url.query)
        assert parsed_url.scheme == "https"
        assert parsed_url.netloc == "console.cloud.google.com"
        assert parsed_url.path == "/logs/viewer"
        assert {"project", "interval", "resource", "advancedFilter"} == set(parsed_qs.keys())
        assert "global" in parsed_qs["resource"]

        filter_params = parsed_qs["advancedFilter"][0].splitlines()
        date_label = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        expected_filter = [
            'resource.type="global"',
            'logName="projects/project_id/logs/airflow"',
            f'labels.task_id="{self.ti.task_id}"',
            f'labels.dag_id="{self.DAG_ID}"',
            f'labels.{date_label}="{self.ti.logical_date.isoformat() if AIRFLOW_V_3_0_PLUS else self.ti.execution_date.isoformat()}"',
            f'labels.try_number="{self.ti.try_number}"',
        ]
        assert set(expected_filter) == set(filter_params)


class TestStackdriverTaskHandlerExceptionHandling:
    """Cloud Logging failures must degrade gracefully, not leak internals."""

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_read_falls_back_when_cloud_logging_unavailable(
        self, mock_client, mock_get_creds_and_project_id, caplog
    ):
        """``read()`` must surface a user-facing message when Cloud Logging raises.

        Without a guard, a gRPC error from ``list_log_entries`` propagates as HTTP 500
        on the log viewer. The fix degrades gracefully and logs the full traceback for
        the operator.
        """
        from google.api_core import exceptions as gapi_exceptions

        mock_get_creds_and_project_id.return_value = ("creds", "project_id")
        mock_client.return_value.list_log_entries.side_effect = gapi_exceptions.ServiceUnavailable(
            "Stackdriver returned an internal error for project secret-project-id"
        )

        handler = StackdriverTaskHandler()
        ti = mock.MagicMock()
        ti.task_id = "t"
        ti.dag_id = "d"
        ti.try_number = 1
        ti.logical_date = mock.MagicMock(isoformat=lambda: "2020-01-01T00:00:00+00:00")
        ti.execution_date = ti.logical_date

        with caplog.at_level(logging.ERROR):
            logs, metadata = handler.read(ti, try_number=1)

        # The user-facing message must NOT include the project id / internal details.
        message = logs[0][0][1]
        assert "Cloud Logging is currently unavailable" in message
        assert "secret-project-id" not in message
        assert metadata == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_read_does_not_leak_internals_in_user_facing_message(
        self, mock_client, mock_get_creds_and_project_id
    ):
        """``read()`` must not propagate gRPC error details into user-visible messages.

        A ``PermissionDenied`` from ``list_log_entries`` typically carries the service
        account email + the missing IAM permission. The outer guard in ``read()`` must
        replace it with a generic message so an authenticated user sees no internal
        identifiers.
        """
        from google.api_core import exceptions as gapi_exceptions

        mock_get_creds_and_project_id.return_value = ("creds", "project_id")
        mock_client.return_value.list_log_entries.side_effect = gapi_exceptions.PermissionDenied(
            "service account 'sa@project.iam.gserviceaccount.com' lacks logging.logEntries.list"
        )

        handler = StackdriverTaskHandler()
        ti = mock.MagicMock()
        ti.task_id = "t"
        ti.dag_id = "d"
        ti.try_number = 1
        ti.logical_date = mock.MagicMock(isoformat=lambda: "2020-01-01T00:00:00+00:00")
        ti.execution_date = ti.logical_date

        logs, _ = handler.read(ti, try_number=1)

        message = logs[0][0][1]
        assert "Cloud Logging is currently unavailable" in message
        assert "sa@project.iam.gserviceaccount.com" not in message
        assert "logging.logEntries.list" not in message

    def test_close_swallows_transport_flush_errors(self, capsys):
        """``close()`` must never raise — even when transport ``flush()`` fails."""
        handler = StackdriverTaskHandler()
        broken_transport = mock.MagicMock()
        broken_transport.flush.side_effect = RuntimeError("flush failed during shutdown")
        # ``transport`` is a cached_property on the slotted attrs class
        # ``StackdriverRemoteLogIO``; its value lives in a slot, not ``__dict__``, so
        # assign the attribute directly to pre-seed it without building a real transport.
        handler.io.transport = broken_transport

        # Must not raise.
        handler.close()

        # The failure is surfaced on stderr because the logging machinery may be shutting down.
        captured = capsys.readouterr()
        assert "transport flush failed" in captured.err
        assert "flush failed during shutdown" in captured.err
