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
import io
import logging
import os
from types import GeneratorType
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.providers.google.cloud.log.gcs_task_handler import GCSRemoteLogIO, GCSTaskHandler
from airflow.sdk import BaseOperator
from airflow.utils.state import TaskInstanceState
from airflow.utils.timezone import datetime

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from pathlib import Path


def patch_mock_client_for_list_blobs(mock_client: MagicMock, blob_names: list[str]):
    mock_blobs = []
    for name in blob_names:
        mock_blob = MagicMock()
        mock_blob.name = name
        mock_blobs.append(mock_blob)
    mock_client.return_value.list_blobs.return_value = mock_blobs


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="This path only works on Airflow 3")
class TestGCSRemoteLogIO:
    @pytest.fixture(autouse=True)
    def setup_tests(self, create_runtime_ti):
        # setup remote IO
        self.base_log_folder = "local/airflow/logs"
        self.gcs_log_folder = "gs://bucket/airflow/logs"
        self.ti = create_runtime_ti(BaseOperator(task_id="task_1"))

    @pytest.mark.parametrize(
        "is_absolute",
        [pytest.param(True, id="absolute"), pytest.param(False, id="relative")],
    )
    @pytest.mark.parametrize(
        "file_exists",
        [pytest.param(True, id="file-exists"), pytest.param(False, id="file-not-exists")],
    )
    @pytest.mark.parametrize(
        "delete_local_copy",
        [pytest.param(True, id="delete-local"), pytest.param(False, id="keep-local")],
    )
    @pytest.mark.parametrize(
        "mock_write_method_result",
        [pytest.param(True, id="write-success"), pytest.param(False, id="write-fail")],
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    @mock.patch("shutil.rmtree")
    def test_upload(
        self,
        mock_rmtree,
        mock_blob,
        mock_client,
        tmp_path: Path,
        is_absolute: bool,
        file_exists: bool,
        delete_local_copy: bool,
        mock_write_method_result: bool,
    ):
        # setup
        gcs_remote_log_io = GCSRemoteLogIO(
            remote_base=self.gcs_log_folder,
            base_log_folder=tmp_path.as_posix(),
            delete_local_copy=delete_local_copy,
        )
        if file_exists:
            file_path = tmp_path / "existing.log" if is_absolute else "existing.log"
            with open(tmp_path / "existing.log", "w") as f:
                f.write("log content")
        else:
            file_path = tmp_path / "non_existing.log"

        # action
        with mock.patch.object(
            gcs_remote_log_io,
            "write",
            return_value=mock_write_method_result,
        ) as mock_write_method:
            gcs_remote_log_io.upload(file_path, self.ti)

            # verify
            if file_exists:
                mock_write_method.assert_called_once()
                if delete_local_copy and mock_write_method_result:
                    mock_rmtree.assert_called_once_with(tmp_path.as_posix())
                else:
                    mock_rmtree.assert_not_called()
            else:
                mock_write_method.assert_not_called()
                mock_rmtree.assert_not_called()

    @pytest.mark.parametrize(
        "old_log_exists",
        [pytest.param(True, id="old-log-exists"), pytest.param(False, id="old-log-not-exists")],
    )
    @pytest.mark.parametrize(
        "upload_success",
        [pytest.param(True, id="upload-success"), pytest.param(False, id="upload-fail")],
    )
    @pytest.mark.parametrize(
        "old_log_read_error",
        [pytest.param(None, id="no-read-error"), pytest.param(Exception("Read error"), id="read-error")],
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_write(
        self,
        mock_blob,
        mock_client,
        old_log_exists: bool,
        upload_success: bool,
        old_log_read_error: Exception | None,
    ):
        # setup
        remote_log_location = f"{self.gcs_log_folder}/task_1/attempt_1.log"
        new_log_content = "NEW LOG CONTENT"
        old_log_content = "OLD LOG CONTENT"

        # mock download_as_bytes for reading old log
        if old_log_read_error:
            mock_blob.from_string.return_value.download_as_bytes.side_effect = old_log_read_error
        elif old_log_exists:
            mock_blob.from_string.return_value.download_as_bytes.return_value = old_log_content.encode()
        else:
            # Simulate 404 error for no log found
            not_found_error = Exception("No such object: bucket/airflow/logs/task_1/attempt_1.log")
            mock_blob.from_string.return_value.download_as_bytes.side_effect = not_found_error

        # mock upload_from_string
        if not upload_success:
            mock_blob.from_string.return_value.upload_from_string.side_effect = Exception("Upload failed")

        gcs_remote_log_io = GCSRemoteLogIO(
            remote_base=self.gcs_log_folder,
            base_log_folder=self.base_log_folder,
            delete_local_copy=False,
        )

        # action
        result = gcs_remote_log_io.write(new_log_content, remote_log_location)

        # verify
        assert result == upload_success

        # verify the content that was uploaded
        if upload_success or not upload_success:  # upload_from_string is called regardless
            call_args = mock_blob.from_string.return_value.upload_from_string.call_args
            if call_args:
                uploaded_content = call_args[0][0]
                if old_log_exists and not old_log_read_error:
                    assert uploaded_content == f"{old_log_content}\n{new_log_content}"
                else:
                    assert uploaded_content == new_log_content

    @pytest.mark.parametrize(
        "is_stream_method",
        [pytest.param(True, id="is-stream"), pytest.param(False, id="not-stream")],
    )
    @pytest.mark.parametrize(
        "blob_names",
        [
            pytest.param(
                ["airflow/logs/task_1/attempt_1.log", "airflow/logs/task_1/attempt_2.log"], id="blobs-exists"
            ),
            pytest.param([], id="blobs-not-exists"),
        ],
    )
    @pytest.mark.parametrize(
        "read_success",
        [pytest.param(True, id="read-success"), pytest.param(False, id="read-fail")],
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_stream_and_read_methods(
        self,
        mock_blob,
        mock_client,
        is_stream_method: bool,
        blob_names: list[str],
        read_success: bool,
    ):
        # setup
        patch_mock_client_for_list_blobs(mock_client, blob_names)
        if read_success:
            mock_blob.from_string.return_value.open.side_effect = lambda mode: io.TextIOWrapper(
                io.BytesIO(b"LOG\nCONTENT"), encoding="utf-8"
            )
        else:
            mock_blob.from_string.return_value.open.side_effect = Exception("Read failed")

        gcs_remote_log_io = GCSRemoteLogIO(
            remote_base=self.gcs_log_folder,
            base_log_folder=self.base_log_folder,
            delete_local_copy=False,
        )
        # action
        if is_stream_method:
            messages, logs = gcs_remote_log_io.stream("airflow/logs/task_1", self.ti)
        else:
            messages, logs = gcs_remote_log_io.read("airflow/logs/task_1", self.ti)

        # early return for no blobs
        if not blob_names:
            assert messages == []
            assert logs is None
            return

        # verify messages
        expected_messages = [
            f"{self.gcs_log_folder}/task_1/attempt_1.log",
            f"{self.gcs_log_folder}/task_1/attempt_2.log",
        ]
        if not AIRFLOW_V_3_0_PLUS:
            expected_messages = messages.extend(
                ["Found remote logs:", *[f"  * {x}" for x in sorted(expected_messages)]]
            )
        if not read_success and not AIRFLOW_V_3_0_PLUS:
            expected_messages + [f"Unable to read remote log {Exception('Read failed')}"]
        assert messages == expected_messages

        # verify logs
        expected_logs = ["LOG\nCONTENT", "LOG\nCONTENT"]
        if is_stream_method:
            for log_stream, expected_log in zip(logs, expected_logs):
                assert isinstance(log_stream, GeneratorType)
                assert "".join(log_stream) == expected_log
        else:
            if read_success:
                assert logs == expected_logs
            else:
                assert logs == []


@pytest.mark.db_test
class TestGCSTaskHandler:
    @pytest.fixture(autouse=True)
    def task_instance(self, create_task_instance, session):
        self.ti = ti = create_task_instance(
            dag_id="dag_for_testing_gcs_task_handler",
            task_id="task_for_testing_gcs_task_handler",
            logical_date=datetime(2020, 1, 1),
            state=TaskInstanceState.RUNNING,
        )
        ti.try_number = 1
        ti.raw = False
        session.add(ti)
        session.commit()
        yield
        clear_db_runs()
        clear_db_dags()

    @pytest.fixture(autouse=True)
    def local_log_location(self, tmp_path_factory):
        return str(tmp_path_factory.mktemp("local-gcs-log-location"))

    @pytest.fixture(autouse=True)
    def gcs_task_handler(self, create_log_template, local_log_location):
        create_log_template("{try_number}.log")
        self.gcs_task_handler = GCSTaskHandler(
            base_log_folder=local_log_location,
            gcs_log_folder="gs://bucket/remote/log/location",
        )
        return self.gcs_task_handler

    @mock.patch("airflow.providers.google.cloud.log.gcs_task_handler.GCSHook")
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id")
    @pytest.mark.parametrize(
        "conn_id",
        [pytest.param("", id="no-conn"), pytest.param("my_gcs_conn", id="with-conn")],
    )
    def test_client_conn_id_behavior(self, mock_get_cred, mock_client, mock_hook, conn_id):
        """When remote log conn id configured, hook will be used"""
        mock_hook.return_value.get_credentials_and_project_id.return_value = (
            "test_cred",
            "test_proj",
        )
        mock_get_cred.return_value = ("test_cred", "test_proj")
        with conf_vars({("logging", "remote_log_conn_id"): conn_id}):
            return_value = self.gcs_task_handler.io.client
        if conn_id:
            mock_hook.assert_called_once_with(gcp_conn_id="my_gcs_conn")
            mock_get_cred.assert_not_called()
        else:
            mock_hook.assert_not_called()
            mock_get_cred.assert_called()

        mock_client.assert_called_once_with(
            client_info=mock.ANY, credentials="test_cred", project="test_proj"
        )
        assert mock_client.return_value == return_value

    @conf_vars({("logging", "remote_log_conn_id"): "gcs_default"})
    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_should_read_logs_from_remote(
        self, mock_blob, mock_client, mock_creds, session, sdk_connection_not_found
    ):
        blob_name = "remote/log/location/1.log"
        patch_mock_client_for_list_blobs(mock_client, [blob_name])
        mock_blob.from_string.return_value.open.return_value = io.TextIOWrapper(
            io.BytesIO(b"CONTENT"), encoding="utf-8"
        )
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        session.add(ti)
        session.commit()
        logs, metadata = self.gcs_task_handler._read(ti, self.ti.try_number)
        expected_gs_uri = f"gs://bucket/{blob_name}"

        mock_blob.from_string.assert_called_once_with(expected_gs_uri, mock_client.return_value)

        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            assert logs[0].event == "::group::Log message source details"
            assert logs[0].sources == [expected_gs_uri]
            assert logs[1].event == "::endgroup::"
            assert logs[2].event == "CONTENT"
            assert metadata == {"end_of_log": True, "log_pos": 1}
        else:
            assert f"*** Found remote logs:\n***   * {expected_gs_uri}\n" in logs
            assert logs.endswith("CONTENT")
            assert metadata == {"end_of_log": True, "log_pos": 7}

    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_should_read_from_local_on_logs_read_error(self, mock_blob, mock_client, mock_creds):
        blob_name = "remote/log/location/1.log"
        patch_mock_client_for_list_blobs(mock_client, [blob_name])
        mock_blob.from_string.return_value.open.side_effect = Exception("Failed to connect")

        self.gcs_task_handler.set_context(self.ti)
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        log, metadata = self.gcs_task_handler._read(ti, self.ti.try_number)
        expected_gs_uri = f"gs://bucket/{blob_name}"

        if AIRFLOW_V_3_0_PLUS:
            log = list(log)
            assert log[0].event == "::group::Log message source details"
            assert log[0].sources == [
                expected_gs_uri,
                f"{self.gcs_task_handler.local_base}/1.log",
            ]
            assert log[1].event == "::endgroup::"
            assert metadata == {"end_of_log": True, "log_pos": 0}
        else:
            assert (
                "*** Found remote logs:\n"
                "***   * gs://bucket/remote/log/location/1.log\n"
                "*** Unable to read remote log Failed to connect\n"
                "*** Found local files:\n"
                f"***   * {self.gcs_task_handler.local_base}/1.log\n"
            ) in log
            assert metadata == {"end_of_log": True, "log_pos": 0}
        mock_blob.from_string.assert_called_once_with(expected_gs_uri, mock_client.return_value)

    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_write_to_remote_on_close(self, mock_blob, mock_client, mock_creds):
        mock_blob.from_string.return_value.download_as_bytes.return_value = b"CONTENT"

        self.gcs_task_handler.set_context(self.ti)
        self.gcs_task_handler.emit(
            logging.LogRecord(
                name="NAME",
                level="DEBUG",
                pathname=None,
                lineno=None,
                msg="MESSAGE",
                args=None,
                exc_info=None,
            )
        )
        self.gcs_task_handler.close()

        mock_blob.assert_has_calls(
            [
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().download_as_bytes(),
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().upload_from_string("CONTENT\nMESSAGE\n", content_type="text/plain"),
            ],
            any_order=False,
        )
        mock_blob.from_string.return_value.upload_from_string(data="CONTENT\nMESSAGE\n")
        assert self.gcs_task_handler.closed is True

    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_failed_write_to_remote_on_close(self, mock_blob, mock_client, mock_creds, caplog):
        caplog.at_level(logging.ERROR, logger=self.gcs_task_handler.log.name)
        mock_blob.from_string.return_value.upload_from_string.side_effect = Exception("Failed to connect")
        mock_blob.from_string.return_value.download_as_bytes.return_value = b"Old log"

        self.gcs_task_handler.set_context(self.ti)
        self.gcs_task_handler.emit(
            logging.LogRecord(
                name="NAME",
                level="DEBUG",
                pathname=None,
                lineno=None,
                msg="MESSAGE",
                args=None,
                exc_info=None,
            )
        )
        self.gcs_task_handler.close()

        assert caplog.record_tuples == [
            (
                "airflow.providers.google.cloud.log.gcs_task_handler.GCSRemoteLogIO",
                logging.ERROR,
                "Could not write logs to gs://bucket/remote/log/location/1.log: Failed to connect",
            ),
        ]
        mock_blob.assert_has_calls(
            [
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().download_as_bytes(),
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().upload_from_string("Old log\nMESSAGE\n", content_type="text/plain"),
            ],
            any_order=False,
        )

    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_write_to_remote_on_close_failed_read_old_logs(self, mock_blob, mock_client, mock_creds):
        mock_blob.from_string.return_value.download_as_bytes.side_effect = Exception("Fail to download")

        self.gcs_task_handler.set_context(self.ti)
        self.gcs_task_handler.emit(
            logging.LogRecord(
                name="NAME",
                level="DEBUG",
                pathname=None,
                lineno=None,
                msg="MESSAGE",
                args=None,
                exc_info=None,
            )
        )
        self.gcs_task_handler.close()

        mock_blob.from_string.assert_has_calls(
            [
                mock.call("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call().download_as_bytes(),
                mock.call("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call().upload_from_string(
                    "MESSAGE\n",
                    content_type="text/plain",
                ),
            ],
            any_order=False,
        )

    @pytest.mark.parametrize(
        ("delete_local_copy", "expected_existence_of_local_copy"),
        [(True, False), (False, True)],
    )
    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_close_with_delete_local_copy_conf(
        self,
        mock_blob,
        mock_client,
        mock_creds,
        local_log_location,
        delete_local_copy,
        expected_existence_of_local_copy,
    ):
        mock_blob.from_string.return_value.open.return_value = io.TextIOWrapper(
            io.BytesIO(b"CONTENT"), encoding="utf-8"
        )
        with conf_vars({("logging", "delete_local_logs"): str(delete_local_copy)}):
            handler = GCSTaskHandler(
                base_log_folder=local_log_location,
                gcs_log_folder="gs://bucket/remote/log/location",
            )

        handler.log.info("test")
        handler.set_context(self.ti)
        assert handler.upload_on_close

        handler.close()
        assert os.path.exists(handler.handler.baseFilename) == expected_existence_of_local_copy

    @pytest.fixture(autouse=True)
    def test_filename_template_for_backward_compatibility(self, local_log_location):
        # filename_template arg support for running the latest provider on airflow 2
        GCSTaskHandler(
            base_log_folder=local_log_location,
            gcs_log_folder="gs://bucket/remote/log/location",
            filename_template=None,
        )
