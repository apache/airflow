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
import logging
import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest
from azure.common import AzureHttpError
from azure.core.exceptions import ResourceModifiedError
from azure.storage.blob import BlobType

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.log.wasb_task_handler import WasbRemoteLogIO, WasbTaskHandler
from airflow.utils.state import TaskInstanceState
from airflow.utils.timezone import datetime

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_2_PLUS

pytestmark = pytest.mark.db_test


DEFAULT_DATE = datetime(2020, 8, 10)


class TestWasbRemoteLogIOFromConfig:
    @conf_vars(
        {
            ("logging", "base_log_folder"): "~/airflow/logs",
            ("logging", "remote_base_log_folder"): "wasb://path/to/logs",
            ("logging", "delete_local_logs"): "True",
            ("azure_remote_logging", "remote_wasb_log_container"): "my-container",
        }
    )
    def test_from_config(self):
        subject = WasbRemoteLogIO.from_config()

        assert subject.remote_base == "path/to/logs"
        assert subject.base_log_folder == Path(os.path.expanduser("~/airflow/logs"))
        assert subject.delete_local_copy is True
        assert subject.wasb_container == "my-container"
        assert subject.write_mode == "block_blob"

    @conf_vars(
        {
            ("logging", "base_log_folder"): "~/airflow/logs",
            ("logging", "remote_base_log_folder"): "wasb://path/to/logs",
            ("logging", "delete_local_logs"): "True",
            ("azure_remote_logging", "remote_wasb_log_write_mode"): "append_blob",
        }
    )
    def test_from_config_reads_append_blob_write_mode(self):
        subject = WasbRemoteLogIO.from_config()

        assert subject.write_mode == "append_blob"

    @conf_vars(
        {
            ("logging", "base_log_folder"): "/tmp/airflow/logs",
            ("logging", "remote_base_log_folder"): "wasb://path/to/logs",
            ("logging", "delete_local_logs"): "False",
            ("logging", "remote_task_handler_kwargs"): '{"delete_local_copy": true, "max_bytes": 1024}',
        }
    )
    def test_from_config_applies_io_kwargs_and_filters_file_handler_kwargs(self):
        subject = WasbRemoteLogIO.from_config()

        assert subject.delete_local_copy is True
        assert not hasattr(subject, "max_bytes")
        assert subject.wasb_container == "airflow-logs"

    @conf_vars({("logging", "remote_task_handler_kwargs"): '["not", "a", "dict"]'})
    def test_from_config_rejects_non_dict_remote_task_handler_kwargs(self):
        with pytest.raises(ValueError, match="remote_task_handler_kwargs"):
            WasbRemoteLogIO.from_config()

    def test_provider_registers_wasb_scheme(self):
        from airflow.providers_manager import ProvidersManager

        manager = ProvidersManager()
        if not hasattr(manager, "remote_logging_handler_by_scheme"):
            pytest.skip("Airflow core does not support remote logging provider dispatch")

        info = manager.remote_logging_handler_by_scheme("wasb")

        assert info is not None
        assert info.classpath == "airflow.providers.microsoft.azure.log.wasb_task_handler.WasbRemoteLogIO"

    @pytest.mark.parametrize(
        "manager_classpath",
        [
            pytest.param("airflow.providers_manager.ProvidersManager", id="core"),
            pytest.param(
                "airflow.sdk.providers_manager_runtime.ProvidersManagerTaskRuntime", id="task-runtime"
            ),
        ],
    )
    @conf_vars(
        {
            ("logging", "remote_logging"): "True",
            ("logging", "remote_base_log_folder"): "wasb://path/to/logs",
            ("logging", "remote_log_conn_id"): "wasb_default",
        }
    )
    def test_resolve_remote_task_log_uses_provider_dispatch_not_local_settings(self, manager_classpath):
        factory = pytest.importorskip("airflow._shared.logging.factory")
        from airflow._shared.module_loading import import_string
        from airflow.configuration import conf

        with mock.patch.object(factory, "discover_remote_log_handler", autospec=True) as legacy_discover:
            remote_task_log, conn_id = factory.resolve_remote_task_log(
                conf=conf,
                providers_manager=import_string(manager_classpath)(),
                import_string=import_string,
            )

        assert isinstance(remote_task_log, WasbRemoteLogIO)
        assert remote_task_log.remote_base == "path/to/logs"
        assert conn_id == "wasb_default"
        legacy_discover.assert_not_called()


class TestWasbTaskHandler:
    @pytest.fixture(autouse=True)
    def ti(self, create_task_instance, create_log_template):
        create_log_template("{try_number}.log")
        ti = create_task_instance(
            dag_id="dag_for_testing_wasb_task_handler",
            task_id="task_for_testing_wasb_log_handler",
            logical_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            dagrun_state=TaskInstanceState.RUNNING,
            state=TaskInstanceState.RUNNING,
        )
        ti.try_number = 1
        ti.hostname = "localhost"
        ti.raw = False
        yield ti
        clear_db_runs()
        clear_db_dags()

    def setup_method(self):
        self.wasb_log_folder = "wasb://container/remote/log/location"
        self.remote_log_location = "remote/log/location/1.log"
        self.local_log_location = str(Path(tempfile.tempdir) / "local/log/location")
        self.container_name = "wasb-container"
        self.wasb_task_handler = WasbTaskHandler(
            base_log_folder=self.local_log_location,
            wasb_log_folder=self.wasb_log_folder,
            wasb_container=self.container_name,
            delete_local_copy=True,
        )

    @conf_vars({("logging", "remote_log_conn_id"): "wasb_default"})
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    def test_hook(self, mock_service):
        assert isinstance(self.wasb_task_handler.io.hook, WasbHook)

    @conf_vars({("logging", "remote_log_conn_id"): "wasb_default"})
    def test_hook_warns(self):
        handler = self.wasb_task_handler
        with mock.patch.object(handler.io.log, "exception") as mock_exc:
            with mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook") as mock_hook:
                mock_hook.side_effect = AzureHttpError("failed to connect", 404)
                # Initialize the hook
                handler.io.hook

        assert "Could not create a WasbHook with connection id '%s'" in mock_exc.call_args.args[0]

    def test_set_context_raw(self, ti):
        ti.raw = True
        self.wasb_task_handler.set_context(ti)
        assert self.wasb_task_handler.upload_on_close is False

    def test_set_context_not_raw(self, ti):
        self.wasb_task_handler.set_context(ti)
        assert self.wasb_task_handler.upload_on_close is True

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    def test_wasb_log_exists(self, mock_hook):
        instance = mock_hook.return_value
        instance.check_for_blob.return_value = True
        self.wasb_task_handler.io.wasb_log_exists(self.remote_log_location)
        mock_hook.return_value.check_for_blob.assert_called_once_with(
            self.container_name, self.remote_log_location
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    def test_wasb_read(self, mock_hook_cls, ti):
        mock_hook = mock_hook_cls.return_value
        mock_hook.blob_service_client.primary_endpoint = "https://storage-account.blob.core.windows.net/"
        mock_hook.blob_service_client.account_name = "storage-account"
        mock_hook.get_blobs_list.return_value = ["abc/hello.log"]
        mock_hook.read_file.return_value = "Log line"
        assert self.wasb_task_handler.io.wasb_read(self.remote_log_location) == "Log line"
        ti = copy.copy(ti)
        ti.state = TaskInstanceState.SUCCESS

        logs, metadata = self.wasb_task_handler.read(ti)

        if AIRFLOW_V_3_2_2_PLUS:
            logs = list(logs)
            assert logs[0].event == "::group::Log message source details"
            assert (
                logs[1].event == "https://storage-account.blob.core.windows.net/wasb-container/abc/hello.log"
            )
            assert logs[2].event == "::endgroup::"
            assert logs[3].event == "Log line"
            assert metadata == {"end_of_log": True, "log_pos": 1}
        elif AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            assert logs[0].event == "::group::Log message source details"
            assert logs[0].sources == [
                "https://storage-account.blob.core.windows.net/wasb-container/abc/hello.log"
            ]
            assert logs[1].event == "::endgroup::"
            assert logs[2].event == "Log line"
            assert metadata == {"end_of_log": True, "log_pos": 1}
        else:
            assert logs[0][0][0] == "localhost"
            assert (
                "*** Found remote logs:\n"
                "***   * https://storage-account.blob.core.windows.net/wasb-container/abc/hello.log\n"
                in logs[0][0][1]
            )
            assert "Log line" in logs[0][0][1]
            assert metadata[0] == {
                "end_of_log": True,
                "log_pos": 8,
            }

    def test_log_source_url_keeps_endpoint_path_and_removes_query_string(self):
        mock_hook = mock.MagicMock()
        mock_hook.blob_service_client.primary_endpoint = "http://127.0.0.1:10000/devstoreaccount1?sastoken"
        mock_hook.blob_service_client.account_name = "devstoreaccount1"

        with mock.patch.object(WasbRemoteLogIO, "hook", new=mock_hook):
            assert (
                self.wasb_task_handler.io._build_log_source_url("abc/hello.log")
                == "http://127.0.0.1:10000/devstoreaccount1/wasb-container/abc/hello.log"
            )

    def test_log_source_url_removes_query_and_fragment_from_primary_endpoint(self):
        mock_hook = mock.MagicMock()
        mock_hook.blob_service_client.primary_endpoint = (
            "https://storage-account.blob.core.windows.net/?sv=2020&sig=secret#fragment"
        )
        mock_hook.blob_service_client.account_name = "storage-account"

        with mock.patch.object(WasbRemoteLogIO, "hook", new=mock_hook):
            assert (
                self.wasb_task_handler.io._build_log_source_url("abc/hello.log")
                == "https://storage-account.blob.core.windows.net/wasb-container/abc/hello.log"
            )

    def test_log_source_url_removes_sas_token_from_endpoint_path(self):
        mock_hook = mock.MagicMock()
        mock_hook.blob_service_client.primary_endpoint = (
            "https://storage-account.blob.core.windows.net/SAStoken/"
        )
        mock_hook.blob_service_client.account_name = "storage-account"

        with mock.patch.object(WasbRemoteLogIO, "hook", new=mock_hook):
            assert (
                self.wasb_task_handler.io._build_log_source_url("abc/hello.log")
                == "https://storage-account.blob.core.windows.net/wasb-container/abc/hello.log"
            )

    def test_log_source_url_uses_account_name_when_primary_endpoint_is_unavailable(self):
        mock_hook = mock.MagicMock()
        mock_hook.blob_service_client.primary_endpoint = None
        mock_hook.blob_service_client.account_name = "storage-account"

        with mock.patch.object(WasbRemoteLogIO, "hook", new=mock_hook):
            assert (
                self.wasb_task_handler.io._build_log_source_url("abc/hello.log")
                == "https://storage-account.blob.core.windows.net/wasb-container/abc/hello.log"
            )

    def test_log_source_url_uses_legacy_url_when_endpoint_and_account_name_are_unavailable(self):
        mock_hook = mock.MagicMock()
        mock_hook.blob_service_client.primary_endpoint = None
        mock_hook.blob_service_client.account_name = None

        with mock.patch.object(WasbRemoteLogIO, "hook", new=mock_hook):
            assert (
                self.wasb_task_handler.io._build_log_source_url("abc/hello.log")
                == "https://wasb-container.blob.core.windows.net/abc/hello.log"
            )

    def test_log_source_url_uses_legacy_url_when_hook_is_unavailable(self):
        with mock.patch.object(WasbRemoteLogIO, "hook", new=None):
            assert (
                self.wasb_task_handler.io._build_log_source_url("abc/hello.log")
                == "https://wasb-container.blob.core.windows.net/abc/hello.log"
            )

    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.wasb.WasbHook",
        **{"return_value.read_file.side_effect": AzureHttpError("failed to connect", 404)},
    )
    def test_wasb_read_raises(self, mock_hook, caplog):
        handler = self.wasb_task_handler
        with caplog.at_level(logging.ERROR):
            handler.io.wasb_read(self.remote_log_location, return_error=True)
        assert len(caplog.records) == 1
        rec = caplog.records[0]
        assert rec.levelno == logging.ERROR
        assert rec.message == "Could not read logs from remote/log/location/1.log"
        assert rec.exc_info is not None

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    @mock.patch.object(WasbRemoteLogIO, "wasb_read")
    @mock.patch.object(WasbRemoteLogIO, "wasb_log_exists")
    def test_write_log(self, mock_log_exists, mock_wasb_read, mock_hook):
        mock_log_exists.return_value = True
        mock_wasb_read.return_value = ""
        self.wasb_task_handler.io.write("text", self.remote_log_location)
        mock_hook.return_value.load_string.assert_called_once_with(
            "text", self.container_name, self.remote_log_location, overwrite=True
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    @mock.patch.object(WasbRemoteLogIO, "wasb_read")
    @mock.patch.object(WasbRemoteLogIO, "wasb_log_exists")
    def test_write_on_existing_log(self, mock_log_exists, mock_wasb_read, mock_hook):
        mock_log_exists.return_value = True
        mock_wasb_read.return_value = "old log"
        self.wasb_task_handler.io.write("text", self.remote_log_location)
        mock_hook.return_value.load_string.assert_called_once_with(
            "old log\ntext",
            self.container_name,
            self.remote_log_location,
            overwrite=True,
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    @mock.patch.object(WasbRemoteLogIO, "wasb_read")
    @mock.patch.object(WasbRemoteLogIO, "wasb_log_exists")
    def test_write_on_existing_log_already_newline_terminated(
        self, mock_log_exists, mock_wasb_read, mock_hook
    ):
        mock_log_exists.return_value = True
        mock_wasb_read.return_value = "old log\n"
        self.wasb_task_handler.io.write("text", self.remote_log_location)
        mock_hook.return_value.load_string.assert_called_once_with(
            "old log\ntext",
            self.container_name,
            self.remote_log_location,
            overwrite=True,
        )

    def test_upload_repeated_appends_no_duplication(self, tmp_path):
        """Each execution lifecycle of one attempt appends to the same local log and uploads it."""
        blobs: dict[str, str] = {}

        class FakeHook:
            def check_for_blob(self, container, blob_name, **kwargs):
                return blob_name in blobs

            def read_file(self, container, blob_name, **kwargs):
                return blobs.get(blob_name, "")

            def load_string(self, string_data, container, blob_name, **kwargs):
                blobs[blob_name] = string_data

        io = WasbRemoteLogIO(
            remote_base="remote/log/location",
            base_log_folder=str(tmp_path),
            delete_local_copy=False,
            wasb_container=self.container_name,
        )
        io.hook = FakeHook()

        local_log = tmp_path / "attempt=1.log"
        for cycle in range(1, 4):
            with open(local_log, "a") as f:
                f.write(f"cycle {cycle}\n")
            io.upload("attempt=1.log")

        assert blobs["remote/log/location/attempt=1.log"] == "cycle 1\ncycle 2\ncycle 3\n"
        assert local_log.read_text() == ""

    @pytest.mark.parametrize(
        ("has_uploaded", "expected_local_content"),
        [(True, ""), (False, "some log\n")],
    )
    @mock.patch.object(WasbRemoteLogIO, "write")
    def test_upload_truncates_local_log_only_after_successful_write(
        self, mock_write, tmp_path, has_uploaded, expected_local_content
    ):
        """A failed upload must leave the local log intact so it can be retried."""
        mock_write.return_value = has_uploaded
        io = WasbRemoteLogIO(
            remote_base="remote/log/location",
            base_log_folder=str(tmp_path),
            delete_local_copy=False,
            wasb_container=self.container_name,
        )
        local_log = tmp_path / "attempt=1.log"
        local_log.write_text("some log\n")

        io.upload("attempt=1.log")

        assert local_log.read_text() == expected_local_content

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    def test_write_when_append_is_false(self, mock_hook):
        self.wasb_task_handler.io.write("text", self.remote_log_location, False)
        mock_hook.return_value.load_string.assert_called_once_with(
            "text", self.container_name, self.remote_log_location, overwrite=True
        )

    def test_write_raises(self, caplog):
        handler = self.wasb_task_handler
        with caplog.at_level(logging.ERROR):
            with mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook") as mock_hook:
                mock_hook.return_value.load_string.side_effect = AzureHttpError("failed to connect", 404)

                handler.io.write("text", self.remote_log_location, append=False)

        assert len(caplog.records) == 1
        rec = caplog.records[0]
        assert rec.levelno == logging.ERROR
        assert rec.message == "Could not write logs to remote/log/location/1.log"
        assert rec.exc_info is not None

    @pytest.mark.parametrize(
        ("delete_local_copy", "expected_existence_of_local_copy"),
        [(True, False), (False, True)],
    )
    @mock.patch.object(WasbRemoteLogIO, "write")
    def test_close_with_delete_local_logs_conf(
        self,
        wasb_write_mock,
        ti,
        tmp_path_factory,
        delete_local_copy,
        expected_existence_of_local_copy,
    ):
        with conf_vars({("logging", "delete_local_logs"): str(delete_local_copy)}):
            handler = WasbTaskHandler(
                base_log_folder=str(tmp_path_factory.mktemp("local-s3-log-location")),
                wasb_log_folder=self.wasb_log_folder,
                wasb_container=self.container_name,
            )
        wasb_write_mock.return_value = True
        handler.log.info("test")
        handler.set_context(ti)
        assert handler.upload_on_close

        handler.close()
        assert os.path.exists(handler.handler.baseFilename) == expected_existence_of_local_copy

    def test_filename_template_for_backward_compatibility(self):
        # filename_template arg support for running the latest provider on airflow 2
        WasbTaskHandler(
            base_log_folder=self.local_log_location,
            wasb_log_folder=self.wasb_log_folder,
            wasb_container=self.container_name,
            delete_local_copy=True,
            filename_template=None,
        )


class TestWasbRemoteLogIOAppendBlobMode:
    """Tests for the opt-in append_blob write mode. Regression tests for #70867."""

    container_name = "test-container"

    class FakeAppendHook:
        def __init__(self):
            self.blobs: dict[str, dict] = {}
            self.append_block_calls: list[tuple[str, bytes, int]] = []
            self.fail_append_at_offset: int | None = None

        def check_for_blob(self, container, blob_name, **kwargs):
            return blob_name in self.blobs

        def get_blob_properties(self, container, blob_name, **kwargs):
            blob = self.blobs[blob_name]
            props = mock.Mock()
            props.blob_type = BlobType.APPENDBLOB if blob["type"] == "AppendBlob" else BlobType.BLOCKBLOB
            props.size = len(blob["content"])
            return props

        def create_append_blob(self, container, blob_name, **kwargs):
            self.blobs.setdefault(blob_name, {"type": "AppendBlob", "content": b""})

        def append_block(self, container, blob_name, data, offset, **kwargs):
            if self.fail_append_at_offset is not None and offset == self.fail_append_at_offset:
                raise ConnectionError("simulated transient failure")
            blob = self.blobs[blob_name]
            if len(blob["content"]) != offset:
                raise ResourceModifiedError("append position mismatch")
            blob["content"] += data
            self.append_block_calls.append((blob_name, data, offset))

        def read_file(self, container, blob_name, **kwargs):
            return self.blobs[blob_name]["content"].decode("utf-8")

        def load_string(self, string_data, container, blob_name, **kwargs):
            self.blobs[blob_name] = {"type": "BlockBlob", "content": string_data.encode("utf-8")}

    def _io(self, tmp_path, hook, write_mode="append_blob"):
        io = WasbRemoteLogIO(
            remote_base="remote/log/location",
            base_log_folder=str(tmp_path),
            delete_local_copy=False,
            wasb_container=self.container_name,
            write_mode=write_mode,
        )
        io.hook = hook
        return io

    def test_append_blob_multiple_lifecycles_no_redownload(self, tmp_path):
        hook = self.FakeAppendHook()
        io = self._io(tmp_path, hook)

        local_log = tmp_path / "attempt=1.log"
        for cycle in range(1, 4):
            with open(local_log, "a") as f:
                f.write(f"cycle {cycle}\n")
            io.upload("attempt=1.log")

        blob_name = "remote/log/location/attempt=1.log"
        assert hook.blobs[blob_name]["content"].decode() == "cycle 1\ncycle 2\ncycle 3\n"
        assert [call[1] for call in hook.append_block_calls] == [b"cycle 1\n", b"cycle 2\n", b"cycle 3\n"]
        assert local_log.read_text() == ""

    def test_append_blob_creates_blob_on_first_write(self, tmp_path):
        hook = self.FakeAppendHook()
        io = self._io(tmp_path, hook)

        assert io.write("first segment\n", "remote/log/location/attempt=1.log") is True
        assert hook.blobs["remote/log/location/attempt=1.log"]["content"] == b"first segment\n"

    def test_existing_block_blob_is_not_converted(self, tmp_path):
        hook = self.FakeAppendHook()
        hook.blobs["remote/log/location/attempt=1.log"] = {"type": "BlockBlob", "content": b"old log\n"}
        io = self._io(tmp_path, hook)

        assert io.write("new segment\n", "remote/log/location/attempt=1.log") is True
        blob = hook.blobs["remote/log/location/attempt=1.log"]
        assert blob["type"] == "BlockBlob"
        assert blob["content"] == b"old log\nnew segment\n"
        assert hook.append_block_calls == []

    def test_segment_larger_than_block_limit_is_chunked(self, tmp_path):
        hook = self.FakeAppendHook()
        io = self._io(tmp_path, hook)
        io._MAX_APPEND_BLOCK_BYTES = 10

        assert io.write("a" * 25, "remote/log/location/attempt=1.log") is True

        blob_name = "remote/log/location/attempt=1.log"
        assert hook.blobs[blob_name]["content"] == b"a" * 25
        assert [call[2] for call in hook.append_block_calls] == [0, 10, 20]

    def test_append_position_mismatch_fails_safely(self, tmp_path):
        hook = self.FakeAppendHook()
        hook.create_append_blob(self.container_name, "remote/log/location/attempt=1.log")
        io = self._io(tmp_path, hook)

        original_append_block = hook.append_block

        def append_block_with_race(container, blob_name, data, offset, **kwargs):
            hook.blobs[blob_name]["content"] += b"a concurrent writer's segment\n"
            original_append_block(container, blob_name, data, offset, **kwargs)

        hook.append_block = append_block_with_race

        local_log = tmp_path / "attempt=1.log"
        local_log.write_text("our segment\n")

        io.upload("attempt=1.log")

        assert local_log.read_text() == "our segment\n"

    def test_partial_chunk_failure_trims_local_file_to_uncommitted_remainder(self, tmp_path):
        hook = self.FakeAppendHook()
        hook.fail_append_at_offset = 10
        io = self._io(tmp_path, hook)
        io._MAX_APPEND_BLOCK_BYTES = 10
        local_log = tmp_path / "attempt=1.log"
        local_log.write_text("A" * 10 + "B" * 10 + "C" * 5)
        io.upload("attempt=1.log")
        blob_name = "remote/log/location/attempt=1.log"
        assert hook.blobs[blob_name]["content"] == b"A" * 10
        assert local_log.read_text() == "B" * 10 + "C" * 5
        hook.fail_append_at_offset = None
        io.upload("attempt=1.log")
        assert hook.blobs[blob_name]["content"] == b"A" * 10 + b"B" * 10 + b"C" * 5
        assert local_log.read_text() == ""

    def test_chunk_boundary_never_splits_a_multibyte_character(self, tmp_path):
        hook = self.FakeAppendHook()
        hook.fail_append_at_offset = 9
        io = self._io(tmp_path, hook)
        io._MAX_APPEND_BLOCK_BYTES = 10
        segment = "a" * 9 + "\u20ac" + "b" * 9
        local_log = tmp_path / "attempt=1.log"
        local_log.write_text(segment)
        io.upload("attempt=1.log")
        blob_name = "remote/log/location/attempt=1.log"
        committed = hook.blobs[blob_name]["content"]
        committed.decode("utf-8")
        assert committed == b"a" * 9
        assert local_log.read_text() == "\u20ac" + "b" * 9
        hook.fail_append_at_offset = None
        io.upload("attempt=1.log")
        assert hook.blobs[blob_name]["content"].decode("utf-8") == segment
        assert local_log.read_text() == ""
