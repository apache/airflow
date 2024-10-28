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
import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest
from azure.common import AzureHttpError

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.log.wasb_task_handler import WasbTaskHandler
from airflow.utils.state import TaskInstanceState
from airflow.utils.timezone import datetime

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test


DEFAULT_DATE = datetime(2020, 8, 10)


class TestWasbTaskHandler:
    @pytest.fixture(autouse=True)
    def ti(self, create_task_instance, create_log_template):
        create_log_template("{try_number}.log")
        ti = create_task_instance(
            dag_id="dag_for_testing_wasb_task_handler",
            task_id="task_for_testing_wasb_log_handler",
            execution_date=DEFAULT_DATE,
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
        assert isinstance(self.wasb_task_handler.hook, WasbHook)

    @conf_vars({("logging", "remote_log_conn_id"): "wasb_default"})
    def test_hook_warns(self):
        handler = self.wasb_task_handler
        with mock.patch.object(handler.log, "exception") as mock_exc:
            with mock.patch(
                "airflow.providers.microsoft.azure.hooks.wasb.WasbHook"
            ) as mock_hook:
                mock_hook.side_effect = AzureHttpError("failed to connect", 404)
                # Initialize the hook
                handler.hook

        assert (
            "Could not create a WasbHook with connection id '%s'"
            in mock_exc.call_args.args[0]
        )

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
        self.wasb_task_handler.wasb_log_exists(self.remote_log_location)
        mock_hook.return_value.check_for_blob.assert_called_once_with(
            self.container_name, self.remote_log_location
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    def test_wasb_read(self, mock_hook_cls, ti):
        mock_hook = mock_hook_cls.return_value
        mock_hook.get_blobs_list.return_value = ["abc/hello.log"]
        mock_hook.read_file.return_value = "Log line"
        assert self.wasb_task_handler.wasb_read(self.remote_log_location) == "Log line"
        ti = copy.copy(ti)
        ti.state = TaskInstanceState.SUCCESS
        assert self.wasb_task_handler.read(ti) == (
            [
                [
                    (
                        "localhost",
                        "*** Found remote logs:\n"
                        "***   * https://wasb-container.blob.core.windows.net/abc/hello.log\nLog line",
                    )
                ]
            ],
            [{"end_of_log": True, "log_pos": 8}],
        )

    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.wasb.WasbHook",
        **{
            "return_value.read_file.side_effect": AzureHttpError("failed to connect", 404)
        },
    )
    def test_wasb_read_raises(self, mock_hook):
        handler = self.wasb_task_handler
        with mock.patch.object(handler.log, "error") as mock_error:
            handler.wasb_read(self.remote_log_location, return_error=True)
            mock_error.assert_called_once_with(
                "Could not read logs from remote/log/location/1.log",
                exc_info=True,
            )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    @mock.patch.object(WasbTaskHandler, "wasb_read")
    @mock.patch.object(WasbTaskHandler, "wasb_log_exists")
    def test_write_log(self, mock_log_exists, mock_wasb_read, mock_hook):
        mock_log_exists.return_value = True
        mock_wasb_read.return_value = ""
        self.wasb_task_handler.wasb_write("text", self.remote_log_location)
        mock_hook.return_value.load_string.assert_called_once_with(
            "text", self.container_name, self.remote_log_location, overwrite=True
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    @mock.patch.object(WasbTaskHandler, "wasb_read")
    @mock.patch.object(WasbTaskHandler, "wasb_log_exists")
    def test_write_on_existing_log(self, mock_log_exists, mock_wasb_read, mock_hook):
        mock_log_exists.return_value = True
        mock_wasb_read.return_value = "old log"
        self.wasb_task_handler.wasb_write("text", self.remote_log_location)
        mock_hook.return_value.load_string.assert_called_once_with(
            "old log\ntext", self.container_name, self.remote_log_location, overwrite=True
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
    def test_write_when_append_is_false(self, mock_hook):
        self.wasb_task_handler.wasb_write("text", self.remote_log_location, False)
        mock_hook.return_value.load_string.assert_called_once_with(
            "text", self.container_name, self.remote_log_location, overwrite=True
        )

    def test_write_raises(self):
        handler = self.wasb_task_handler
        with mock.patch.object(handler.log, "error") as mock_error:
            with mock.patch(
                "airflow.providers.microsoft.azure.hooks.wasb.WasbHook"
            ) as mock_hook:
                mock_hook.return_value.load_string.side_effect = AzureHttpError(
                    "failed to connect", 404
                )

                handler.wasb_write("text", self.remote_log_location, append=False)

            mock_error.assert_called_once_with(
                "Could not write logs to %s", "remote/log/location/1.log", exc_info=True
            )

    @pytest.mark.parametrize(
        "delete_local_copy, expected_existence_of_local_copy",
        [(True, False), (False, True)],
    )
    @mock.patch(
        "airflow.providers.microsoft.azure.log.wasb_task_handler.WasbTaskHandler.wasb_write"
    )
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
        assert (
            os.path.exists(handler.handler.baseFilename)
            == expected_existence_of_local_copy
        )

    def test_filename_template_for_backward_compatibility(self):
        # filename_template arg support for running the latest provider on airflow 2
        WasbTaskHandler(
            base_log_folder=self.local_log_location,
            wasb_log_folder=self.wasb_log_folder,
            wasb_container=self.container_name,
            delete_local_copy=True,
            filename_template=None,
        )
