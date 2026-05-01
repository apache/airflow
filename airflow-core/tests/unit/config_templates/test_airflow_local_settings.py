#
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

import importlib
import inspect
import json
from unittest import mock

import pytest

from airflow.config_templates import airflow_local_settings
from airflow.utils.log.file_task_handler import FileTaskHandler

from tests_common.test_utils.config import conf_vars

REMOTE_IO_PROVIDERS = [
    ("s3://bucket/path", "airflow.providers.amazon.aws.log.s3_task_handler.S3RemoteLogIO"),
    ("wasb-logs", "airflow.providers.microsoft.azure.log.wasb_task_handler.WasbRemoteLogIO"),
    ("gs://bucket/path", "airflow.providers.google.cloud.log.gcs_task_handler.GCSRemoteLogIO"),
    (
        "cloudwatch://arn:aws:logs:us-east-1:0:log-group:foo",
        "airflow.providers.amazon.aws.log.cloudwatch_task_handler.CloudWatchRemoteLogIO",
    ),
    ("oss://bucket/path", "airflow.providers.alibaba.cloud.log.oss_task_handler.OSSRemoteLogIO"),
    ("hdfs://host/path", "airflow.providers.apache.hdfs.log.hdfs_task_handler.HdfsRemoteLogIO"),
]
REMOTE_IO_IDS = ["s3", "wasb", "gcs", "cloudwatch", "oss", "hdfs"]


@pytest.fixture
def restore_local_settings():
    yield
    importlib.reload(airflow_local_settings)


@pytest.mark.parametrize(("remote_base", "remote_io_path"), REMOTE_IO_PROVIDERS, ids=REMOTE_IO_IDS)
def test_io_kwargs_forwarded_to_remote_log_io(remote_base, remote_io_path, restore_local_settings):
    """IO-level kwargs reach the RemoteLogIO constructor and don't leak into the handler config."""
    pytest.importorskip(remote_io_path.rsplit(".", 1)[0])
    io_kwargs = {"remote_base": "ignored", "custom_key": "v"}
    with (
        mock.patch(remote_io_path) as mock_remote_io,
        conf_vars(
            {
                ("logging", "remote_logging"): "True",
                ("logging", "remote_base_log_folder"): remote_base,
                ("logging", "remote_task_handler_kwargs"): json.dumps(io_kwargs),
            }
        ),
    ):
        importlib.reload(airflow_local_settings)
        task_cfg = airflow_local_settings.DEFAULT_LOGGING_CONFIG["handlers"]["task"]
        for k in io_kwargs:
            assert k not in task_cfg, f"IO kwarg {k!r} leaked into task handler config"

        for k, v in io_kwargs.items():
            assert mock_remote_io.call_args.kwargs[k] == v


@pytest.mark.parametrize(("remote_base", "remote_io_path"), REMOTE_IO_PROVIDERS, ids=REMOTE_IO_IDS)
def test_handler_kwargs_reach_file_task_handler(remote_base, remote_io_path, restore_local_settings):
    """Handler-level kwargs (max_bytes, backup_count, delay) reach the FileTaskHandler config."""
    pytest.importorskip(remote_io_path.rsplit(".", 1)[0])
    handler_kwargs = {"max_bytes": 5_000_000, "backup_count": 5}
    with (
        mock.patch(remote_io_path) as mock_remote_io,
        conf_vars(
            {
                ("logging", "remote_logging"): "True",
                ("logging", "remote_base_log_folder"): remote_base,
                ("logging", "remote_task_handler_kwargs"): json.dumps(handler_kwargs),
            }
        ),
    ):
        importlib.reload(airflow_local_settings)
        task_cfg = airflow_local_settings.DEFAULT_LOGGING_CONFIG["handlers"]["task"]
        for k, v in handler_kwargs.items():
            assert task_cfg[k] == v, f"Handler kwarg {k!r} not found in task handler config"

        for k in handler_kwargs:
            assert k not in mock_remote_io.call_args.kwargs, (
                f"Handler kwarg {k!r} leaked into RemoteLogIO constructor"
            )


@pytest.mark.parametrize(("remote_base", "remote_io_path"), REMOTE_IO_PROVIDERS, ids=REMOTE_IO_IDS)
def test_mixed_kwargs_split_correctly(remote_base, remote_io_path, restore_local_settings):
    """When both handler and IO kwargs are present, each goes to the right place."""
    pytest.importorskip(remote_io_path.rsplit(".", 1)[0])
    mixed_kwargs = {"max_bytes": 5_000_000, "backup_count": 5, "custom_io_key": "val"}
    with (
        mock.patch(remote_io_path) as mock_remote_io,
        conf_vars(
            {
                ("logging", "remote_logging"): "True",
                ("logging", "remote_base_log_folder"): remote_base,
                ("logging", "remote_task_handler_kwargs"): json.dumps(mixed_kwargs),
            }
        ),
    ):
        importlib.reload(airflow_local_settings)
        task_cfg = airflow_local_settings.DEFAULT_LOGGING_CONFIG["handlers"]["task"]

        assert task_cfg["max_bytes"] == 5_000_000
        assert task_cfg["backup_count"] == 5
        assert "custom_io_key" not in task_cfg

        assert mock_remote_io.call_args.kwargs["custom_io_key"] == "val"
        assert "max_bytes" not in mock_remote_io.call_args.kwargs
        assert "backup_count" not in mock_remote_io.call_args.kwargs


def test_file_handler_params_introspected_correctly():
    """The introspected FileTaskHandler params include the expected kwargs."""
    init_params = set(inspect.signature(FileTaskHandler.__init__).parameters) - {"self", "base_log_folder"}
    assert {"max_bytes", "backup_count", "delay"} <= init_params
