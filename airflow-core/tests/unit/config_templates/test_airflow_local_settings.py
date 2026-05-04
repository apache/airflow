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
import json
from unittest import mock

import pytest

from airflow.config_templates import airflow_local_settings

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def restore_local_settings():
    yield
    importlib.reload(airflow_local_settings)


@pytest.mark.parametrize(
    ("remote_base", "remote_io_path"),
    [
        ("s3://bucket/path", "airflow.providers.amazon.aws.log.s3_task_handler.S3RemoteLogIO"),
        ("wasb-logs", "airflow.providers.microsoft.azure.log.wasb_task_handler.WasbRemoteLogIO"),
        ("gs://bucket/path", "airflow.providers.google.cloud.log.gcs_task_handler.GCSRemoteLogIO"),
        (
            "cloudwatch://arn:aws:logs:us-east-1:0:log-group:foo",
            "airflow.providers.amazon.aws.log.cloudwatch_task_handler.CloudWatchRemoteLogIO",
        ),
        ("oss://bucket/path", "airflow.providers.alibaba.cloud.log.oss_task_handler.OSSRemoteLogIO"),
        ("hdfs://host/path", "airflow.providers.apache.hdfs.log.hdfs_task_handler.HdfsRemoteLogIO"),
    ],
    ids=["s3", "wasb", "gcs", "cloudwatch", "oss", "hdfs"],
)
def test_remote_task_handler_kwargs_not_leaked_to_local_task_handler(
    remote_base, remote_io_path, restore_local_settings
):
    """Verify remote_task_handler_kwargs are passed to RemoteLogIO and not leaked to FileTaskHandler."""
    pytest.importorskip(remote_io_path.rsplit(".", 1)[0])
    user_kwargs = {"remote_base": "ignored", "custom_key": "v"}
    with (
        mock.patch(remote_io_path) as mock_remote_io,
        conf_vars(
            {
                ("logging", "remote_logging"): "True",
                ("logging", "remote_base_log_folder"): remote_base,
                ("logging", "remote_task_handler_kwargs"): json.dumps(user_kwargs),
            }
        ),
    ):
        importlib.reload(airflow_local_settings)
        task_cfg = airflow_local_settings.DEFAULT_LOGGING_CONFIG["handlers"]["task"]
        for k in user_kwargs:
            assert k not in task_cfg, f"{k!r} leaked into task handler for {remote_base}"

        # Verify kwargs were passed to REMOTE_TASK_LOG
        for k, v in user_kwargs.items():
            assert mock_remote_io.call_args.kwargs[k] == v
