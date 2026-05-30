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

from unittest import mock

import pytest

from airflow.logging_config import (
    DEFAULT_LOGGING_CONFIG_PATH,
    _ActiveLoggingConfig,
    _warn_if_missing_remote_task_log,
)


class TestWarnIfMissingRemoteTaskLog:
    @pytest.fixture(autouse=True)
    def _reset_active_logging_config(self, monkeypatch):
        monkeypatch.setattr(_ActiveLoggingConfig, "remote_task_log", None, raising=False)
        monkeypatch.setattr(_ActiveLoggingConfig, "logging_config_loaded", True, raising=False)

    def test_warns_when_user_module_missing_remote_task_log_and_remote_logging_enabled(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW__LOGGING__REMOTE_LOGGING", "True")
        with mock.patch("airflow.logging_config.log") as mock_log:
            _warn_if_missing_remote_task_log("my_pkg.custom_settings.LOGGING_CONFIG")
        mock_log.warning.assert_called_once()
        assert "my_pkg.custom_settings" in mock_log.warning.call_args.args

    def test_no_warning_when_using_fallback_path(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW__LOGGING__REMOTE_LOGGING", "True")
        with mock.patch("airflow.logging_config.log") as mock_log:
            _warn_if_missing_remote_task_log(DEFAULT_LOGGING_CONFIG_PATH)
        mock_log.warning.assert_not_called()

    def test_no_warning_when_remote_logging_disabled(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW__LOGGING__REMOTE_LOGGING", "False")
        with mock.patch("airflow.logging_config.log") as mock_log:
            _warn_if_missing_remote_task_log("my_pkg.custom_settings.LOGGING_CONFIG")
        mock_log.warning.assert_not_called()

    def test_no_warning_when_remote_task_log_is_set(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW__LOGGING__REMOTE_LOGGING", "True")
        monkeypatch.setattr(_ActiveLoggingConfig, "remote_task_log", object(), raising=False)
        with mock.patch("airflow.logging_config.log") as mock_log:
            _warn_if_missing_remote_task_log("my_pkg.custom_settings.LOGGING_CONFIG")
        mock_log.warning.assert_not_called()

    def test_no_warning_when_empty_logging_class_path(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW__LOGGING__REMOTE_LOGGING", "True")
        with mock.patch("airflow.logging_config.log") as mock_log:
            _warn_if_missing_remote_task_log("")
        mock_log.warning.assert_not_called()
