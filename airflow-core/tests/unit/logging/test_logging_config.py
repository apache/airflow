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

import sys
from types import SimpleNamespace
from unittest import mock

import pytest

from airflow._shared.logging.factory import DEFAULT_LOGGING_CONFIG_PATH
from airflow.logging_config import (
    _ActiveLoggingConfig,
    _get_logging_config,
    _load_logging_config,
    get_default_remote_conn_id,
    get_remote_task_log,
    load_logging_config,
)


@pytest.fixture(autouse=True)
def _reset_active_logging_config(monkeypatch):
    monkeypatch.setattr(_ActiveLoggingConfig, "logging_config_loaded", False, raising=False)
    monkeypatch.setattr(_ActiveLoggingConfig, "remote_task_log", None, raising=False)
    monkeypatch.setattr(_ActiveLoggingConfig, "default_remote_conn_id", None, raising=False)


class TestGetLoggingConfig:
    def test_returns_default_logging_dict(self):
        from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

        config = _get_logging_config()
        assert config == DEFAULT_LOGGING_CONFIG

    def test_user_defined_dict_is_imported(self, monkeypatch):
        fake_module = "fake_user_logging_module_for_test"
        custom = {"version": 1, "marker": "user-defined"}
        monkeypatch.setitem(sys.modules, fake_module, SimpleNamespace(LOGGING_CONFIG=custom))
        with mock.patch("airflow.logging_config.conf") as mocked_conf:
            mocked_conf.get.return_value = f"{fake_module}.LOGGING_CONFIG"
            config = _get_logging_config()
        assert config is custom

    @pytest.mark.parametrize(
        "logging_class_path",
        [pytest.param("", id="empty-string"), pytest.param(None, id="none")],
    )
    def test_falsy_logging_class_path_falls_back_to_default(self, logging_class_path):
        from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

        with mock.patch("airflow.logging_config.conf") as mocked_conf:
            mocked_conf.get.return_value = logging_class_path
            config = _get_logging_config()
        assert config == DEFAULT_LOGGING_CONFIG

    def test_raises_import_error_when_path_invalid(self):
        with mock.patch("airflow.logging_config.conf") as mocked_conf:
            mocked_conf.get.return_value = "nonexistent.module.LOGGING"
            with pytest.raises(ImportError, match="Unable to load custom logging config"):
                _get_logging_config()

    def test_raises_import_error_when_value_not_dict(self, monkeypatch):
        fake_module = "fake_user_logging_module_not_dict"
        monkeypatch.setitem(sys.modules, fake_module, SimpleNamespace(LOGGING_CONFIG="not-a-dict"))
        with mock.patch("airflow.logging_config.conf") as mocked_conf:
            mocked_conf.get.return_value = f"{fake_module}.LOGGING_CONFIG"
            with pytest.raises(ImportError, match="Logging Config should be of dict type"):
                _get_logging_config()


class TestLoadLoggingConfigPrivate:
    def test_caches_resolved_remote_handler_and_conn_id(self):
        sentinel_handler = object()
        with (
            mock.patch("airflow.logging_config.resolve_remote_task_log") as mock_resolve,
            mock.patch("airflow.providers_manager.ProvidersManager") as mock_pm,
        ):
            mock_resolve.return_value = (sentinel_handler, "my_conn")
            _load_logging_config()

        assert _ActiveLoggingConfig.logging_config_loaded is True
        assert _ActiveLoggingConfig.remote_task_log is sentinel_handler
        assert _ActiveLoggingConfig.default_remote_conn_id == "my_conn"
        # resolve_remote_task_log must be called with the core providers manager.
        mock_resolve.assert_called_once()
        assert mock_resolve.call_args.kwargs["providers_manager"] is mock_pm.return_value

    def test_caches_none_when_resolver_returns_nothing(self):
        with (
            mock.patch("airflow.logging_config.resolve_remote_task_log") as mock_resolve,
            mock.patch("airflow.providers_manager.ProvidersManager"),
        ):
            mock_resolve.return_value = (None, None)
            _load_logging_config()

        assert _ActiveLoggingConfig.logging_config_loaded is True
        assert _ActiveLoggingConfig.remote_task_log is None
        assert _ActiveLoggingConfig.default_remote_conn_id is None


class TestLoadLoggingConfigDeprecated:
    def test_emits_deprecation_warning_and_returns_tuple(self):
        sentinel_handler = object()
        with (
            mock.patch("airflow.logging_config.resolve_remote_task_log") as mock_resolve,
            mock.patch("airflow.providers_manager.ProvidersManager"),
        ):
            mock_resolve.return_value = (sentinel_handler, "my_conn")
            with pytest.warns(DeprecationWarning, match="load_logging_config is deprecated"):
                logging_config, logging_class_path = load_logging_config()

        assert isinstance(logging_config, dict)
        assert logging_class_path == DEFAULT_LOGGING_CONFIG_PATH
        # The deprecated wrapper still primes the remote handler cache.
        assert _ActiveLoggingConfig.remote_task_log is sentinel_handler

    def test_returns_user_logging_class_path(self, monkeypatch):
        fake_module = "fake_user_logging_module_deprecated"
        custom = {"version": 1}
        custom_path = f"{fake_module}.LOGGING_CONFIG"
        monkeypatch.setitem(sys.modules, fake_module, SimpleNamespace(LOGGING_CONFIG=custom))

        with (
            mock.patch("airflow.logging_config.conf") as mocked_conf,
            mock.patch("airflow.logging_config.resolve_remote_task_log") as mock_resolve,
            mock.patch("airflow.providers_manager.ProvidersManager"),
        ):
            mocked_conf.get.return_value = custom_path
            mock_resolve.return_value = (None, None)
            with pytest.warns(DeprecationWarning, match="load_logging_config is deprecated"):
                _, logging_class_path = load_logging_config()

        assert logging_class_path == custom_path


class TestGetRemoteTaskLog:
    def test_returns_cached_value_without_reload(self):
        sentinel = object()
        _ActiveLoggingConfig.set(sentinel, None)
        with mock.patch("airflow.logging_config._load_logging_config") as mock_load:
            result = get_remote_task_log()
        assert result is sentinel
        mock_load.assert_not_called()

    def test_triggers_load_when_not_loaded(self):
        sentinel = object()

        def _fake_load():
            _ActiveLoggingConfig.set(sentinel, None)

        with mock.patch("airflow.logging_config._load_logging_config", side_effect=_fake_load) as mock_load:
            result = get_remote_task_log()
        mock_load.assert_called_once()
        assert result is sentinel


class TestGetDefaultRemoteConnId:
    def test_prefers_explicit_conf_value(self):
        with (
            mock.patch("airflow.logging_config.conf") as mocked_conf,
            mock.patch("airflow.logging_config._load_logging_config") as mock_load,
        ):
            mocked_conf.get.return_value = "explicit_conn"
            assert get_default_remote_conn_id() == "explicit_conn"
        mock_load.assert_not_called()

    def test_falls_back_to_cached_default(self):
        def _fake_load():
            _ActiveLoggingConfig.set(None, "cached_conn")

        with (
            mock.patch("airflow.logging_config.conf") as mocked_conf,
            mock.patch("airflow.logging_config._load_logging_config", side_effect=_fake_load),
        ):
            mocked_conf.get.return_value = None
            assert get_default_remote_conn_id() == "cached_conn"

    def test_skips_reload_when_already_loaded(self):
        _ActiveLoggingConfig.set(None, "cached_conn")
        with (
            mock.patch("airflow.logging_config.conf") as mocked_conf,
            mock.patch("airflow.logging_config._load_logging_config") as mock_load,
        ):
            mocked_conf.get.return_value = None
            assert get_default_remote_conn_id() == "cached_conn"
        mock_load.assert_not_called()
