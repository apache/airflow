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

import json
import logging
import sys
import warnings
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.sdk._shared.providers_discovery import (
    HookClassProvider,
    LazyDictWithCache,
    ProviderInfo,
)
from airflow.sdk.providers_manager_runtime import ProvidersManagerTaskRuntime

from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker, skip_if_not_on_main
from tests_common.test_utils.paths import AIRFLOW_ROOT_PATH

PY313 = sys.version_info >= (3, 13)


def test_cleanup_providers_manager_runtime(cleanup_providers_manager):
    """Check the cleanup provider manager functionality."""
    provider_manager = ProvidersManagerTaskRuntime()
    # Check by type name since symlinks create different module paths
    assert type(provider_manager.hooks).__name__ == "LazyDictWithCache"
    hooks = provider_manager.hooks
    ProvidersManagerTaskRuntime()._cleanup()
    assert not len(hooks)
    assert ProvidersManagerTaskRuntime().hooks is hooks


@skip_if_force_lowest_dependencies_marker
class TestProvidersManagerRuntime:
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog, cleanup_providers_manager_runtime):
        self._caplog = caplog

    def test_hooks_deprecation_warnings_generated(self):
        providers_manager = ProvidersManagerTaskRuntime()
        providers_manager._provider_dict["test-package"] = ProviderInfo(
            version="0.0.1",
            data={"hook-class-names": ["airflow.providers.sftp.hooks.sftp.SFTPHook"]},
        )
        with pytest.warns(expected_warning=DeprecationWarning, match="hook-class-names") as warning_records:
            providers_manager._discover_hooks()
        assert warning_records

    def test_hooks_deprecation_warnings_not_generated(self):
        with warnings.catch_warnings(record=True) as warning_records:
            providers_manager = ProvidersManagerTaskRuntime()
            providers_manager._provider_dict["apache-airflow-providers-sftp"] = ProviderInfo(
                version="0.0.1",
                data={
                    "hook-class-names": ["airflow.providers.sftp.hooks.sftp.SFTPHook"],
                    "connection-types": [
                        {
                            "hook-class-name": "airflow.providers.sftp.hooks.sftp.SFTPHook",
                            "connection-type": "sftp",
                        }
                    ],
                },
            )
            providers_manager._discover_hooks()
        assert [w.message for w in warning_records if "hook-class-names" in str(w.message)] == []

    def test_warning_logs_generated(self):
        providers_manager = ProvidersManagerTaskRuntime()
        providers_manager._hooks_lazy_dict = LazyDictWithCache()
        with self._caplog.at_level(logging.WARNING):
            providers_manager._provider_dict["apache-airflow-providers-sftp"] = ProviderInfo(
                version="0.0.1",
                data={
                    "hook-class-names": ["airflow.providers.sftp.hooks.sftp.SFTPHook"],
                    "connection-types": [
                        {
                            "hook-class-name": "airflow.providers.sftp.hooks.sftp.SFTPHook",
                            "connection-type": "wrong-connection-type",
                        }
                    ],
                },
            )
            providers_manager._discover_hooks()
            _ = providers_manager._hooks_lazy_dict["wrong-connection-type"]
        assert len(self._caplog.entries) == 1
        assert "Inconsistency!" in self._caplog[0]["event"]
        assert "sftp" not in providers_manager._hooks_lazy_dict

    def test_warning_logs_not_generated(self):
        with self._caplog.at_level(logging.WARNING):
            providers_manager = ProvidersManagerTaskRuntime()
            providers_manager._provider_dict["apache-airflow-providers-sftp"] = ProviderInfo(
                version="0.0.1",
                data={
                    "hook-class-names": ["airflow.providers.sftp.hooks.sftp.SFTPHook"],
                    "connection-types": [
                        {
                            "hook-class-name": "airflow.providers.sftp.hooks.sftp.SFTPHook",
                            "connection-type": "sftp",
                        }
                    ],
                },
            )
            providers_manager._discover_hooks()
            _ = providers_manager._hooks_lazy_dict["sftp"]
        assert not self._caplog.records
        assert "sftp" in providers_manager.hooks

    def test_already_registered_conn_type_in_provide(self):
        with self._caplog.at_level(logging.WARNING):
            providers_manager = ProvidersManagerTaskRuntime()
            providers_manager._provider_dict["apache-airflow-providers-dummy"] = ProviderInfo(
                version="0.0.1",
                data={
                    "connection-types": [
                        {
                            "hook-class-name": "airflow.providers.dummy.hooks.dummy.DummyHook",
                            "connection-type": "dummy",
                        },
                        {
                            "hook-class-name": "airflow.providers.dummy.hooks.dummy.DummyHook2",
                            "connection-type": "dummy",
                        },
                    ],
                },
            )
            providers_manager._discover_hooks()
            _ = providers_manager._hooks_lazy_dict["dummy"]
        assert len(self._caplog.records) == 1
        msg = self._caplog.messages[0]
        assert msg.startswith("The connection type 'dummy' is already registered")
        assert (
            "different class names: 'airflow.providers.dummy.hooks.dummy.DummyHook'"
            " and 'airflow.providers.dummy.hooks.dummy.DummyHook2'."
        ) in msg

    def test_hooks(self):
        with warnings.catch_warnings(record=True) as warning_records:
            with self._caplog.at_level(logging.WARNING):
                provider_manager = ProvidersManagerTaskRuntime()
                connections_list = list(provider_manager.hooks.keys())
                assert len(connections_list) > 60
        if len(self._caplog.records) != 0:
            for record in self._caplog.records:
                print(record.message, file=sys.stderr)
                print(record.exc_info, file=sys.stderr)
            raise AssertionError("There are warnings generated during hook imports. Please fix them")
        assert [w.message for w in warning_records if "hook-class-names" in str(w.message)] == []

    @skip_if_not_on_main
    @pytest.mark.execution_timeout(150)
    def test_hook_values(self):
        provider_dependencies = json.loads(
            (AIRFLOW_ROOT_PATH / "generated" / "provider_dependencies.json").read_text()
        )
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
        excluded_providers: list[str] = []
        for provider_name, provider_info in provider_dependencies.items():
            if python_version in provider_info.get("excluded-python-versions", []):
                excluded_providers.append(f"apache-airflow-providers-{provider_name.replace('.', '-')}")
        with warnings.catch_warnings(record=True) as warning_records:
            with self._caplog.at_level(logging.WARNING):
                provider_manager = ProvidersManagerTaskRuntime()
                connections_list = list(provider_manager.hooks.values())
                assert len(connections_list) > 60
        if len(self._caplog.records) != 0:
            real_warning_count = 0
            for record in self._caplog.entries:
                # When there is error importing provider that is excluded the provider name is in the message
                if any(excluded_provider in record["event"] for excluded_provider in excluded_providers):
                    continue
                print(record["event"], file=sys.stderr)
                print(record.get("exc_info"), file=sys.stderr)
                real_warning_count += 1
            if real_warning_count:
                if PY313:
                    only_ydb_and_yandexcloud_warnings = True
                    for record in warning_records:
                        if "ydb" in str(record.message) or "yandexcloud" in str(record.message):
                            continue
                        only_ydb_and_yandexcloud_warnings = False
                    if only_ydb_and_yandexcloud_warnings:
                        print(
                            "Only warnings from ydb and yandexcloud providers are generated, "
                            "which is expected in Python 3.13+",
                            file=sys.stderr,
                        )
                        return
                raise AssertionError("There are warnings generated during hook imports. Please fix them")
        assert [w.message for w in warning_records if "hook-class-names" in str(w.message)] == []

    @patch("airflow.sdk.providers_manager_runtime.import_string")
    def test_optional_feature_no_warning(self, mock_importlib_import_string):
        with self._caplog.at_level(logging.WARNING):
            mock_importlib_import_string.side_effect = AirflowOptionalProviderFeatureException()
            providers_manager = ProvidersManagerTaskRuntime()
            providers_manager._hook_provider_dict["test_connection"] = HookClassProvider(
                package_name="test_package", hook_class_name="HookClass"
            )
            providers_manager._import_hook(
                hook_class_name=None, provider_info=None, package_name=None, connection_type="test_connection"
            )
            assert self._caplog.messages == []

    @patch("airflow.sdk.providers_manager_runtime.import_string")
    def test_optional_feature_debug(self, mock_importlib_import_string):
        with self._caplog.at_level(logging.INFO):
            mock_importlib_import_string.side_effect = AirflowOptionalProviderFeatureException()
            providers_manager = ProvidersManagerTaskRuntime()
            providers_manager._hook_provider_dict["test_connection"] = HookClassProvider(
                package_name="test_package", hook_class_name="HookClass"
            )
            providers_manager._import_hook(
                hook_class_name=None, provider_info=None, package_name=None, connection_type="test_connection"
            )
            assert self._caplog.messages == [
                "Optional provider feature disabled when importing 'HookClass' from 'test_package' package"
            ]
