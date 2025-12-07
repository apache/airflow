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
import re
import sys

PY313 = sys.version_info >= (3, 13)
import warnings
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers_manager import (
    DialectInfo,
    HookClassProvider,
    LazyDictWithCache,
    PluginInfo,
    ProviderInfo,
    ProvidersManager,
)

from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker, skip_if_not_on_main
from tests_common.test_utils.paths import AIRFLOW_ROOT_PATH


def test_cleanup_providers_manager(cleanup_providers_manager):
    """Check the cleanup provider manager functionality."""
    provider_manager = ProvidersManager()
    assert isinstance(provider_manager.hooks, LazyDictWithCache)
    hooks = provider_manager.hooks
    ProvidersManager()._cleanup()
    assert not len(hooks)
    assert ProvidersManager().hooks is hooks


@skip_if_force_lowest_dependencies_marker
class TestProviderManager:
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog, cleanup_providers_manager):
        self._caplog = caplog

    def test_providers_are_loaded(self):
        with self._caplog.at_level(logging.WARNING):
            self._caplog.clear()
            provider_manager = ProvidersManager()
            provider_list = list(provider_manager.providers.keys())
            # No need to sort the list - it should be sorted alphabetically !
            for provider in provider_list:
                package_name = provider_manager.providers[provider].data["package-name"]
                version = provider_manager.providers[provider].version
                assert re.search(r"[0-9]*\.[0-9]*\.[0-9]*.*", version)
                assert package_name == provider
            # just a coherence check - no exact number as otherwise we would have to update
            # several tests if we add new connections/provider which is not ideal
            assert len(provider_list) > 65
            assert self._caplog.records == []

    def test_hooks_deprecation_warnings_generated(self):
        providers_manager = ProvidersManager()
        providers_manager._provider_dict["test-package"] = ProviderInfo(
            version="0.0.1",
            data={"hook-class-names": ["airflow.providers.sftp.hooks.sftp.SFTPHook"]},
        )
        with pytest.warns(expected_warning=DeprecationWarning, match="hook-class-names") as warning_records:
            providers_manager._discover_hooks()
        assert warning_records

    def test_hooks_deprecation_warnings_not_generated(self):
        with warnings.catch_warnings(record=True) as warning_records:
            providers_manager = ProvidersManager()
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
        providers_manager = ProvidersManager()
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
        assert "sftp" not in providers_manager.hooks

    def test_warning_logs_not_generated(self):
        with self._caplog.at_level(logging.WARNING):
            providers_manager = ProvidersManager()
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
            providers_manager = ProvidersManager()
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

    def test_providers_manager_register_plugins(self):
        providers_manager = ProvidersManager()
        providers_manager._provider_dict = LazyDictWithCache()
        providers_manager._provider_dict["apache-airflow-providers-apache-hive"] = ProviderInfo(
            version="0.0.1",
            data={
                "plugins": [
                    {
                        "name": "plugin1",
                        "plugin-class": "airflow.providers.apache.hive.plugins.hive.HivePlugin",
                    }
                ]
            },
        )
        providers_manager._discover_plugins()
        assert len(providers_manager._plugins_set) == 1
        assert providers_manager._plugins_set.pop() == PluginInfo(
            name="plugin1",
            plugin_class="airflow.providers.apache.hive.plugins.hive.HivePlugin",
            provider_name="apache-airflow-providers-apache-hive",
        )

    def test_providers_manager_register_dialects(self):
        providers_manager = ProvidersManager()
        providers_manager._provider_dict = LazyDictWithCache()
        providers_manager._provider_dict["airflow.providers.common.sql"] = ProviderInfo(
            version="1.19.0",
            data={
                "dialects": [
                    {
                        "dialect-type": "default",
                        "dialect-class-name": "airflow.providers.common.sql.dialects.dialect.Dialect",
                    }
                ]
            },
        )
        providers_manager._discover_hooks()
        assert len(providers_manager._dialect_provider_dict) == 1
        assert providers_manager._dialect_provider_dict.popitem() == (
            "default",
            DialectInfo(
                name="default",
                dialect_class_name="airflow.providers.common.sql.dialects.dialect.Dialect",
                provider_name="airflow.providers.common.sql",
            ),
        )

    def test_hooks(self):
        with warnings.catch_warnings(record=True) as warning_records:
            with self._caplog.at_level(logging.WARNING):
                provider_manager = ProvidersManager()
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
                provider_manager = ProvidersManager()
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

    def test_connection_form_widgets(self):
        provider_manager = ProvidersManager()
        connections_form_widgets = list(provider_manager.connection_form_widgets.keys())
        # Connection form widgets use flask_appbuilder widgets, so they're only available when it's installed
        try:
            import flask_appbuilder  # noqa: F401

            assert len(connections_form_widgets) > 29
        except ImportError:
            assert len(connections_form_widgets) == 0

    def test_field_behaviours(self):
        provider_manager = ProvidersManager()
        connections_with_field_behaviours = list(provider_manager.field_behaviours.keys())
        # Field behaviours are often related to connection forms, only available when flask_appbuilder is installed
        try:
            import flask_appbuilder  # noqa: F401

            assert len(connections_with_field_behaviours) > 16
        except ImportError:
            assert len(connections_with_field_behaviours) == 0

    def test_extra_links(self):
        provider_manager = ProvidersManager()
        extra_link_class_names = list(provider_manager.extra_links_class_names)
        assert len(extra_link_class_names) > 6

    def test_logging(self):
        provider_manager = ProvidersManager()
        logging_class_names = list(provider_manager.logging_class_names)
        assert len(logging_class_names) > 5

    def test_secrets_backends(self):
        provider_manager = ProvidersManager()
        secrets_backends_class_names = list(provider_manager.secrets_backend_class_names)
        assert len(secrets_backends_class_names) > 4

    def test_trigger(self):
        provider_manager = ProvidersManager()
        trigger_class_names = list(provider_manager.trigger)
        assert len(trigger_class_names) > 10

    def test_notification(self):
        provider_manager = ProvidersManager()
        notification_class_names = list(provider_manager.notification)
        assert len(notification_class_names) > 5

    def test_auth_managers(self):
        provider_manager = ProvidersManager()
        auth_manager_class_names = list(provider_manager.auth_managers)
        assert len(auth_manager_class_names) > 0

    def test_dialects(self):
        provider_manager = ProvidersManager()
        dialect_class_names = list(provider_manager.dialects)
        assert len(dialect_class_names) == 3
        assert dialect_class_names == ["default", "mssql", "postgresql"]

    @patch("airflow.providers_manager.import_string")
    def test_optional_feature_no_warning(self, mock_importlib_import_string):
        with self._caplog.at_level(logging.WARNING):
            mock_importlib_import_string.side_effect = AirflowOptionalProviderFeatureException()
            providers_manager = ProvidersManager()
            providers_manager._hook_provider_dict["test_connection"] = HookClassProvider(
                package_name="test_package", hook_class_name="HookClass"
            )
            providers_manager._import_hook(
                hook_class_name=None, provider_info=None, package_name=None, connection_type="test_connection"
            )
            assert self._caplog.messages == []

    @patch("airflow.providers_manager.import_string")
    def test_optional_feature_debug(self, mock_importlib_import_string):
        with self._caplog.at_level(logging.INFO):
            mock_importlib_import_string.side_effect = AirflowOptionalProviderFeatureException()
            providers_manager = ProvidersManager()
            providers_manager._hook_provider_dict["test_connection"] = HookClassProvider(
                package_name="test_package", hook_class_name="HookClass"
            )
            providers_manager._import_hook(
                hook_class_name=None, provider_info=None, package_name=None, connection_type="test_connection"
            )
            assert self._caplog.messages == [
                "Optional provider feature disabled when importing 'HookClass' from 'test_package' package"
            ]


@pytest.mark.parametrize(
    ("value", "expected_outputs"),
    [
        ("a", "a"),
        (1, 1),
        (None, None),
        (lambda: 0, 0),
        (lambda: None, None),
        (lambda: "z", "z"),
    ],
)
def test_lazy_cache_dict_resolving(value, expected_outputs):
    lazy_cache_dict = LazyDictWithCache()
    lazy_cache_dict["key"] = value
    assert lazy_cache_dict["key"] == expected_outputs
    # Retrieve it again to see if it is correctly returned again
    assert lazy_cache_dict["key"] == expected_outputs


def test_lazy_cache_dict_raises_error():
    def raise_method():
        raise RuntimeError("test")

    lazy_cache_dict = LazyDictWithCache()
    lazy_cache_dict["key"] = raise_method
    with pytest.raises(RuntimeError, match="test"):
        _ = lazy_cache_dict["key"]


def test_lazy_cache_dict_del_item():
    lazy_cache_dict = LazyDictWithCache()

    def answer():
        return 42

    lazy_cache_dict["spam"] = answer
    assert "spam" in lazy_cache_dict._raw_dict
    assert "spam" not in lazy_cache_dict._resolved  # Not resoled yet
    assert lazy_cache_dict["spam"] == 42
    assert "spam" in lazy_cache_dict._resolved
    del lazy_cache_dict["spam"]
    assert "spam" not in lazy_cache_dict._raw_dict
    assert "spam" not in lazy_cache_dict._resolved

    lazy_cache_dict["foo"] = answer
    assert lazy_cache_dict["foo"] == 42
    assert "foo" in lazy_cache_dict._resolved
    # Emulate some mess in data, e.g. value from `_raw_dict` deleted but not from `_resolved`
    del lazy_cache_dict._raw_dict["foo"]
    assert "foo" in lazy_cache_dict._resolved
    with pytest.raises(KeyError):
        # Error expected here, but we still expect to remove also record into `resolved`
        del lazy_cache_dict["foo"]
    assert "foo" not in lazy_cache_dict._resolved

    lazy_cache_dict["baz"] = answer
    # Key in `_resolved` not created yet
    assert "baz" in lazy_cache_dict._raw_dict
    assert "baz" not in lazy_cache_dict._resolved
    del lazy_cache_dict._raw_dict["baz"]
    assert "baz" not in lazy_cache_dict._raw_dict
    assert "baz" not in lazy_cache_dict._resolved


def test_lazy_cache_dict_clear():
    def answer():
        return 42

    lazy_cache_dict = LazyDictWithCache()
    assert len(lazy_cache_dict) == 0
    lazy_cache_dict["spam"] = answer
    lazy_cache_dict["foo"] = answer
    lazy_cache_dict["baz"] = answer

    assert len(lazy_cache_dict) == 3
    assert len(lazy_cache_dict._raw_dict) == 3
    assert not lazy_cache_dict._resolved
    assert lazy_cache_dict["spam"] == 42
    assert len(lazy_cache_dict._resolved) == 1
    # Emulate some mess in data, contain some data into the `_resolved`
    lazy_cache_dict._resolved.add("biz")
    assert len(lazy_cache_dict) == 3
    assert len(lazy_cache_dict._resolved) == 2
    # And finally cleanup everything
    lazy_cache_dict.clear()
    assert len(lazy_cache_dict) == 0
    assert not lazy_cache_dict._raw_dict
    assert not lazy_cache_dict._resolved
