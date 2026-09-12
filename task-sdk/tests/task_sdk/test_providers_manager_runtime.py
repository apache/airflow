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
from airflow.sdk.providers_manager_runtime import ProvidersManagerTaskRuntime, RemoteLoggingInfo

from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker, skip_if_not_on_main
from tests_common.test_utils.paths import AIRFLOW_ROOT_PATH

PY313 = sys.version_info >= (3, 13)


class FakeRemoteLogIO:
    """Importable stub used by remote-logging discovery tests."""

    processors: tuple = ()

    @classmethod
    def from_config(cls):
        return cls()


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
        # 'wrong-connection-type' is also read back under a different name, so discovery
        # warns about that as well. Both are expected, and no others.
        assert len(self._caplog.entries) == 2
        assert sum("Inconsistency!" in entry["event"] for entry in self._caplog.entries) == 1
        assert (
            sum("read back under a different name" in entry["event"] for entry in self._caplog.entries) == 1
        )
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

    @staticmethod
    def _provider_declaring(*connection_types: str) -> ProviderInfo:
        return ProviderInfo(
            version="0.0.1",
            data={
                "connection-types": [
                    {
                        "hook-class-name": f"airflow.providers.dummy.hooks.dummy.Hook{index}",
                        "connection-type": connection_type,
                    }
                    for index, connection_type in enumerate(connection_types)
                ],
            },
        )

    @pytest.mark.parametrize(
        ("declared", "read_back_as"),
        [
            # '-' is the URI-scheme encoding of '_', so it is decoded on the way back in.
            pytest.param("dummy-vendor", "dummy_vendor", id="hyphen"),
            # get_uri() lowercases the scheme.
            pytest.param("DummyVendor", "dummyvendor", id="uppercase"),
            # _normalize_conn_type also applies this alias, which is why the check asks it
            # rather than restating the separator rule.
            pytest.param("postgresql", "postgres", id="alias"),
        ],
    )
    def test_warns_about_a_connection_type_read_back_under_another_name(self, declared, read_back_as):
        """
        Such a type registers verbatim and resolves for a connection created through the UI,
        the REST API or the CLI, so nothing fails there. Every connection read from a URI or
        from JSON presents the decoded name instead and never reaches the hook.
        """
        with self._caplog.at_level(logging.WARNING):
            providers_manager = ProvidersManagerTaskRuntime()
            providers_manager._provider_dict["apache-airflow-providers-dummy"] = self._provider_declaring(
                declared
            )
            providers_manager._discover_hooks()

        entries = [
            entry for entry in self._caplog.entries if "read back under a different name" in entry["event"]
        ]
        assert len(entries) == 1
        assert entries[0]["connection_type"] == declared
        assert entries[0]["read_back_as"] == read_back_as
        assert entries[0]["package"] == "apache-airflow-providers-dummy"

    def test_warns_about_a_connection_type_a_uri_cannot_carry(self):
        """A type that is not a usable scheme is lost altogether rather than re-spelled."""
        with self._caplog.at_level(logging.WARNING):
            providers_manager = ProvidersManagerTaskRuntime()
            providers_manager._provider_dict["apache-airflow-providers-dummy"] = self._provider_declaring(
                "dummy vendor"
            )
            providers_manager._discover_hooks()

        entries = [
            entry
            for entry in self._caplog.entries
            if "cannot be carried in a connection URI" in entry["event"]
        ]
        assert len(entries) == 1
        assert entries[0]["connection_types"] == ["dummy vendor"]

    def test_warns_when_two_providers_declare_the_two_spellings_of_one_name(self):
        """
        This one resolves rather than failing, which is why get_hook() cannot report it: the
        connection is handed whichever hook holds the decoded name, so it can belong to the
        other provider.
        """
        with self._caplog.at_level(logging.WARNING):
            providers_manager = ProvidersManagerTaskRuntime()
            providers_manager._provider_dict["apache-airflow-providers-one"] = self._provider_declaring(
                "shared-name"
            )
            providers_manager._provider_dict["apache-airflow-providers-two"] = self._provider_declaring(
                "shared_name"
            )
            providers_manager._discover_hooks()

        entries = [entry for entry in self._caplog.entries if "read back under one name" in entry["event"]]
        assert len(entries) == 1
        assert entries[0]["connection_types"] == ["shared-name", "shared_name"]
        assert entries[0]["read_back_as"] == "shared_name"
        assert sorted(entries[0]["packages"]) == [
            "apache-airflow-providers-one",
            "apache-airflow-providers-two",
        ]

    def test_does_not_warn_about_a_connection_type_that_survives_being_stored(self):
        with self._caplog.at_level(logging.WARNING):
            providers_manager = ProvidersManagerTaskRuntime()
            providers_manager._provider_dict["apache-airflow-providers-dummy"] = self._provider_declaring(
                "dummy_vendor", "postgres", "s3", "a.b"
            )
            providers_manager._discover_hooks()

        assert not self._caplog.entries

    @pytest.mark.parametrize(
        "declared",
        [
            "dummy_vendor",
            "dummy-vendor",
            "DummyVendor",
            "postgres",
            "postgresql",
            "dummy vendor",
            "a.b",
            "a+b",
            "foo-bar_baz",
        ],
    )
    def test_stored_name_matches_a_real_connection_round_trip(self, declared):
        """
        The check models what get_uri() writes and what reading a connection back decodes,
        so it has to agree with actually doing it. This fails if either side changes.
        """
        from airflow.sdk.definitions.connection import Connection

        uri = Connection(conn_id="c", conn_type=declared, host="host").get_uri()

        assert ProvidersManagerTaskRuntime._connection_type_as_stored(declared) == (
            Connection.from_uri(uri, conn_id="c").conn_type
        )

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

    def test_already_initialized_provider_configs_emits_deprecation_warning(self):
        """Test that already_initialized_provider_configs emits a DeprecationWarning."""
        pm = ProvidersManagerTaskRuntime()
        with pytest.warns(DeprecationWarning, match="already_initialized_provider_configs.*deprecated"):
            pm.already_initialized_provider_configs

    def test_initialize_provider_configs_can_reload_sdk_conf(self):
        from airflow.sdk.configuration import conf

        providers_manager = ProvidersManagerTaskRuntime()
        provider_config = {
            "test_sdk_provider": {
                "description": "Provider config used in runtime tests.",
                "options": {
                    "test_option": {
                        "default": "provider-default",
                    }
                },
            }
        }

        def initialize_provider_configs() -> None:
            providers_manager._provider_dict["apache-airflow-providers-test-sdk"] = ProviderInfo(
                version="0.0.1",
                data={"config": provider_config},
            )
            with patch.object(providers_manager, "initialize_providers_list"):
                providers_manager.initialize_provider_configs()

        conf.invalidate_cache()
        try:
            initialize_provider_configs()
            assert conf.get("test_sdk_provider", "test_option") == "provider-default"

            providers_manager._cleanup()

            initialize_provider_configs()
            assert conf.get("test_sdk_provider", "test_option") == "provider-default"
        finally:
            conf.invalidate_cache()

    def test_register_remote_logging_by_scheme(self):
        providers_manager = ProvidersManagerTaskRuntime()
        providers_manager._provider_dict = LazyDictWithCache()
        providers_manager._provider_dict["fake.remote.logging"] = ProviderInfo(
            version="0.0.1",
            data={
                "remote-logging": [
                    {
                        "classpath": f"{__name__}.FakeRemoteLogIO",
                        "scheme": "fake",
                    }
                ]
            },
        )
        providers_manager._discover_remote_logging()
        assert len(providers_manager._remote_logging_info_list) == 1
        assert providers_manager._remote_logging_by_scheme["fake"] == RemoteLoggingInfo(
            classpath=f"{__name__}.FakeRemoteLogIO",
            scheme="fake",
            package_name="fake.remote.logging",
        )

    def test_register_remote_logging_duplicate_scheme_first_wins(self):
        providers_manager = ProvidersManagerTaskRuntime()
        providers_manager._provider_dict = LazyDictWithCache()
        providers_manager._provider_dict["fake.remote.logging.first"] = ProviderInfo(
            version="0.0.1",
            data={"remote-logging": [{"classpath": f"{__name__}.FakeRemoteLogIO", "scheme": "dup"}]},
        )
        providers_manager._provider_dict["fake.remote.logging.second"] = ProviderInfo(
            version="0.0.1",
            data={"remote-logging": [{"classpath": f"{__name__}.FakeRemoteLogIO", "scheme": "dup"}]},
        )
        providers_manager._discover_remote_logging()
        assert providers_manager._remote_logging_by_scheme["dup"].package_name == (
            "fake.remote.logging.first"
        )
        assert len(providers_manager._remote_logging_info_list) == 1
        assert providers_manager._remote_logging_info_list[0].package_name == "fake.remote.logging.first"

    def test_register_remote_logging_bad_class_filtered(self):
        providers_manager = ProvidersManagerTaskRuntime()
        providers_manager._provider_dict = LazyDictWithCache()
        providers_manager._provider_dict["fake.remote.logging"] = ProviderInfo(
            version="0.0.1",
            data={
                "remote-logging": [
                    {
                        "classpath": "fake.module.does.not.exist.FakeRemoteLogIO",
                        "scheme": "bad",
                    }
                ]
            },
        )
        providers_manager._discover_remote_logging()
        assert "bad" not in providers_manager._remote_logging_by_scheme
        assert providers_manager._remote_logging_info_list == []
