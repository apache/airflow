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

import logging
import re
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING

PY313 = sys.version_info >= (3, 13)
from unittest.mock import patch

import pytest

from airflow.providers_manager import (
    DialectInfo,
    LazyDictWithCache,
    PluginInfo,
    ProviderInfo,
    ProvidersManager,
)

from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker

if TYPE_CHECKING:
    from unittest.mock import MagicMock

    from airflow.cli.cli_config import CLICommand


def test_cleanup_providers_manager(cleanup_providers_manager):
    """Check the cleanup provider manager functionality."""
    provider_manager = ProvidersManager()
    assert isinstance(provider_manager.providers, dict)
    providers = provider_manager.providers
    assert len(providers) > 0

    ProvidersManager()._cleanup()

    # even after cleanup the singleton should return same instance but internal state is reset
    assert len(ProvidersManager().providers) > 0


@pytest.fixture
def yaml_ui_metadata_counts(cleanup_providers_manager):
    """Get counts of UI metadata defined in YAML (widgets, behaviours)."""
    pm = ProvidersManager()
    pm.initialize_providers_list()

    widgets = 0
    behaviours = 0
    for provider in pm._provider_dict.values():
        for conn_config in provider.data.get("connection-types", []):
            widgets += len(conn_config.get("conn-fields", {}))
            if conn_config.get("ui-field-behaviour"):
                behaviours += 1

    return widgets, behaviours


@skip_if_force_lowest_dependencies_marker
class TestProviderManager:
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog, cleanup_providers_manager):
        self._caplog = caplog

    def test_providers_manager_singleton(self):
        """Test that ProvidersManager returns the same instance and shares state."""
        pm1 = ProvidersManager()
        pm2 = ProvidersManager()

        assert pm1 is pm2

        # assert their states are same
        assert pm1._provider_dict is pm2._provider_dict
        assert pm1._hook_provider_dict is pm2._hook_provider_dict

        # update property on one instance and check on another
        pm1.resource_version = "updated_version"
        assert pm2.resource_version == "updated_version"

    def test_providers_are_loaded(self):
        with self._caplog.at_level(logging.WARNING):
            self._caplog.clear()
            provider_manager = ProvidersManager()
            provider_list = list(provider_manager.providers.keys())
            # No need to sort the list - it should be sorted alphabetically !
            for provider in provider_list:
                package_name = provider_manager.providers[provider].data["package-name"]
                version = provider_manager.providers[provider].version
                documentation_url = provider_manager.providers[provider].data["documentation-url"]
                assert re.search(r"[0-9]*\.[0-9]*\.[0-9]*.*", version)
                assert package_name == provider
                assert isinstance(documentation_url, str)
            # just a coherence check - no exact number as otherwise we would have to update
            # several tests if we add new connections/provider which is not ideal
            assert len(provider_list) > 65
            assert self._caplog.records == []

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

    def test_connection_form_widgets(self, yaml_ui_metadata_counts):
        yaml_widgets, _ = yaml_ui_metadata_counts
        provider_manager = ProvidersManager()
        connections_form_widgets = list(provider_manager.connection_form_widgets.keys())
        # Connection form widgets use flask_appbuilder widgets, so they're only available when it's installed
        try:
            import flask_appbuilder  # noqa: F401

            assert len(connections_form_widgets) > 29
        except ImportError:
            # widgets loaded from YAML metadata only
            assert len(connections_form_widgets) == yaml_widgets

    def test_field_behaviours(self, yaml_ui_metadata_counts):
        _, yaml_behaviours = yaml_ui_metadata_counts
        provider_manager = ProvidersManager()
        connections_with_field_behaviours = list(provider_manager.field_behaviours.keys())
        try:
            import flask_appbuilder  # noqa: F401

            assert len(connections_with_field_behaviours) > 16
        except ImportError:
            # widgets loaded from YAML metadata only
            assert len(connections_with_field_behaviours) == yaml_behaviours

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

    def test_cli(self):
        provider_manager = ProvidersManager()

        # assert cli_command_functions is set of Callable[[], list[CLICommand]]
        assert isinstance(provider_manager.cli_command_functions, set)
        assert all(callable(func) for func in provider_manager.cli_command_functions)
        # assert cli_command_providers is set of str
        assert isinstance(provider_manager.cli_command_providers, set)
        assert all(isinstance(provider, str) for provider in provider_manager.cli_command_providers)

        sorted_cli_command_functions: list[Callable[[], list[CLICommand]]] = sorted(
            provider_manager.cli_command_functions, key=lambda x: x.__module__
        )
        sorted_cli_command_providers: list[str] = sorted(provider_manager.cli_command_providers)

        expected_functions_modules = [
            "airflow.providers.amazon.aws.cli.definition",
            "airflow.providers.celery.cli.definition",
            "airflow.providers.cncf.kubernetes.cli.definition",
            "airflow.providers.edge3.cli.definition",
            "airflow.providers.fab.cli.definition",
            "airflow.providers.keycloak.cli.definition",
        ]
        expected_providers = [
            "apache-airflow-providers-amazon",
            "apache-airflow-providers-celery",
            "apache-airflow-providers-cncf-kubernetes",
            "apache-airflow-providers-edge3",
            "apache-airflow-providers-fab",
            "apache-airflow-providers-keycloak",
        ]
        assert [func.__module__ for func in sorted_cli_command_functions] == expected_functions_modules
        assert sorted_cli_command_providers == expected_providers

    def test_dialects(self):
        provider_manager = ProvidersManager()
        dialect_class_names = list(provider_manager.dialects)
        assert len(dialect_class_names) == 3
        assert dialect_class_names == ["default", "mssql", "postgresql"]


class TestWithoutCheckProviderManager:
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, cleanup_providers_manager):
        pass

    @patch("airflow.providers_manager.import_string")
    @patch("airflow.providers_manager._correctness_check")
    @patch("airflow.providers_manager.ProvidersManager._discover_auth_managers")
    def test_auth_manager_without_check_property_should_not_called_import_string(
        self,
        mock_discover_auth_managers: MagicMock,
        mock_correctness_check: MagicMock,
        mock_importlib_import_string: MagicMock,
    ):
        providers_manager = ProvidersManager()
        result = providers_manager.auth_manager_without_check

        mock_discover_auth_managers.assert_called_once_with(check=False)
        mock_importlib_import_string.assert_not_called()
        mock_correctness_check.assert_not_called()

        assert providers_manager._auth_manager_without_check_set == result

    @patch("airflow.providers_manager.import_string")
    @patch("airflow.providers_manager._correctness_check")
    @patch("airflow.providers_manager.ProvidersManager._discover_executors")
    def test_executors_without_check_property_should_not_called_import_string(
        self,
        mock_discover_executors: MagicMock,
        mock_correctness_check: MagicMock,
        mock_importlib_import_string: MagicMock,
    ):
        providers_manager = ProvidersManager()
        result = providers_manager.executor_without_check

        mock_discover_executors.assert_called_once_with(check=False)
        mock_importlib_import_string.assert_not_called()
        mock_correctness_check.assert_not_called()

        assert providers_manager._executor_without_check_set == result


class TestProvidersMetadataLoading:
    @pytest.mark.parametrize(
        ("field_name", "field_def", "expected_title", "expected_checks"),
        [
            pytest.param(
                "api_url",
                {
                    "label": "API URL",
                    "description": "The API endpoint URL",
                    "schema": {
                        "type": "string",
                        "default": "https://api.example.com",
                    },
                },
                "API URL",
                lambda x: (
                    x["description"] == "The API endpoint URL" and x["value"] == "https://api.example.com"
                ),
                id="string_field",
            ),
            pytest.param(
                "timeout",
                {
                    "label": "Timeout",
                    "description": "Connection timeout in seconds",
                    "schema": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 300,
                        "default": 30,
                    },
                },
                "Timeout",
                lambda x: x["value"] == 30,
                id="integer_field",
            ),
            pytest.param(
                "use_ssl",
                {
                    "label": "Use SSL",
                    "schema": {
                        "type": "boolean",
                        "default": True,
                    },
                },
                "Use SSL",
                lambda x: x["value"] is True,
                id="boolean_field",
            ),
            pytest.param(
                "api_key",
                {
                    "label": "API Key",
                    "sensitive": True,
                    "schema": {
                        "type": "string",
                        "format": "password",
                    },
                },
                "API Key",
                lambda x: x["schema"].get("format") == "password",
                id="password_field",
            ),
            pytest.param(
                "ssl_mode",
                {
                    "label": "SSL Mode",
                    "description": "SSL connection mode",
                    "schema": {
                        "type": "string",
                        "enum": ["disable", "prefer", "require", "verify-full"],
                        "default": "prefer",
                    },
                },
                "SSL Mode",
                lambda x: (
                    x["value"] == "prefer"
                    and "enum" in x["schema"]
                    and x["schema"]["enum"] == ["disable", "prefer", "require", "verify-full"]
                ),
                id="enum_field",
            ),
        ],
    )
    def test_to_api_format(self, field_name, field_def, expected_title, expected_checks):
        """Test converting field definitions to API format."""
        pm = ProvidersManager()
        x = pm._to_api_format(field_name, field_def)

        assert x is not None
        assert isinstance(x, dict)
        assert x["schema"]["title"] == expected_title
        assert expected_checks(x)

    def test_add_customized_fields(self):
        """Test adding customized field behaviour from provider info."""
        pm = ProvidersManager()
        pm.initialize_providers_list()

        behaviour = {
            "hidden-fields": ["schema", "extra"],
            "relabeling": {"login": "Email Address"},
            "placeholders": {"host": "smtp.gmail.com", "port": "587"},
        }

        pm._add_customized_fields(
            package_name="test-provider", connection_type="test_conn", behaviour=behaviour
        )

        assert "test_conn" in pm._field_behaviours
        behaviour = pm._field_behaviours["test_conn"]
        assert behaviour["hidden_fields"] == ["schema", "extra"]
        assert behaviour["relabeling"] == {"login": "Email Address"}
        assert behaviour["placeholders"]["host"] == "smtp.gmail.com"

    def test_load_ui_for_http_provider(self):
        """Test that HTTP provider ui metadata is loaded from provider info."""
        pm = ProvidersManager()
        pm.initialize_providers_hooks()

        assert "http" in pm._field_behaviours
        behaviour = pm._field_behaviours["http"]

        assert "hidden_fields" in behaviour
        assert "relabeling" in behaviour
        assert "placeholders" in behaviour

    def test_ui_metadata_loading_without_hook_import(self):
        """Test that UI metadata loads from provider info without importing hook classes."""
        with patch("airflow.providers_manager.import_string") as mock_import:
            pm = ProvidersManager()
            pm.initialize_providers_hooks()

            assert "http" in pm._field_behaviours

            # assert that HttpHook was not imported during initialization, which means yaml path was taken
            assert len([call for call in mock_import.call_args_list if "HttpHook" in str(call)]) == 0
