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
from unittest.mock import patch

import pytest
from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
from flask_babel import lazy_gettext
from wtforms import BooleanField, Field, StringField

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers_manager import HookClassProvider, ProviderInfo, ProvidersManager


class TestProviderManager:
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog

    def test_providers_are_loaded(self):
        with self._caplog.at_level(logging.WARNING):
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
            assert [] == self._caplog.records

    def test_hooks_deprecation_warnings_generated(self):
        with pytest.warns(expected_warning=DeprecationWarning, match="hook-class-names") as warning_records:
            providers_manager = ProvidersManager()
            providers_manager._provider_dict["test-package"] = ProviderInfo(
                version="0.0.1",
                data={"hook-class-names": ["airflow.providers.sftp.hooks.sftp.SFTPHook"]},
                package_or_source="package",
            )
            providers_manager._discover_hooks()
        assert warning_records

    def test_hooks_deprecation_warnings_not_generated(self):
        with pytest.warns(expected_warning=None) as warning_records:
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
                package_or_source="package",
            )
            providers_manager._discover_hooks()
        assert [] == [w.message for w in warning_records.list if "hook-class-names" in str(w.message)]

    def test_warning_logs_generated(self):
        with self._caplog.at_level(logging.WARNING):
            providers_manager = ProvidersManager()
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
                package_or_source="package",
            )
            providers_manager._discover_hooks()
            _ = providers_manager._hooks_lazy_dict["wrong-connection-type"]
        assert len(self._caplog.records) == 1
        assert "Inconsistency!" in self._caplog.records[0].message
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
                package_or_source="package",
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
                package_or_source="package",
            )
            providers_manager._discover_hooks()
            _ = providers_manager._hooks_lazy_dict["dummy"]
        assert len(self._caplog.records) == 1
        assert "The connection type 'dummy' is already registered" in self._caplog.records[0].message
        assert (
            "different class names: 'airflow.providers.dummy.hooks.dummy.DummyHook'"
            " and 'airflow.providers.dummy.hooks.dummy.DummyHook2'."
        ) in self._caplog.records[0].message

    def test_hooks(self):
        with pytest.warns(expected_warning=None) as warning_records:
            with self._caplog.at_level(logging.WARNING):
                provider_manager = ProvidersManager()
                connections_list = list(provider_manager.hooks.keys())
                assert len(connections_list) > 60
        assert [] == [w.message for w in warning_records.list if "hook-class-names" in str(w.message)]
        assert len(self._caplog.records) == 0

    def test_hook_values(self):
        with pytest.warns(expected_warning=None) as warning_records:
            with self._caplog.at_level(logging.WARNING):
                provider_manager = ProvidersManager()
                connections_list = list(provider_manager.hooks.values())
                assert len(connections_list) > 60
        assert [] == [w.message for w in warning_records.list if "hook-class-names" in str(w.message)]
        assert len(self._caplog.records) == 0

    def test_connection_form_widgets(self):
        provider_manager = ProvidersManager()
        connections_form_widgets = list(provider_manager.connection_form_widgets.keys())
        assert len(connections_form_widgets) > 29

    @pytest.mark.parametrize(
        "scenario",
        [
            "prefix",
            "no_prefix",
            "both_1",
            "both_2",
        ],
    )
    def test_connection_form__add_widgets_prefix_backcompat(self, scenario):
        """
        When the field name is prefixed, it should be used as is.
        When not prefixed, we should add the prefix
        When there's a collision, the one that appears first in the list will be used.
        """

        class MyHook:
            conn_type = "test"

        provider_manager = ProvidersManager()
        widget_field = StringField(lazy_gettext("My Param"), widget=BS3TextFieldWidget())
        dummy_field = BooleanField(label=lazy_gettext("Dummy param"), description="dummy")
        widgets: dict[str, Field] = {}
        if scenario == "prefix":
            widgets["extra__test__my_param"] = widget_field
        elif scenario == "no_prefix":
            widgets["my_param"] = widget_field
        elif scenario == "both_1":
            widgets["my_param"] = widget_field
            widgets["extra__test__my_param"] = dummy_field
        elif scenario == "both_2":
            widgets["extra__test__my_param"] = widget_field
            widgets["my_param"] = dummy_field
        else:
            raise Exception("unexpected")

        provider_manager._add_widgets(
            package_name="abc",
            hook_class=MyHook,
            widgets=widgets,
        )
        assert provider_manager.connection_form_widgets["extra__test__my_param"].field == widget_field

    def test_connection_field_behaviors_placeholders_prefix(self):
        class MyHook:
            conn_type = "test"

            @classmethod
            def get_ui_field_behaviour(cls):
                return {
                    "hidden_fields": ["host", "schema"],
                    "relabeling": {},
                    "placeholders": {"abc": "hi", "extra__anything": "n/a", "password": "blah"},
                }

        provider_manager = ProvidersManager()
        provider_manager._add_customized_fields(
            package_name="abc",
            hook_class=MyHook,
            customized_fields=MyHook.get_ui_field_behaviour(),
        )
        expected = {
            "extra__test__abc": "hi",  # prefix should be added, since `abc` is not reserved
            "extra__anything": "n/a",  # no change since starts with extra
            "password": "blah",  # no change since it's a conn attr
        }
        assert provider_manager.field_behaviours["test"]["placeholders"] == expected

    def test_connection_form_widgets_fields_order(self):
        """Check that order of connection for widgets preserved by original Hook order."""
        test_conn_type = "test"
        field_prefix = f"extra__{test_conn_type}__"
        field_names = ("yyy_param", "aaa_param", "000_param", "foo", "bar", "spam", "egg")

        expected_field_names_order = tuple(f"{field_prefix}{f}" for f in field_names)

        class TestHook:
            conn_type = test_conn_type

        provider_manager = ProvidersManager()
        provider_manager._connection_form_widgets = {}
        provider_manager._add_widgets(
            package_name="mock",
            hook_class=TestHook,
            widgets={f: BooleanField(lazy_gettext("Dummy param")) for f in expected_field_names_order},
        )
        actual_field_names_order = tuple(
            key for key in provider_manager.connection_form_widgets.keys() if key.startswith(field_prefix)
        )
        assert actual_field_names_order == expected_field_names_order, "Not keeping original fields order"

    def test_connection_form_widgets_fields_order_multiple_hooks(self):
        """
        Check that order of connection for widgets preserved by original Hooks order.
        Even if different hooks specified field with the same connection type.
        """
        test_conn_type = "test"
        field_prefix = f"extra__{test_conn_type}__"
        field_names_hook_1 = ("foo", "bar", "spam", "egg")
        field_names_hook_2 = ("yyy_param", "aaa_param", "000_param")

        expected_field_names_order = tuple(
            f"{field_prefix}{f}" for f in [*field_names_hook_1, *field_names_hook_2]
        )

        class TestHook1:
            conn_type = test_conn_type

        class TestHook2:
            conn_type = "another"

        provider_manager = ProvidersManager()
        provider_manager._connection_form_widgets = {}
        provider_manager._add_widgets(
            package_name="mock",
            hook_class=TestHook1,
            widgets={
                f"{field_prefix}{f}": BooleanField(lazy_gettext("Dummy param")) for f in field_names_hook_1
            },
        )
        provider_manager._add_widgets(
            package_name="another_mock",
            hook_class=TestHook2,
            widgets={
                f"{field_prefix}{f}": BooleanField(lazy_gettext("Dummy param")) for f in field_names_hook_2
            },
        )
        actual_field_names_order = tuple(
            key for key in provider_manager.connection_form_widgets.keys() if key.startswith(field_prefix)
        )
        assert actual_field_names_order == expected_field_names_order, "Not keeping original fields order"

    def test_field_behaviours(self):
        provider_manager = ProvidersManager()
        connections_with_field_behaviours = list(provider_manager.field_behaviours.keys())
        assert len(connections_with_field_behaviours) > 16

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

    def test_auth_backends(self):
        provider_manager = ProvidersManager()
        auth_backend_module_names = list(provider_manager.auth_backend_module_names)
        assert len(auth_backend_module_names) > 0

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
            assert [] == self._caplog.messages

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
            assert [
                "Optional provider feature disabled when importing 'HookClass' from 'test_package' package"
            ] == self._caplog.messages
