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

from typing import TYPE_CHECKING

import pytest
from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
from flask_babel import lazy_gettext
from wtforms.fields.simple import BooleanField, StringField

from airflow.providers_manager import ProvidersManager

if TYPE_CHECKING:
    from wtforms.fields.core import Field

pytestmark = pytest.mark.db_test


@pytest.mark.parametrize(
    "scenario",
    [
        "prefix",
        "no_prefix",
        "both_1",
        "both_2",
    ],
)
def test_connection_form__add_widgets_prefix_backcompat(scenario, cleanup_providers_manager):
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
        raise ValueError("unexpected")

    if hasattr(provider_manager, "_add_widgets_from_hook"):
        provider_manager._add_widgets_from_hook(
            package_name="abc",
            hook_class=MyHook,
            widgets=widgets,
        )
    else:
        # backcompat for airflow < 3.2
        provider_manager._add_widgets(
            package_name="abc",
            hook_class=MyHook,
            widgets=widgets,
        )
    assert provider_manager.connection_form_widgets["extra__test__my_param"].field == widget_field


def test_connection_field_behaviors_placeholders_prefix(cleanup_providers_manager):
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
    if hasattr(provider_manager, "_add_customized_fields_from_hook"):
        provider_manager._add_customized_fields_from_hook(
            package_name="abc",
            hook_class=MyHook,
            customized_fields=MyHook.get_ui_field_behaviour(),
        )
    else:
        # backcompat for airflow < 3.2
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


def test_connection_form_widgets_fields_order(cleanup_providers_manager):
    """Check that order of connection for widgets preserved by original Hook order."""
    test_conn_type = "test"
    field_prefix = f"extra__{test_conn_type}__"
    field_names = ("yyy_param", "aaa_param", "000_param", "foo", "bar", "spam", "egg")

    expected_field_names_order = tuple(f"{field_prefix}{f}" for f in field_names)

    class TestHook:
        conn_type = test_conn_type

    provider_manager = ProvidersManager()
    provider_manager._connection_form_widgets = {}
    widgets = {f: BooleanField(lazy_gettext("Dummy param")) for f in expected_field_names_order}
    if hasattr(provider_manager, "_add_widgets_from_hook"):
        provider_manager._add_widgets_from_hook(
            package_name="mock",
            hook_class=TestHook,
            widgets=widgets,
        )
    else:
        # backcompat for airflow < 3.2
        provider_manager._add_widgets(
            package_name="mock",
            hook_class=TestHook,
            widgets=widgets,
        )
    actual_field_names_order = tuple(
        key for key in provider_manager.connection_form_widgets.keys() if key.startswith(field_prefix)
    )
    assert actual_field_names_order == expected_field_names_order, "Not keeping original fields order"


def test_connection_form_widgets_fields_order_multiple_hooks(cleanup_providers_manager):
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
    widgets_1 = {f"{field_prefix}{f}": BooleanField(lazy_gettext("Dummy param")) for f in field_names_hook_1}
    widgets_2 = {f"{field_prefix}{f}": BooleanField(lazy_gettext("Dummy param")) for f in field_names_hook_2}
    if hasattr(provider_manager, "_add_widgets_from_hook"):
        provider_manager._add_widgets_from_hook(
            package_name="mock",
            hook_class=TestHook1,
            widgets=widgets_1,
        )
        provider_manager._add_widgets_from_hook(
            package_name="another_mock",
            hook_class=TestHook2,
            widgets=widgets_2,
        )
    else:
        # backcompat for airflow < 3.2
        provider_manager._add_widgets(
            package_name="mock",
            hook_class=TestHook1,
            widgets=widgets_1,
        )
        provider_manager._add_widgets(
            package_name="another_mock",
            hook_class=TestHook2,
            widgets=widgets_2,
        )
    actual_field_names_order = tuple(
        key for key in provider_manager.connection_form_widgets.keys() if key.startswith(field_prefix)
    )
    assert actual_field_names_order == expected_field_names_order, "Not keeping original fields order"
