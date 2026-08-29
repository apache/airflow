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
from unittest import mock

import pytest

from airflow.api_fastapi.core_api.datamodels.connections import (
    ConnectionHookFieldBehavior,
    ConnectionHookMetaData,
    StandardHookFields,
)
from airflow.api_fastapi.core_api.services.ui.connections import HookMetaService
from airflow.providers_manager import ConnectionFormWidgetInfo, ConnectionTypeHookUIMetadata, ProvidersManager

CONNECTIONS_MODULE = "airflow.api_fastapi.core_api.services.ui.connections"

MOCKED_FAB_MODULES = [
    "wtforms",
    "wtforms.csrf",
    "wtforms.fields",
    "wtforms.fields.simple",
    "wtforms.validators",
    "flask_babel",
    "flask_appbuilder",
    "flask_appbuilder.fieldwidgets",
]


class UnboundField:
    """Stand-in for ``wtforms.fields.core.UnboundField``, which is matched by class name."""

    def __init__(self, field_class, *args, **kwargs):
        self.field_class = field_class
        self.args = args
        self.kwargs = kwargs


class AnyOf:
    """Stand-in for ``wtforms.validators.AnyOf``, which is matched by class name."""

    def __init__(self, values):
        self.values = values


class StringField: ...


class BooleanField: ...


class IntegerField: ...


class PasswordField: ...


def find_spec_succeeds(name, package=None):
    return object()


def find_spec_returns_none(name, package=None):
    return None


def find_spec_raises(name, package=None):
    raise ModuleNotFoundError(f"No module named {name!r}", name=name)


def make_widget(field, field_name: str) -> ConnectionFormWidgetInfo:
    return ConnectionFormWidgetInfo(
        hook_class_name="SomeHook",
        package_name="apache-airflow-providers-some",
        field=field,
        field_name=field_name,
        is_sensitive=False,
    )


class TestMockOptional:
    def test_mock_optional_is_callable(self):
        """MockOptional instances must be callable to satisfy WTForms validator checks."""
        validator = HookMetaService.MockOptional()
        assert callable(validator)

    def test_mock_optional_call_is_noop(self):
        """Calling MockOptional should be a no-op (returns None)."""
        validator = HookMetaService.MockOptional()
        result = validator(None, None)
        assert result is None


class TestMockBaseField:
    @pytest.mark.parametrize(
        ("field_class", "expected_type", "expected_format"),
        [
            (HookMetaService.MockStringField, "string", None),
            (HookMetaService.MockIntegerField, "integer", None),
            (HookMetaService.MockPasswordField, "string", "password"),
            (HookMetaService.MockBooleanField, "boolean", None),
        ],
    )
    def test_schema_type_and_format_per_field_class(self, field_class, expected_type, expected_format):
        schema = field_class(label="Some label").param.dump()["schema"]

        assert schema["type"] == [expected_type, "null"]
        assert schema.get("format") == expected_format

    @pytest.mark.parametrize(
        ("field_kwargs", "expected_dump"),
        [
            pytest.param(
                {},
                {
                    "value": None,
                    "schema": {"title": None, "type": ["string", "null"]},
                    "description": None,
                    "source": None,
                },
                id="omitted",
            ),
            pytest.param(
                {
                    "label": "Some label",
                    "description": "Some description",
                    "default": "abc",
                    "source": "task",
                },
                {
                    "value": "abc",
                    "schema": {"title": "Some label", "type": ["string", "null"]},
                    "description": "Some description",
                    "source": "task",
                },
                id="provided",
            ),
        ],
    )
    def test_dumped_param_reflects_the_field_arguments(self, field_kwargs, expected_dump):
        assert HookMetaService.MockStringField(**field_kwargs).param.dump() == expected_dump

    @pytest.mark.parametrize(
        "validators",
        [
            pytest.param([HookMetaService.MockEnum(["a", "b"])], id="enum-only"),
            pytest.param(
                [HookMetaService.MockOptional(), HookMetaService.MockEnum(["a", "b"])],
                id="optional-and-enum",
            ),
        ],
    )
    def test_validators_are_reflected_in_the_schema(self, validators):
        field = HookMetaService.MockStringField(label="Some label", validators=validators)

        schema = field.param.dump()["schema"]

        assert schema["type"] == ["string", "null"]
        assert schema["enum"] == ["a", "b"]

    def test_widget_and_field_class_are_recorded(self):
        widget = HookMetaService.MockAnyWidget()
        field = HookMetaService.MockStringField(widget=widget)

        assert field.widget is widget
        # ProvidersManager checks this attribute before accepting connection widgets.
        assert field.field_class is HookMetaService.MockStringField


class TestMakeStandardFields:
    def test_returns_none_without_field_behaviour(self):
        assert HookMetaService._make_standard_fields(None) is None

    def test_builds_configured_field_behaviours(self):
        result = HookMetaService._make_standard_fields(
            {
                "hidden_fields": ["description"],
                "relabeling": {"host": "Server"},
                "placeholders": {"port": "5432"},
            }
        )

        assert result == StandardHookFields(
            description=ConnectionHookFieldBehavior(hidden=True),
            url_schema=None,
            host=ConnectionHookFieldBehavior(title="Server"),
            port=ConnectionHookFieldBehavior(placeholder="5432"),
            login=None,
            password=None,
        )


class TestConvertExtraFields:
    def test_dict_fields_are_passed_through_and_grouped_by_connection_type(self):
        mysql_foo = {"value": None, "schema": {"type": ["string", "null"]}, "description": None}
        mysql_bar = {"value": "abc", "schema": {}, "description": None}
        ftp_baz = {"value": None, "schema": {}, "description": "Some description"}

        result = HookMetaService._convert_extra_fields(
            {
                "extra__mysql__foo": make_widget(mysql_foo, "foo"),
                "extra__mysql__bar": make_widget(mysql_bar, "bar"),
                "extra__ftp__baz": make_widget(ftp_baz, "baz"),
            }
        )

        assert result == {"mysql": {"foo": mysql_foo, "bar": mysql_bar}, "ftp": {"baz": ftp_baz}}

    def test_mocked_wtforms_field_is_dumped(self):
        field = HookMetaService.MockStringField(label="Some label")

        result = HookMetaService._convert_extra_fields({"extra__mysql__foo": make_widget(field, "foo")})

        assert result == {"mysql": {"foo": field.param.dump()}}

    @pytest.mark.parametrize(
        ("field_class", "expected_type", "expected_format"),
        [
            (StringField, "string", None),
            (BooleanField, "boolean", None),
            (IntegerField, "integer", None),
            (PasswordField, "string", "password"),
        ],
    )
    def test_unbound_field_type_and_format_per_field_class(self, field_class, expected_type, expected_format):
        field = UnboundField(field_class, "Some label")

        result = HookMetaService._convert_extra_fields({"extra__mysql__foo": make_widget(field, "foo")})

        schema = result["mysql"]["foo"]["schema"]
        assert schema["type"] == [expected_type, "null"]
        assert schema.get("format") == expected_format

    @pytest.mark.parametrize(
        ("field_args", "field_kwargs", "expected_title"),
        [
            pytest.param(("Some label",), {}, "Some label", id="positional"),
            pytest.param((), {"label": "Some label"}, "Some label", id="keyword"),
            pytest.param((), {}, None, id="missing"),
        ],
    )
    def test_unbound_field_label_becomes_the_schema_title(self, field_args, field_kwargs, expected_title):
        field = UnboundField(StringField, *field_args, **field_kwargs)

        result = HookMetaService._convert_extra_fields({"extra__mysql__foo": make_widget(field, "foo")})

        assert result["mysql"]["foo"]["schema"]["title"] == expected_title

    def test_unbound_field_keeps_description_and_default(self):
        field = UnboundField(StringField, "Some label", description="Some description", default="abc")

        result = HookMetaService._convert_extra_fields({"extra__mysql__foo": make_widget(field, "foo")})

        assert result["mysql"]["foo"]["description"] == "Some description"
        assert result["mysql"]["foo"]["value"] == "abc"

    def test_unbound_field_reads_the_enum_from_any_of_validators_only(self):
        field = UnboundField(StringField, "Some label", validators=[object(), AnyOf(["a", "b"])])

        result = HookMetaService._convert_extra_fields({"extra__mysql__foo": make_widget(field, "foo")})

        assert result["mysql"]["foo"]["schema"]["enum"] == ["a", "b"]

    def test_unknown_field_is_skipped_without_dropping_the_other_widgets(self):
        result = HookMetaService._convert_extra_fields(
            {
                "extra__mysql__foo": make_widget({"schema": {}}, "foo"),
                "extra__mysql__unknown": make_widget(object(), "unknown"),
            }
        )

        assert result == {"mysql": {"foo": {"schema": {}}}}


class TestGetHooksWithMockedFab:
    """
    Both branches are pinned to a patched ``find_spec`` rather than to what is installed.

    ``airflow-core`` does not depend on FAB, so the real branch taken varies by environment, and the
    production code deliberately leaves its ``sys.modules`` mocks in place — hence the ``patch.dict``.
    """

    @staticmethod
    def call_without_fab_modules(find_spec, build_providers_manager):
        with (
            mock.patch.dict(sys.modules),
            mock.patch("importlib.util.find_spec", autospec=True, side_effect=find_spec),
            mock.patch(
                f"{CONNECTIONS_MODULE}.ProvidersManager",
                autospec=True,
                side_effect=build_providers_manager,
            ),
        ):
            for module_name in MOCKED_FAB_MODULES:
                sys.modules.pop(module_name, None)

            result = HookMetaService._get_hooks_with_mocked_fab()
            mocked_modules = [
                isinstance(sys.modules.get(name), mock.MagicMock) for name in MOCKED_FAB_MODULES
            ]

        return result, mocked_modules

    def test_providers_manager_is_used_directly_when_fab_is_importable(self):
        providers_manager = mock.MagicMock(spec=ProvidersManager)

        result, mocked_modules = self.call_without_fab_modules(find_spec_succeeds, lambda: providers_manager)

        assert not any(mocked_modules)
        assert result == (
            providers_manager.hooks,
            providers_manager.connection_form_widgets,
            providers_manager.field_behaviours,
        )

    @pytest.mark.parametrize(
        "find_spec",
        [
            pytest.param(find_spec_raises, id="import-raises"),
            pytest.param(find_spec_returns_none, id="spec-not-found"),
        ],
    )
    def test_missing_fab_dependencies_are_replaced_by_mock_modules(self, find_spec):
        providers_manager = mock.MagicMock(spec=ProvidersManager)

        result, mocked_modules = self.call_without_fab_modules(find_spec, lambda: providers_manager)

        assert all(mocked_modules)
        assert result == (
            providers_manager.hooks,
            providers_manager.connection_form_widgets,
            providers_manager.field_behaviours,
        )

    def test_patched_helpers_return_the_mock_substitutes(self):
        """
        The substitutes are reached through ``wtforms.validators``, not ``sys.modules``.

        ``mock.patch`` resolves ``wtforms.validators.any_of`` by attribute lookup on the ``wtforms``
        mock, which is a different object from the ``sys.modules["wtforms.validators"]`` mock that
        ``from wtforms.validators import any_of`` would return.
        """
        captured = {}

        def read_patched_helpers():
            import flask_babel
            import wtforms

            captured["label"] = flask_babel.lazy_gettext("Some label")
            captured["validator"] = wtforms.validators.any_of(["a", "b"])
            return mock.MagicMock(spec=ProvidersManager)

        self.call_without_fab_modules(find_spec_raises, read_patched_helpers)

        assert captured["label"] == "Some label"
        assert isinstance(captured["validator"], HookMetaService.MockEnum)
        assert captured["validator"].allowed_values == ["a", "b"]


class TestHookMetaData:
    @pytest.fixture(autouse=True)
    def clear_cache(self):
        HookMetaService.hook_meta_data.cache_clear()
        yield
        HookMetaService.hook_meta_data.cache_clear()

    @mock.patch(f"{CONNECTIONS_MODULE}.ProvidersManager", autospec=True)
    def test_builds_metadata(self, providers_manager):
        provider_manager = providers_manager.return_value
        provider_manager._connection_form_widgets_from_metadata = {
            "extra__some__token": make_widget({"schema": {}}, "token")
        }
        provider_manager.iter_connection_type_hook_ui_metadata.return_value = [
            ConnectionTypeHookUIMetadata(
                connection_type="some",
                hook_name="Some hook",
                hook_class_name="some.hooks.SomeHook",
                field_behaviour={"hidden_fields": ["password"]},
            )
        ]

        assert HookMetaService.hook_meta_data() == [
            ConnectionHookMetaData(
                connection_type="some",
                hook_class_name="some.hooks.SomeHook",
                default_conn_name=None,
                hook_name="Some hook",
                standard_fields=StandardHookFields(
                    description=None,
                    url_schema=None,
                    host=None,
                    port=None,
                    login=None,
                    password=ConnectionHookFieldBehavior(hidden=True),
                ),
                extra_fields={"token": {"schema": {}}},
            )
        ]
