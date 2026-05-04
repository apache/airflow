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
from collections.abc import MutableMapping
from functools import cache
from typing import TYPE_CHECKING, Literal

from airflow.api_fastapi.core_api.datamodels.connections import (
    ConnectionHookFieldBehavior,
    ConnectionHookMetaData,
    StandardHookFields,
)
from airflow.providers_manager import HookInfo, ProvidersManager
from airflow.serialization.definitions.param import SerializedParam

if TYPE_CHECKING:
    from airflow.providers_manager import ConnectionFormWidgetInfo

log = logging.getLogger(__name__)


class HookMetaService:
    """Service for retrieving details about hooks to render UI."""

    class MockOptional:
        """Mock for wtforms.validators.Optional."""

        def __init__(
            self,
            *args,
            **kwargs,
        ):
            pass

        def __call__(self, form, field):
            """No-op call to satisfy WTForms validator protocol."""
            return None

    class MockEnum:
        """Mock for wtforms.validators.AnyOf."""

        def __init__(self, allowed_values):
            self.allowed_values = allowed_values

    class MockBaseField:
        """Mock of WTForms Field."""

        param_type: str = "UNDEFINED"
        param_format: str | None = None
        widget = None

        def __init__(
            self,
            label: str | None = None,
            validators=None,
            description: str = "",
            default: str | None = None,
            widget=None,
            source: Literal["dag", "task"] | None = None,
        ):
            type: str | list[str] = [self.param_type, "null"]
            enum = {}
            format = {"format": self.param_format} if self.param_format else {}
            if validators:
                if any(isinstance(v, HookMetaService.MockOptional) for v in validators):
                    type = [self.param_type, "null"]
                for v in validators:
                    if isinstance(v, HookMetaService.MockEnum):
                        enum = {"enum": v.allowed_values}
            self.param = SerializedParam(
                default=default,
                title=label,
                description=description or None,
                source=source or None,
                type=type,
                **format,
                **enum,
            )
            self.widget = widget
            self.field_class = self.__class__

    class MockStringField(MockBaseField):
        """Mock of WTForms StringField."""

        param_type: str = "string"

    class MockIntegerField(MockBaseField):
        """Mock of WTForms IntegerField."""

        param_type: str = "integer"

    class MockPasswordField(MockBaseField):
        """Mock of WTForms PasswordField."""

        param_type: str = "string"
        param_format: str | None = "password"

    class MockBooleanField(MockBaseField):
        """Mock of WTForms BooleanField."""

        param_type: str = "boolean"

    class MockAnyWidget:
        """Mock any flask appbuilder widget."""

    @staticmethod
    def _get_hooks_with_mocked_fab() -> tuple[
        MutableMapping[str, HookInfo | None], dict[str, ConnectionFormWidgetInfo], dict[str, dict]
    ]:
        """Get hooks with all details w/o FAB needing to be installed."""
        from unittest import mock

        def mock_lazy_gettext(txt: str) -> str:
            """Mock for flask_babel.lazy_gettext."""
            return txt

        def mock_any_of(allowed_values: list) -> HookMetaService.MockEnum:
            """Mock for wtforms.validators.any_of."""
            return HookMetaService.MockEnum(allowed_values)

        # Before importing ProvidersManager, we need to mock all FAB and WTForms
        # dependencies to avoid ImportErrors when FAB is not installed.
        import sys
        from importlib.util import find_spec
        from unittest.mock import MagicMock

        for mod_name in [
            "wtforms",
            "wtforms.csrf",
            "wtforms.fields",
            "wtforms.fields.simple",
            "wtforms.validators",
            "flask_babel",
            "flask_appbuilder",
            "flask_appbuilder.fieldwidgets",
        ]:
            try:
                if not find_spec(mod_name):
                    raise ModuleNotFoundError
            except ModuleNotFoundError:
                sys.modules[mod_name] = MagicMock()

        # We conditionally inject mock classes for missing dependencies
        # to ensure `ProvidersManager` can initialize hook connection widgets
        # without crashing when FAB/WTForms are not installed.
        if "wtforms.StringField" not in sys.modules:
            # Only apply mocks if the actual module wasn't loaded beforehand.
            # This avoids thread-safety issues caused by `unittest.mock.patch` mutating global states.
            with (
                mock.patch("wtforms.StringField", HookMetaService.MockStringField),
                mock.patch("wtforms.fields.StringField", HookMetaService.MockStringField),
                mock.patch("wtforms.fields.simple.StringField", HookMetaService.MockStringField),
                mock.patch("wtforms.IntegerField", HookMetaService.MockIntegerField),
                mock.patch("wtforms.fields.IntegerField", HookMetaService.MockIntegerField),
                mock.patch("wtforms.PasswordField", HookMetaService.MockPasswordField),
                mock.patch("wtforms.BooleanField", HookMetaService.MockBooleanField),
                mock.patch("wtforms.fields.BooleanField", HookMetaService.MockBooleanField),
                mock.patch("wtforms.fields.simple.BooleanField", HookMetaService.MockBooleanField),
                mock.patch("flask_babel.lazy_gettext", mock_lazy_gettext),
                mock.patch("flask_appbuilder.fieldwidgets.BS3TextFieldWidget", HookMetaService.MockAnyWidget),
                mock.patch(
                    "flask_appbuilder.fieldwidgets.BS3TextAreaFieldWidget", HookMetaService.MockAnyWidget
                ),
                mock.patch(
                    "flask_appbuilder.fieldwidgets.BS3PasswordFieldWidget", HookMetaService.MockAnyWidget
                ),
                mock.patch("wtforms.validators.Optional", HookMetaService.MockOptional),
                mock.patch("wtforms.validators.any_of", mock_any_of),
                # Prevent poisoning the global ProvidersManager singleton with mocks
                mock.patch("airflow.providers_manager.ProvidersManager._instance", None),
                mock.patch("airflow.providers_manager.ProvidersManager.initialized", return_value=False),
            ):
                pm = ProvidersManager()
                return pm.hooks, pm.connection_form_widgets, pm.field_behaviours  # Will init providers hooks
        else:
            pm = ProvidersManager()
            return pm.hooks, pm.connection_form_widgets, pm.field_behaviours  # Will init providers hooks

    @staticmethod
    def _make_standard_fields(field_behaviour: dict | None) -> StandardHookFields | None:
        if not field_behaviour:
            return None

        def make_field(field_name: str, field_behaviour: dict) -> ConnectionHookFieldBehavior | None:
            hidden_fields = field_behaviour.get("hidden_fields", [])
            relabeling = field_behaviour.get("relabeling", {}).get(field_name)
            placeholder = field_behaviour.get("placeholders", {}).get(field_name)
            if any([field_name in hidden_fields, relabeling, placeholder]):
                return ConnectionHookFieldBehavior(
                    hidden=field_name in hidden_fields,
                    title=relabeling,
                    placeholder=placeholder,
                )
            return None

        return StandardHookFields(
            description=make_field("description", field_behaviour),
            url_schema=make_field("schema", field_behaviour),
            host=make_field("host", field_behaviour),
            port=make_field("port", field_behaviour),
            login=make_field("login", field_behaviour),
            password=make_field("password", field_behaviour),
        )

    @staticmethod
    def _convert_extra_fields(form_widgets: dict[str, ConnectionFormWidgetInfo]) -> dict[str, MutableMapping]:
        result: dict[str, MutableMapping] = {}
        for key, form_widget in form_widgets.items():
            hook_key = key.split("__")[1]
            hook_widgets = result.get(hook_key, {})

            if isinstance(form_widget.field, dict):
                # yaml path, form widgets read from yaml and already present in SerializedParam.dump() format
                hook_widgets[form_widget.field_name] = form_widget.field
            elif isinstance(form_widget.field, HookMetaService.MockBaseField):
                # legacy path, form widgets created using mocked WTForms fields, need to convert to SerializedParam.dump()
                hook_widgets[form_widget.field_name] = form_widget.field.param.dump()
            elif type(form_widget.field).__name__ == "UnboundField":
                # handle real WTForms fields gracefully without needing mock patches
                field_class_name = getattr(form_widget.field.field_class, "__name__", "")
                param_type = "string"
                param_format = None
                if field_class_name == "BooleanField":
                    param_type = "boolean"
                elif field_class_name == "IntegerField":
                    param_type = "integer"
                elif field_class_name == "PasswordField":
                    param_format = "password"

                label = (
                    form_widget.field.args[0]
                    if len(form_widget.field.args) > 0
                    else form_widget.field.kwargs.get("label")
                )
                validators = form_widget.field.kwargs.get("validators", [])
                description = form_widget.field.kwargs.get("description", "")
                default = form_widget.field.kwargs.get("default", None)

                enum = {}
                for v in validators:
                    if type(v).__name__ == "AnyOf":
                        enum["enum"] = getattr(v, "values", [])

                types = [param_type, "null"]
                format_dict = {"format": param_format} if param_format else {}

                param = SerializedParam(
                    default=default,
                    title=str(label) if label is not None else None,
                    description=str(description) if description else None,
                    source=None,
                    type=types,
                    **format_dict,
                    **enum,
                ).dump()
                hook_widgets[form_widget.field_name] = param
            else:
                log.error("Unknown form widget in %s: %s", hook_key, form_widget)
                continue

            result[hook_key] = hook_widgets
        return result

    @staticmethod
    @cache
    def hook_meta_data() -> list[ConnectionHookMetaData]:
        pm = ProvidersManager()
        widgets = HookMetaService._convert_extra_fields(pm._connection_form_widgets_from_metadata)
        return [
            ConnectionHookMetaData(
                connection_type=meta.connection_type,
                hook_class_name=meta.hook_class_name,
                default_conn_name=None,
                hook_name=meta.hook_name,
                standard_fields=HookMetaService._make_standard_fields(meta.field_behaviour),
                extra_fields=widgets.get(meta.connection_type),
            )
            for meta in pm.iter_connection_type_hook_ui_metadata()
        ]
