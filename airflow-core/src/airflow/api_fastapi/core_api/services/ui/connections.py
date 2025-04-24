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

import contextlib
import importlib
import logging
from collections.abc import MutableMapping
from functools import cache
from typing import TYPE_CHECKING

from airflow.api_fastapi.core_api.datamodels.connections import (
    ConnectionHookFieldBehavior,
    ConnectionHookMetaData,
    StandardHookFields,
)
from airflow.sdk import Param

if TYPE_CHECKING:
    from airflow.providers_manager import ConnectionFormWidgetInfo, HookInfo

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

    class MockEnum:
        """Mock for wtforms.validators.Optional."""

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
            self.param = Param(
                default=default,
                title=label,
                description=description or None,
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

        from airflow.providers_manager import ProvidersManager

        def mock_lazy_gettext(txt: str) -> str:
            """Mock for flask_babel.lazy_gettext."""
            return txt

        def mock_any_of(allowed_values: list) -> HookMetaService.MockEnum:
            """Mock for wtforms.validators.any_of."""
            return HookMetaService.MockEnum(allowed_values)

        with contextlib.ExitStack() as stack:
            try:
                importlib.import_module("wtforms")
                stack.enter_context(mock.patch("wtforms.StringField", HookMetaService.MockStringField))
                stack.enter_context(mock.patch("wtforms.fields.StringField", HookMetaService.MockStringField))
                stack.enter_context(
                    mock.patch("wtforms.fields.simple.StringField", HookMetaService.MockStringField)
                )

                stack.enter_context(mock.patch("wtforms.IntegerField", HookMetaService.MockIntegerField))
                stack.enter_context(
                    mock.patch("wtforms.fields.IntegerField", HookMetaService.MockIntegerField)
                )
                stack.enter_context(mock.patch("wtforms.PasswordField", HookMetaService.MockPasswordField))
                stack.enter_context(mock.patch("wtforms.BooleanField", HookMetaService.MockBooleanField))
                stack.enter_context(
                    mock.patch("wtforms.fields.BooleanField", HookMetaService.MockBooleanField)
                )
                stack.enter_context(
                    mock.patch("wtforms.fields.simple.BooleanField", HookMetaService.MockBooleanField)
                )
                stack.enter_context(mock.patch("wtforms.validators.Optional", HookMetaService.MockOptional))
                stack.enter_context(mock.patch("wtforms.validators.any_of", mock_any_of))
            except ImportError:
                pass

            try:
                importlib.import_module("flask_babel")
                stack.enter_context(mock.patch("flask_babel.lazy_gettext", mock_lazy_gettext))
            except ImportError:
                pass

            try:
                importlib.import_module("flask_appbuilder")
                stack.enter_context(
                    mock.patch(
                        "flask_appbuilder.fieldwidgets.BS3TextFieldWidget", HookMetaService.MockAnyWidget
                    )
                )
                stack.enter_context(
                    mock.patch(
                        "flask_appbuilder.fieldwidgets.BS3TextAreaFieldWidget", HookMetaService.MockAnyWidget
                    )
                )
                stack.enter_context(
                    mock.patch(
                        "flask_appbuilder.fieldwidgets.BS3PasswordFieldWidget", HookMetaService.MockAnyWidget
                    )
                )
            except ImportError:
                pass

            pm = ProvidersManager()
            return pm.hooks, pm.connection_form_widgets, pm.field_behaviours

        return (
            {},
            {},
            {},
        )  # Make mypy happy, should never been reached https://github.com/python/mypy/issues/7726

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
            if isinstance(form_widget.field, HookMetaService.MockBaseField):
                hook_widgets = result.get(hook_key, {})
                hook_widgets[form_widget.field_name] = form_widget.field.param.dump()
                result[hook_key] = hook_widgets
            else:
                log.error("Unknown form widget in %s: %s", hook_key, form_widget)
        return result

    @staticmethod
    @cache
    def hook_meta_data() -> list[ConnectionHookMetaData]:
        hooks, connection_form_widgets, field_behaviours = HookMetaService._get_hooks_with_mocked_fab()
        result: list[ConnectionHookMetaData] = []
        widgets = HookMetaService._convert_extra_fields(connection_form_widgets)
        for hook_key, hook_info in hooks.items():
            if not hook_info:
                continue
            hook_meta = ConnectionHookMetaData(
                connection_type=hook_key,
                hook_class_name=hook_info.hook_class_name,
                default_conn_name=None,  # TODO: later
                hook_name=hook_info.hook_name,
                standard_fields=HookMetaService._make_standard_fields(field_behaviours.get(hook_key)),
                extra_fields=widgets.get(hook_key),
            )
            result.append(hook_meta)
        return result
