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

from collections.abc import MutableMapping
from functools import cache
from typing import TYPE_CHECKING

from fastapi import HTTPException, status
from pydantic import ValidationError
from sqlalchemy import select

from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionOnExistence,
    BulkActionResponse,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.connections import (
    ConnectionBody,
    ConnectionHookMetaData,
    HookFieldBehavior,
    StandardHookFields,
)
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.models.connection import Connection
from airflow.sdk import Param

if TYPE_CHECKING:
    from airflow.providers_manager import ConnectionFormWidgetInfo, HookInfo


class BulkConnectionService(BulkService[ConnectionBody]):
    """Service for handling bulk operations on connections."""

    def categorize_connections(self, connection_ids: set) -> tuple[dict, set, set]:
        """
        Categorize the given connection_ids into matched_connection_ids and not_found_connection_ids based on existing connection_ids.

        Existed connections are returned as a dict of {connection_id : Connection}.

        :param connection_ids: set of connection_ids
        :return: tuple of dict of existed connections, set of matched connection_ids, set of not found connection_ids
        """
        existed_connections = self.session.execute(
            select(Connection).filter(Connection.conn_id.in_(connection_ids))
        ).scalars()
        existed_connections_dict = {conn.conn_id: conn for conn in existed_connections}
        matched_connection_ids = set(existed_connections_dict.keys())
        not_found_connection_ids = connection_ids - matched_connection_ids
        return existed_connections_dict, matched_connection_ids, not_found_connection_ids

    def handle_bulk_create(
        self, action: BulkCreateAction[ConnectionBody], results: BulkActionResponse
    ) -> None:
        """Bulk create connections."""
        to_create_connection_ids = {connection.connection_id for connection in action.entities}
        existed_connections_dict, matched_connection_ids, not_found_connection_ids = (
            self.categorize_connections(to_create_connection_ids)
        )
        try:
            if action.action_on_existence == BulkActionOnExistence.FAIL and matched_connection_ids:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"The connections with these connection_ids: {matched_connection_ids} already exist.",
                )
            elif action.action_on_existence == BulkActionOnExistence.SKIP:
                create_connection_ids = not_found_connection_ids
            else:
                create_connection_ids = to_create_connection_ids

            for connection in action.entities:
                if connection.connection_id in create_connection_ids:
                    if connection.connection_id in matched_connection_ids:
                        existed_connection = existed_connections_dict[connection.connection_id]
                        for key, val in connection.model_dump(by_alias=True).items():
                            setattr(existed_connection, key, val)
                    else:
                        self.session.add(Connection(**connection.model_dump(by_alias=True)))
                    results.success.append(connection.connection_id)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

    def handle_bulk_update(
        self, action: BulkUpdateAction[ConnectionBody], results: BulkActionResponse
    ) -> None:
        """Bulk Update connections."""
        to_update_connection_ids = {connection.connection_id for connection in action.entities}
        _, matched_connection_ids, not_found_connection_ids = self.categorize_connections(
            to_update_connection_ids
        )

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_connection_ids:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The connections with these connection_ids: {not_found_connection_ids} were not found.",
                )
            elif action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                update_connection_ids = matched_connection_ids
            else:
                update_connection_ids = to_update_connection_ids

            for connection in action.entities:
                if connection.connection_id in update_connection_ids:
                    old_connection = self.session.scalar(
                        select(Connection).filter(Connection.conn_id == connection.connection_id).limit(1)
                    )
                    ConnectionBody(**connection.model_dump())
                    for key, val in connection.model_dump(by_alias=True).items():
                        setattr(old_connection, key, val)
                    results.success.append(connection.connection_id)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

        except ValidationError as e:
            results.errors.append({"error": f"{e.errors()}"})

    def handle_bulk_delete(
        self, action: BulkDeleteAction[ConnectionBody], results: BulkActionResponse
    ) -> None:
        """Bulk delete connections."""
        to_delete_connection_ids = set(action.entities)
        _, matched_connection_ids, not_found_connection_ids = self.categorize_connections(
            to_delete_connection_ids
        )

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_connection_ids:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The connections with these connection_ids: {not_found_connection_ids} were not found.",
                )
            elif action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                delete_connection_ids = matched_connection_ids
            else:
                delete_connection_ids = to_delete_connection_ids

            for connection_id in delete_connection_ids:
                existing_connection = self.session.scalar(
                    select(Connection).where(Connection.conn_id == connection_id).limit(1)
                )
                if existing_connection:
                    self.session.delete(existing_connection)
                    results.success.append(connection_id)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})


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
            type: str | list[str] = self.param_type
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
    def _get_hooks_with_mocked_fab() -> (
        tuple[MutableMapping[str, HookInfo | None], dict[str, ConnectionFormWidgetInfo], dict[str, dict]]
    ):
        """Get hooks with all details w/o FAB needing to be installed."""
        from unittest import mock

        from airflow.providers_manager import ProvidersManager

        def mock_lazy_gettext(txt: str) -> str:
            """Mock for flask_babel.lazy_gettext."""
            return txt

        def mock_any_of(allowed_values: list) -> HookMetaService.MockEnum:
            """Mock for wtforms.validators.any_of."""
            return HookMetaService.MockEnum(allowed_values)

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
            mock.patch("flask_appbuilder.fieldwidgets.BS3TextAreaFieldWidget", HookMetaService.MockAnyWidget),
            mock.patch("flask_appbuilder.fieldwidgets.BS3PasswordFieldWidget", HookMetaService.MockAnyWidget),
            mock.patch("wtforms.validators.Optional", HookMetaService.MockOptional),
            mock.patch("wtforms.validators.any_of", mock_any_of),
        ):
            pm = ProvidersManager()
            return pm.hooks, pm.connection_form_widgets, pm.field_behaviours

    @staticmethod
    def _make_standard_fields(field_behaviour: dict | None) -> StandardHookFields | None:
        if not field_behaviour:
            return None

        def make_field(field_name: str, field_behaviour: dict) -> HookFieldBehavior | None:
            hidden_fields = field_behaviour.get("hidden_fields", [])
            relabeling = field_behaviour.get("relabeling", {}).get(field_name)
            placeholder = field_behaviour.get("placeholders", {}).get(field_name)
            if any([field_name in hidden_fields, relabeling, placeholder]):
                return HookFieldBehavior(
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
                print(f"Unknown form widget in {hook_key}: {form_widget}")
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
            print(hook_meta.model_dump_json(indent=4))
            print()
        return result
