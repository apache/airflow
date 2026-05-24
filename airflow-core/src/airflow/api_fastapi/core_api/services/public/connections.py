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
from collections.abc import Iterable
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any

from fastapi import HTTPException, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from sqlalchemy import select

from airflow._shared.secrets_masker import merge
from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionOnExistence,
    BulkActionResponse,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.connections import ConnectionBody
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.models.connection import Connection
from airflow.providers_manager import ProvidersManager


class _StopValidation:
    pass


_STOP_VALIDATION = _StopValidation()


class _ConnectionExtraField:
    """Minimal WTForms-compatible field object for legacy connection validators."""

    def __init__(self, name: str, data: Any):
        self.name = name
        self.data = data
        self.raw_data = [] if data is None else [str(data)]
        self.errors: list[str] = []


def _get_form_widget_validators(field: Any) -> Iterable:
    if isinstance(field, dict):
        return ()
    if hasattr(field, "kwargs"):
        return field.kwargs.get("validators", ())
    return getattr(field, "validators", ())


def _get_form_widget_default(field: Any) -> Any:
    if hasattr(field, "kwargs"):
        return field.kwargs.get("default")
    return getattr(field, "default", None)


def _optional_flask_request_context():
    try:
        from flask import Flask, has_request_context

        if has_request_context():
            return nullcontext()

        app = Flask("airflow_connection_form_validation")
        app.secret_key = "airflow-connection-form-validation"
        return app.test_request_context()
    except ImportError:
        return nullcontext()


def _run_form_widget_validator(
    validator, form: SimpleNamespace, field: _ConnectionExtraField
) -> str | _StopValidation | None:
    try:
        with _optional_flask_request_context():
            validator(form, field)
    except Exception as err:
        if err.__class__.__name__ == "StopValidation":
            return str(err) or _STOP_VALIDATION
        if err.__class__.__name__ == "ValidationError" or isinstance(err, ValueError):
            return str(err)
        raise
    return None


def validate_connection_form_widgets(connection: ConnectionBody) -> None:
    """Run legacy provider WTForms validators for extra fields on connection form submission."""
    try:
        extra = json.loads(connection.extra or "{}")
    except json.JSONDecodeError:
        return

    if not isinstance(extra, dict) or not extra:
        return

    prefix = f"extra__{connection.conn_type}__"
    providers_manager = ProvidersManager()
    if connection.conn_type in providers_manager.hooks:
        providers_manager.hooks[connection.conn_type]
    elif not any(
        widget_key.startswith(prefix)
        for widget_key in providers_manager._connection_form_widgets_from_metadata
    ):
        return

    form_fields = {
        "connection_id": _ConnectionExtraField("connection_id", connection.connection_id),
        "conn_type": _ConnectionExtraField("conn_type", connection.conn_type),
        "description": _ConnectionExtraField("description", connection.description),
        "host": _ConnectionExtraField("host", connection.host),
        "login": _ConnectionExtraField("login", connection.login),
        "schema": _ConnectionExtraField("schema", connection.schema_),
        "port": _ConnectionExtraField("port", connection.port),
        "password": _ConnectionExtraField("password", connection.password),
        "team_name": _ConnectionExtraField("team_name", connection.team_name),
    }
    form_fields.update({key: _ConnectionExtraField(key, value) for key, value in extra.items()})

    errors = []
    for widget_key, widget in providers_manager._connection_form_widgets_from_metadata.items():
        if not widget_key.startswith(prefix):
            continue

        validators = tuple(_get_form_widget_validators(widget.field))
        if not validators:
            continue

        field_name = widget.field_name
        field = form_fields.setdefault(
            field_name,
            _ConnectionExtraField(field_name, extra.get(field_name, _get_form_widget_default(widget.field))),
        )
        form = SimpleNamespace(
            **form_fields, data={name: form_field.data for name, form_field in form_fields.items()}
        )

        for validator in validators:
            validation_result = _run_form_widget_validator(validator, form, field)
            if validation_result is _STOP_VALIDATION:
                break
            if validation_result is not None:
                errors.append(
                    {
                        "type": "value_error",
                        "loc": ("body", "extra", field_name),
                        "msg": validation_result,
                        "input": field.data,
                    }
                )
                break

    if errors:
        raise RequestValidationError(errors=errors)


def update_orm_from_pydantic(
    orm_conn: Connection, pydantic_conn: ConnectionBody, update_mask: list[str] | None = None
) -> None:
    """Update ORM object from Pydantic object."""
    # Not all fields match and some need setters, therefore copy partly manually via setters
    non_update_fields = {"connection_id", "conn_id"}
    setter_fields = {"password", "extra"}
    fields_set = pydantic_conn.model_fields_set
    if "schema_" in fields_set:  # Alias is not resolved correctly, need to patch
        fields_set.remove("schema_")
        fields_set.add("schema")
    fields_to_update = fields_set - non_update_fields - setter_fields
    if update_mask:
        fields_to_update = fields_to_update.intersection(update_mask)
    conn_data = pydantic_conn.model_dump(by_alias=True)
    for key, val in conn_data.items():
        if key in fields_to_update:
            setattr(orm_conn, key, val)

    if (not update_mask and "password" in pydantic_conn.model_fields_set) or (
        update_mask and "password" in update_mask
    ):
        if pydantic_conn.password is None:
            orm_conn.set_password(pydantic_conn.password)
        else:
            merged_password = merge(pydantic_conn.password, orm_conn.password, "password")
            orm_conn.set_password(merged_password)
    if (not update_mask and "extra" in pydantic_conn.model_fields_set) or (
        update_mask and "extra" in update_mask
    ):
        if pydantic_conn.extra is None or orm_conn.extra is None:
            orm_conn.set_extra(pydantic_conn.extra)
            return
        try:
            merged_extra = merge(json.loads(pydantic_conn.extra), json.loads(orm_conn.extra))
            orm_conn.set_extra(json.dumps(merged_extra))
        except json.JSONDecodeError:
            # We can't merge fields in an unstructured `extra`
            orm_conn.set_extra(pydantic_conn.extra)


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
            if action.action_on_existence == BulkActionOnExistence.SKIP:
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
        existed_connections_dict, matched_connection_ids, not_found_connection_ids = (
            self.categorize_connections(to_update_connection_ids)
        )

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_connection_ids:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The connections with these connection_ids: {not_found_connection_ids} were not found.",
                )
            if action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                update_connection_ids = matched_connection_ids
            else:
                update_connection_ids = to_update_connection_ids

            for connection in action.entities:
                if connection.connection_id in update_connection_ids:
                    old_connection = existed_connections_dict.get(connection.connection_id)
                    if old_connection is None:
                        raise ValidationError(
                            f"The Connection with connection_id: `{connection.connection_id}` was not found"
                        )
                    ConnectionBody(**connection.model_dump())

                    update_orm_from_pydantic(old_connection, connection)
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
        existed_connections_dict, matched_connection_ids, not_found_connection_ids = (
            self.categorize_connections(to_delete_connection_ids)
        )

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_connection_ids:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The connections with these connection_ids: {not_found_connection_ids} were not found.",
                )
            if action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                delete_connection_ids = matched_connection_ids
            else:
                delete_connection_ids = to_delete_connection_ids

            for connection_id in delete_connection_ids:
                existing_connection = existed_connections_dict.get(connection_id)
                if existing_connection:
                    self.session.delete(existing_connection)
                    results.success.append(connection_id)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})
