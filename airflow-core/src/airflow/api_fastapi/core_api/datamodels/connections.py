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
from collections.abc import Iterable, Mapping
from datetime import datetime
from functools import cache
from typing import Annotated, Any

from pydantic import Field, field_validator, model_validator
from pydantic_core.core_schema import ValidationInfo

from airflow._shared.secrets_masker import redact, should_hide_value_for_key
from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel, make_partial_model
from airflow.configuration import conf

PortNumber = Annotated[int, Field(ge=0, le=65535)]

_MASK = "***"


@cache
def _sensitive_extra_keys() -> frozenset[str]:
    """
    Return the ``extra`` keys any provider declares sensitive, as plain and ``extra__<type>__`` names.

    Providers mark a connection field as a secret with ``format: password`` in their
    ``conn-fields``. The name-based masking in ``redact`` cannot use that knowledge, because it
    only knows a fixed list of secret names, and ``config`` is not one of them. Read from provider
    metadata, without importing any hook.

    Not limited to the connection's current type: after a ``conn_type`` change the stored
    ``extra`` still holds the previous type's secrets, and the new type does not declare them.
    """
    from airflow.providers_manager import ProvidersManager

    keys: set[str] = set()
    for prefixed_name, widget in ProvidersManager()._connection_form_widgets_from_metadata.items():
        if widget.is_sensitive:
            keys.update((widget.field_name, prefixed_name))
    return frozenset(keys)


def restore_provider_sensitive_extra(extra: Any, stored_extra: Any) -> Any:
    """
    Put back the stored value of provider-declared sensitive ``extra`` fields submitted as the mask.

    ``merge`` only restores keys on its fixed list of secret names, so the keys masked by
    ``ConnectionResponse.redact_provider_sensitive_extra`` would otherwise be saved as the mask
    when a client sends a response's ``extra`` back.
    """
    if not isinstance(extra, dict) or not isinstance(stored_extra, dict):
        return extra
    return {
        key: stored_extra[key]
        if value == _MASK and key in stored_extra and key in _sensitive_extra_keys()
        else value
        for key, value in extra.items()
    }


# Response Models
class ConnectionResponse(BaseModel):
    """Connection serializer for responses."""

    connection_id: str = Field(serialization_alias="connection_id", validation_alias="conn_id")
    conn_type: str
    description: str | None
    host: str | None
    login: str | None
    schema_: str | None = Field(alias="schema")
    port: int | None
    password: str | None
    extra: str | None
    team_name: str | None

    @field_validator("password", mode="after")
    @classmethod
    def redact_password(cls, v: str | None, field_info: ValidationInfo) -> str | None:
        if v is None:
            return None
        return str(redact(v, field_info.field_name))

    @model_validator(mode="after")
    def redact_provider_sensitive_extra(self) -> ConnectionResponse:
        """Mask the ``extra`` fields any provider declares sensitive."""
        if not self.extra or not _sensitive_extra_keys():
            return self
        extra = json.loads(self.extra)
        if not isinstance(extra, dict):
            return self
        for key, value in extra.items():
            if key in _sensitive_extra_keys() and value not in (None, ""):
                extra[key] = _MASK
        self.extra = json.dumps(extra)
        return self

    @field_validator("extra", mode="before")
    @classmethod
    def redact_extra(cls, v: str | None) -> str | None:
        if v is None or v == "":
            return v
        try:
            extra_dict = json.loads(v)
            redacted_dict = redact(extra_dict)
            return json.dumps(redacted_dict)
        except json.JSONDecodeError:
            # Do not return un-redacted extra because this could cause sensitive information to be exposed.
            # This code path should never be hit as ``Connection._validate_extra`` makes sure that ``extra`` is
            # always a valid JSON string (if truthy). We add this safeguard just in case and to make the coupling
            # explicit.
            raise ValueError(
                "This code path should never happen as persisted Connections (DB layer) should always enforce `extra` as a JSON string."
            )


class ConnectionCollectionResponse(BaseModel):
    """Connection Collection serializer for responses."""

    connections: Iterable[ConnectionResponse]
    total_entries: int


class ConnectionTestResponse(BaseModel):
    """Connection Test serializer for synchronous test responses."""

    status: bool
    message: str


class ConnectionTestQueuedResponse(BaseModel):
    """Response returned when a connection test has been enqueued for worker execution."""

    token: str
    connection_id: str
    state: str


class AsyncConnectionTestResponse(BaseModel):
    """Response returned when polling for the status of an enqueued connection test."""

    token: str
    connection_id: str
    state: str
    result_message: str | None = None
    created_at: datetime


class ConnectionHookFieldBehavior(BaseModel):
    """A class to store the behavior of each standard field of a Hook."""

    hidden: Annotated[
        bool,
        Field(description="Flag if the form field should be hidden."),
    ] = False
    title: Annotated[
        str | None,
        Field(
            description="Label / title for the field that should be displayed, if re-labelling is needed. Use `None` to display standard title."
        ),
    ] = None
    placeholder: Annotated[
        str | None,
        Field(description="Placeholder text that should be populated to the form."),
    ] = None


class StandardHookFields(BaseModel):
    """Standard fields of a Hook that a form will render."""

    description: ConnectionHookFieldBehavior | None
    url_schema: ConnectionHookFieldBehavior | None
    host: ConnectionHookFieldBehavior | None
    port: ConnectionHookFieldBehavior | None
    login: ConnectionHookFieldBehavior | None
    password: ConnectionHookFieldBehavior | None


class ConnectionHookMetaData(BaseModel):
    """
    Response model for Hook information == Connection type meta data.

    It is used to transfer providers information loaded by providers_manager such that
    the API server/Web UI can use this data to render connection form UI.
    """

    connection_type: str | None
    hook_class_name: str | None
    default_conn_name: str | None
    hook_name: str
    standard_fields: StandardHookFields | None
    extra_fields: Mapping | None

    @field_validator("extra_fields", mode="after")
    @classmethod
    def redact_extra_fields(cls, v: Mapping | None):
        if v is None:
            return None

        # Check if extra_fields contains param spec structures (result of SerializedParam.dump())
        # which have "value" and "schema" keys, or simple dictionary structures
        has_param_spec_structure = any(
            isinstance(field_spec, dict) and "value" in field_spec and "schema" in field_spec
            for field_spec in v.values()
        )

        if has_param_spec_structure:
            redacted_extra_fields: dict[str, Any] = {}
            for field_name, field_spec in v.items():
                if isinstance(field_spec, dict) and "value" in field_spec and "schema" in field_spec:
                    if should_hide_value_for_key(field_name) and field_spec.get("value") is not None:
                        # Mask only the value, preserve everything else including schema.type
                        redacted_extra_fields[field_name] = {**field_spec, "value": "***"}
                    else:
                        # Not sensitive or no value, keep as is
                        redacted_extra_fields[field_name] = field_spec
                else:
                    # Not a param spec structure, apply redact by default
                    redacted_extra_fields[field_name] = redact(field_spec)

            return redacted_extra_fields

        # For simple dictionary structures, use the standard redact function
        return redact(v)


# Request Models
class ConnectionBody(StrictBaseModel):
    """Connection Serializer for requests body."""

    connection_id: str = Field(serialization_alias="conn_id", max_length=200, pattern=r"^[\w.-]+$")
    conn_type: str
    description: str | None = Field(default=None)
    host: str | None = Field(default=None)
    login: str | None = Field(default=None)
    schema_: str | None = Field(None, alias="schema")
    port: PortNumber | None = Field(default=None)
    password: str | None = Field(default=None)
    extra: str | None = Field(default=None)
    team_name: str | None = Field(max_length=50, default=None)

    @field_validator("extra")
    @classmethod
    def validate_extra(cls, v: str | None) -> str | None:
        """
        Validate that `extra` field is a JSON-encoded Python dict.

        If `extra` field is not a valid JSON, it will be returned as is.
        """
        if v is None:
            return v
        if v == "":
            return "{}"  # Backward compatibility: treat "" as empty JSON object
        try:
            extra_dict = json.loads(v)
            if not isinstance(extra_dict, dict):
                raise ValueError("The `extra` field must be a valid JSON object (e.g., {'key': 'value'})")
        except json.JSONDecodeError:
            raise ValueError(
                "The `extra` field must be a valid JSON object (e.g., {'key': 'value'}), "
                "but encountered non-JSON in `extra` field"
            )
        return v

    @model_validator(mode="after")
    def validate_team_name(self) -> ConnectionBody:
        if self.team_name is not None and not conf.getboolean("core", "multi_team"):
            raise ValueError(
                "team_name cannot be set when multi_team mode is disabled. Please contact your administrator."
            )
        return self


ConnectionBodyPartial = make_partial_model(ConnectionBody)


class ConnectionTestRequestBody(ConnectionBody):
    """
    Request body for enqueueing a connection test on a worker.

    Inherits ``connection_id`` pattern, ``extra`` JSON validation, and
    ``team_name`` handling from ``ConnectionBody`` so tested connections share
    the same input contract as persisted ones.
    """

    commit_on_success: bool = Field(
        default=False,
        description="If True, save or update the connection in the connection table when the test succeeds.",
    )
    executor: str | None = Field(
        default=None,
        description="Executor name to dispatch the connection test to.",
    )
    queue: str | None = Field(
        default=None,
        description="Worker queue to route the connection test to (executor-dependent).",
    )
