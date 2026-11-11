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

from typing import Annotated

from cadwyn import (
    RequestInfo,
    ResponseInfo,
    VersionChange,
    VersionChangeWithSideEffects,
    convert_request_to_next_version_for,
    convert_response_to_previous_version_for,
    endpoint,
    schema,
)
from pydantic import Field
from typing_extensions import TypeAliasType

from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import (
    LiteralArgBinding,
    XComArgBinding,
    XComRef,
    _ArgBindingBase,
)
from airflow.api_fastapi.execution_api.datamodels.taskinstance import TIRunContext, TISuccessStatePayload
from airflow.api_fastapi.execution_api.datamodels.variable import VariablePostBody, VariableResponse


class AddDagProcessorJobEndpoints(VersionChange):
    """Add Dag processor lifecycle, inventory, shared-source publication, and requested-work endpoints."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/jobs", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/heartbeat", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/complete", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/parse-token", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/parse-results", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/bundles", ["GET"]).didnt_exist,
        endpoint("/jobs/{job_id}/bundles/{bundle_name:path}/inventory", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/requested-work/{kind}/claim", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/requested-work/{kind}/{work_id}/ack", ["POST"]).didnt_exist,
    )


class AddExecutionTimeoutToTIRunContext(VersionChange):
    """Add ``execution_timeout`` so the supervisor can enforce it."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (
        schema(TIRunContext).field("execution_timeout").didnt_exist,
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_execution_timeout_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Strip ``execution_timeout`` from the run context for older clients."""
        response.body.pop("execution_timeout", None)


class AddExternalClearTargetField(VersionChange):
    """Add the ``external_clear_target`` field a task reports with its success, for cross-Dag clears."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TISuccessStatePayload).field("external_clear_target").didnt_exist,
    )


class MakeVariableValueNonNullable(VersionChange):
    """
    Make the Variable value non-nullable in reads and writes.

    A Variable read never returned null; a missing Variable is a 404. Setting a Variable to null
    stored the string ``"None"``. Older clients may still send null, which keeps doing that.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(VariableResponse).field("val").had(type=str | None),
        schema(VariablePostBody).field("value").had(type=str | None),
    )

    @convert_request_to_next_version_for(VariablePostBody)  # type: ignore[arg-type]
    def store_null_value_as_none_string(request: RequestInfo) -> None:
        """Keep the old behavior of storing ``str(None)`` when an older client sets null."""
        if "val" in request.body and request.body["val"] is None:
            request.body["val"] = "None"


class AddCallbackStateEndpoint(VersionChange):
    """Add the callback terminal-state endpoint."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/callbacks/{callback_id}/state", ["PATCH"]).didnt_exist,
    )


TaskArgBinding = TypeAliasType(
    "TaskArgBinding",
    Annotated[XComArgBinding | LiteralArgBinding, Field(discriminator="kind", title="TaskArgBinding")],
)
"""The ``TaskArgBinding`` union before this version, named alike so older schemas keep the definition name."""


class AddMappedStubArgBindings(VersionChangeWithSideEffects):
    """
    Add ``arg_bindings`` for mapped stub tasks, the ``sequence`` and ``tuple`` kinds, and the per-index binding fields.

    Older clients keep the earlier behavior for a mapped stub, which gets no ``arg_bindings``. Any other
    stub whose bindings need this version fails ``ti_run``.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIRunContext).field("arg_bindings").had(type=list[TaskArgBinding] | None),
        schema(XComRef).field("map_index").didnt_exist,
        schema(XComRef).field("element_index").didnt_exist,
        schema(_ArgBindingBase).field("apply_map").didnt_exist,
        schema(_ArgBindingBase).field("kwarg").didnt_exist,
        schema(_ArgBindingBase).field("fallback").didnt_exist,
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_mapped_arg_bindings(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Strip ``arg_bindings`` holding any shape an older client cannot read."""
        if any(_is_mapped_binding(binding) for binding in response.body.get("arg_bindings") or ()):
            response.body.pop("arg_bindings")


def _is_mapped_binding(binding: dict) -> bool:
    if binding["kind"] in ("sequence", "tuple") or binding.get("apply_map"):
        return True
    return any(
        binding.get(field) is not None for field in ("map_index", "element_index", "kwarg", "fallback")
    )
