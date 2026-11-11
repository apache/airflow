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

"""
Supervisor schema version 2027-02-28.

New bodies in this version:

- ``HITLDetailResponseResult``: the supervisor's reply when a running task reads its own
  Human-in-the-loop response.
"""

from __future__ import annotations

from typing import Any

from cadwyn import (
    RequestInfo,
    VersionChange,
    convert_request_to_next_version_for,
    schema,
)

from airflow.sdk.api.datamodels._generated import (
    LiteralArgBinding,
    TIRunContext,
    TISuccessStatePayload,
    TupleArgBinding,
    VariableResponse,
    XComArgBinding,
    XComSequenceArgBinding,
)
from airflow.sdk.execution_time.comms import PutVariable


class AddExecutionTimeoutToSupervisorTIRunContext(VersionChange):
    """
    Add the ``execution_timeout`` field carrying the task's timeout, which the supervisor enforces.

    The supervisor-schema mirror of the execution API's ``AddExecutionTimeoutToTIRunContext``,
    named apart so the two migrations are not confused.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIRunContext).field("execution_timeout").didnt_exist,
    )


class AddExternalClearTargetToSucceedTask(VersionChange):
    """
    Add the ``external_clear_target`` a task reports with its success, for cross-Dag clears.

    The supervisor-schema mirror of the execution API's ``AddExternalClearTargetField``.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        # SucceedTask inherits the field, and Cadwyn only removes a field from the class that declares it.
        schema(TISuccessStatePayload).field("external_clear_target").didnt_exist,
    )


class MakeSupervisorVariableValueNonNullable(VersionChange):
    """
    Make the Variable value non-nullable in ``VariableResult`` and ``PutVariable``.

    The supervisor-schema mirror of the execution API's ``MakeVariableValueNonNullable``,
    named apart so the two migrations are not confused.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        # VariableResult (and TIRunContext.variables) inherit the field from the generated
        # VariableResponse, and Cadwyn only changes a field on the class that declares it.
        schema(VariableResponse).field("value").had(type=str | None),
        schema(PutVariable).field("value").had(type=str | None),
    )

    @convert_request_to_next_version_for(PutVariable)  # type: ignore[arg-type]
    def store_null_value_as_none_string(request: RequestInfo) -> None:
        """Keep the old behavior of storing ``str(None)`` when an older runtime sets null."""
        if "value" in request.body and request.body["value"] is None:
            request.body["value"] = "None"


class AddMappedStubArgBindingsToSupervisorTIRunContext(VersionChange):
    """
    Add ``arg_bindings`` for mapped stub tasks, the ``sequence`` and ``tuple`` kinds, and the per-index binding fields.

    The supervisor-schema mirror of the execution API's ``AddMappedStubArgBindings``, named apart so
    the two migrations are not confused. The supervisor does not start a task on an older runtime
    when any binding needs this version, so no binding is dropped on the way down.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(XComArgBinding).field("map_index").didnt_exist,
        schema(XComArgBinding).field("element_index").didnt_exist,
        # The generated client flattens the bindings' shared base, so each binding drops these itself.
        *(
            schema(binding).field(field).didnt_exist
            for binding in (XComArgBinding, LiteralArgBinding, XComSequenceArgBinding, TupleArgBinding)
            for field in ("apply_map", "kwarg", "fallback")
        ),
    )


def is_mapped_arg_binding(binding: dict[str, Any]) -> bool:
    """Whether *binding* uses a shape or field this version added."""
    if binding["kind"] in ("sequence", "tuple") or binding.get("apply_map"):
        return True
    return any(
        binding.get(field) is not None for field in ("map_index", "element_index", "kwarg", "fallback")
    )
