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

from cadwyn import (
    ResponseInfo,
    VersionChange,
    VersionChangeWithSideEffects,
    convert_response_to_previous_version_for,
    endpoint,
    enum,
    schema,
)

from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    TerminalStateNonSuccess,
    TIRunContext,
    TITerminalStatePayload,
)


class AddStoppedTaskReport(VersionChange):
    """Allow supervisors to report server-requested termination after the child exits."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (
        enum(TerminalStateNonSuccess).didnt_have("SERVER_TERMINATED"),
        schema(TITerminalStatePayload).field("hostname").didnt_exist,
        schema(TITerminalStatePayload).field("pid").didnt_exist,
    )


class IdentifyRetiredTaskStateUpdates(VersionChangeWithSideEffects):
    """Return 410 for state reports from archived attempts, preserving 404 for unknown attempts."""

    description = __doc__
    instructions_to_migrate_to_previous_version = ()


class AddArgBindingsToTIRunContext(VersionChangeWithSideEffects):
    """Add the ``arg_bindings`` argument-binding spec for stub (foreign-runtime) tasks."""

    description = __doc__

    # A side-effect change, not just a schema one, so ti_run can gate the server-side spec
    # derivation on ``is_applied``: clients older than this version never receive the field.
    instructions_to_migrate_to_previous_version = (schema(TIRunContext).field("arg_bindings").didnt_exist,)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_arg_bindings_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Strip ``arg_bindings`` from the run context for older clients."""
        response.body.pop("arg_bindings", None)


class AddCallbackRunEndpoint(VersionChange):
    """Add the callbacks/{callback_id}/run endpoint a worker uses to exchange its single-use callback token."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/callbacks/{callback_id}/run", ["PATCH"]).didnt_exist,
    )


class AddTerminalStateRetryReasonField(VersionChange):
    """Add the `retry_reason` field to TITerminalStatePayload for failed retry-policy decisions."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TITerminalStatePayload).field("retry_reason").didnt_exist,
    )


class AddMultiTeamToTIRunContext(VersionChange):
    """Add ``multi_team`` so a worker can determine multi-team (e.g. for plugin scoping) without needing to trust its own config."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(TIRunContext).field("multi_team").didnt_exist,)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_multi_team_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Strip ``multi_team`` from the run context for older clients."""
        response.body.pop("multi_team", None)
