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
    schema,
)

from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import XComArgBinding
from airflow.api_fastapi.execution_api.datamodels.taskinstance import TIRunContext


class AddDagProcessorJobEndpoints(VersionChange):
    """Add Dag processor lifecycle and parse-token endpoints."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/jobs", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/heartbeat", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/complete", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/parse-token", ["POST"]).didnt_exist,
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


class AddMappedStubArgBindings(VersionChangeWithSideEffects):
    """
    Add ``arg_bindings`` for mapped stub tasks, the ``sequence``, ``tuple`` and ``unresolved`` kinds, and the XCom element fields.

    Older clients keep the earlier behavior: a mapped stub gets no ``arg_bindings``, and a run context
    holding any of the new shapes drops ``arg_bindings`` entirely.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(XComArgBinding).field("map_index").didnt_exist,
        schema(XComArgBinding).field("element_index").didnt_exist,
        schema(XComArgBinding).field("mapping_key").didnt_exist,
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_mapped_arg_bindings(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Strip ``arg_bindings`` holding any shape an older client cannot read."""
        if any(_is_mapped_binding(binding) for binding in response.body.get("arg_bindings") or ()):
            response.body.pop("arg_bindings")


def _is_mapped_binding(binding: dict) -> bool:
    if binding["kind"] in ("sequence", "tuple", "unresolved"):
        return True
    return binding["kind"] == "xcom" and (
        binding.get("map_index", -1) != -1
        or binding.get("element_index") is not None
        or binding.get("mapping_key") is not None
    )
