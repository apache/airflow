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

"""Supervisor schema version 2027-02-28."""

from __future__ import annotations

from typing import Any

from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for, schema

from airflow.sdk.api.datamodels._generated import TIRunContext, XComArgBinding
from airflow.sdk.execution_time.comms import StartupDetails


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


class AddMappedStubArgBindingsToSupervisorTIRunContext(VersionChange):
    """
    Add ``arg_bindings`` for mapped stub tasks, the ``sequence``, ``tuple`` and ``local`` kinds, and the XCom element fields.

    The supervisor-schema mirror of the execution API's ``AddMappedStubArgBindings``, named apart so
    the two migrations are not confused.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(XComArgBinding).field("map_index").didnt_exist,
        schema(XComArgBinding).field("element_index").didnt_exist,
        schema(XComArgBinding).field("mapping_key").didnt_exist,
    )

    @convert_response_to_previous_version_for(StartupDetails)  # type: ignore[arg-type]
    def remove_mapped_arg_bindings(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Strip ``arg_bindings`` holding any shape an older runtime cannot read."""
        ti_context = response.body.get("ti_context") or {}
        if any(_is_mapped_binding(binding) for binding in ti_context.get("arg_bindings") or ()):
            ti_context.pop("arg_bindings")


def _is_mapped_binding(binding: dict[str, Any]) -> bool:
    if binding["kind"] in ("sequence", "tuple", "local"):
        return True
    return binding["kind"] == "xcom" and (
        binding.get("map_index", -1) != -1
        or binding.get("element_index") is not None
        or binding.get("mapping_key") is not None
    )
