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

from typing import Any

from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for, schema

from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    DagRun,
    TIDeferredStatePayload,
    TIRunContext,
)


class ModifyDeferredTaskKwargsToJsonValue(VersionChange):
    """Change the types of `trigger_kwargs` and `next_kwargs` in TIDeferredStatePayload to JsonValue."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIDeferredStatePayload).field("trigger_kwargs").had(type=dict[str, Any] | str),
        schema(TIDeferredStatePayload).field("next_kwargs").had(type=dict[str, Any]),
    )


class RemoveUpstreamMapIndexesField(VersionChange):
    """Remove upstream_map_indexes field from TIRunContext - now computed by Task SDK."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIRunContext)
        .field("upstream_map_indexes")
        .existed_as(type=dict[str, int | list[int] | None] | None),
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def add_upstream_map_indexes_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Add upstream_map_indexes field with None for older API versions."""
        response.body["upstream_map_indexes"] = None


class AddNoteField(VersionChange):
    """Add note parameter to DagRun Model."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(DagRun).field("note").didnt_exist,)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_note_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Remove note field for older API versions."""
        if "dag_run" in response.body and isinstance(response.body["dag_run"], dict):
            response.body["dag_run"].pop("note", None)
